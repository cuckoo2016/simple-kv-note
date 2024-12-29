mod config; // 导入配置模块
mod error; // 导入错误处理模块
mod network; // 导入网络模块
mod pb; // 导入协议缓冲区模块
mod service; // 导入服务模块
mod storage; // 导入存储模块

use std::{net::SocketAddr, str::FromStr}; // 导入标准库中的SocketAddr和FromStr
pub use config::*; // 公开使用配置模块
pub use error::KvError; // 公开使用错误处理模块中的KvError
pub use network::*; // 公开使用网络模块
pub use pb::abi::*; // 公开使用协议缓冲区模块中的abi
pub use service::*; // 公开使用服务模块
pub use storage::*; // 公开使用存储模块

use anyhow::Result; // 导入anyhow库中的Result类型，用于错误处理
use s2n_quic::{client::Connect, Client, Server}; // 导入s2n_quic库中的Client和Server
use tokio::net::{TcpListener, TcpStream}; // 导入tokio库中的TcpListener和TcpStream
use tokio_rustls::client; // 导入tokio_rustls库中的client
use tokio_util::compat::FuturesAsyncReadCompatExt; // 导入tokio_util库中的FuturesAsyncReadCompatExt
use tracing::{info, instrument, span}; // 导入tracing库中的info、instrument和span

/// 通过配置创建 KV 服务器
#[instrument(skip_all)] // 使用instrument宏来自动记录函数的调用和返回
pub async fn start_server_with_config(config: &ServerConfig) -> Result<()> { // 定义一个异步函数，用于通过配置启动服务器
    let addr = &config.general.addr; // 获取服务器配置中的地址
    match config.general.network { // 根据配置中的网络类型进行匹配
        NetworkType::Tcp => { // 如果网络类型是TCP
            let acceptor = TlsServerAcceptor::new(
                &config.tls.cert,
                &config.tls.key,
                config.tls.ca.as_deref(),
            )?; // 创建TLS服务器接受器

            // 静态分发
            match &config.storage { // 根据配置中的存储类型进行匹配
                StorageConfig::MemTable => {
                    start_tls_server(addr, MemTable::new(), acceptor).await? // 如果存储类型是内存表，则启动TLS服务器
                }
                StorageConfig::SledDb(path) => {
                    start_tls_server(addr, SledDb::new(path), acceptor).await? // 如果存储类型是SledDb，则启动TLS服务器
                }
            };
        }
        NetworkType::Quic => { // 如果网络类型是QUIC
            match &config.storage { // 根据配置中的存储类型进行匹配
                StorageConfig::MemTable => {
                    start_quic_server(addr, MemTable::new(), &config.tls).await? // 如果存储类型是内存表，则启动QUIC服务器
                }
                StorageConfig::SledDb(path) => {
                    start_quic_server(addr, SledDb::new(path), &config.tls).await? // 如果存储类型是SledDb，则启动QUIC服务器
                }
            };
        }
    }

    Ok(()) // 返回成功
}

/// 通过配置创建 KV 客户端
#[instrument(skip_all)] // 使用instrument宏来自动记录函数的调用和返回
pub async fn start_yamux_client_with_config(
    config: &ClientConfig,
) -> Result<YamuxCtrl<client::TlsStream<TcpStream>>> { // 定义一个异步函数，用于通过配置启动Yamux客户端
    let addr = &config.general.addr; // 获取客户端配置中的地址
    let tls = &config.tls; // 获取客户端配置中的TLS配置

    let identity = tls.identity.as_ref().map(|(c, k)| (c.as_str(), k.as_str())); // 获取客户端身份
    let connector = TlsClientConnector::new(&tls.domain, identity, tls.ca.as_deref())?; // 创建TLS客户端连接器
    let stream = TcpStream::connect(addr).await?; // 连接到服务器
    let stream = connector.connect(stream).await?; // 使用连接器连接到服务器

    // 打开一个 stream
    Ok(YamuxCtrl::new_client(stream, None)) // 返回Yamux控制器
}

#[instrument(skip_all)] // 使用instrument宏来自动记录函数的调用和返回
pub async fn start_quic_client_with_config(config: &ClientConfig) -> Result<QuicCtrl> { // 定义一个异步函数，用于通过配置启动QUIC客户端
    let addr = SocketAddr::from_str(&config.general.addr)?; // 将地址字符串转换为SocketAddr
    let tls = &config.tls; // 获取客户端配置中的TLS配置

    let client = Client::builder()
        .with_tls(tls.ca.as_ref().unwrap().as_str())? // 创建QUIC客户端
        .with_io("0.0.0.0:0")? // 设置客户端的IO地址
        .start()
        .map_err(|e| anyhow::anyhow!("Failed to start client. Error: {}", e))?; // 启动客户端

    let connect = Connect::new(addr).with_server_name("localhost"); // 创建连接
    let mut conn = client.connect(connect).await?; // 连接到服务器

    // ensure the connection doesn't time out with inactivity
    conn.keep_alive(true)?; // 保持连接活跃

    Ok(QuicCtrl::new(conn)) // 返回QUIC控制器
}

async fn start_quic_server<Store: Storage>(
    addr: &str,
    store: Store,
    tls_config: &ServerTlsConfig,
) -> Result<()> { // 定义一个异步函数，用于启动QUIC服务器
    let service: Service<Store> = ServiceInner::new(store).into(); // 创建服务
    let mut listener = Server::builder()
        .with_tls((tls_config.cert.as_str(), tls_config.key.as_str()))? // 配置TLS
        .with_io(addr)? // 设置监听地址
        .start()
        .map_err(|e| anyhow::anyhow!("Failed to start server. Error: {}", e))?; // 启动服务器

    info!("Start listening on {addr}"); // 打印监听地址

    loop {
        let root = span!(tracing::Level::INFO, "server_process"); // 创建跟踪Span
        let _enter = root.enter(); // 进入Span

        if let Some(mut conn) = listener.accept().await { // 接受连接
            info!("Client {} connected", conn.remote_addr()?); // 打印客户端连接信息
            let svc = service.clone(); // 克隆服务

            tokio::spawn(async move {
                while let Ok(Some(stream)) = conn.accept_bidirectional_stream().await { // 接受双向流
                    // 多路复用：每次收到1个Command，就会得到1个新的Stream，直到把当前Stream的数据处理完
                    info!(
                        "Accepted stream from {}",
                        stream.connection().remote_addr()?
                    ); // 打印流接受信息
                    let svc1 = svc.clone(); // 克隆服务
                    tokio::spawn(async move {
                        let stream = ProstServerStream::new(stream, svc1.clone()); // 创建流处理器
                        stream.process().await.unwrap(); // 处理流
                    });
                }
                Ok::<(), anyhow::Error>(()) // 返回成功
            });
        }
    }
}

async fn start_tls_server<Store: Storage>(
    addr: &str,
    store: Store,
    acceptor: TlsServerAcceptor,
) -> Result<()> { // 定义一个异步函数，用于启动TLS服务器
    let service: Service<Store> = ServiceInner::new(store).into(); // 创建服务
    let listener = TcpListener::bind(addr).await?; // 创建TCP监听器
    info!("Start listening on {}", addr); // 打印监听地址
    loop {
        let root = span!(tracing::Level::INFO, "server_process"); // 创建跟踪Span
        let _enter = root.enter(); // 进入Span
        let tls = acceptor.clone(); // 克隆TLS接受器
        let (stream, addr) = listener.accept().await?; // 接受连接
        info!("Client {:?} connected", addr); // 打印客户端连接信息

        // 为每个客户端的连接任务分配一个新的Task
        let svc = service.clone(); // 克隆服务
        tokio::spawn(async move {
            let stream = tls.accept(stream).await.unwrap(); // 接受TLS流（流转换）
            YamuxCtrl::new_server(stream, None, move |stream| {
                info!(
                    "Accepted stream from {:?}",
                    addr
                ); // 打印流接受信息

                // 多路复用：每次收到1个Command，就会得到1个新的Stream，直到把当前Stream的数据处理完
                let svc1 = svc.clone(); // 克隆服务
                async move {
                    let stream = ProstServerStream::new(stream.compat(), svc1.clone()); // 创建流处理器
                    stream.process().await.unwrap(); // 处理流
                    Ok(())
                }
            });
        });
    }
}
