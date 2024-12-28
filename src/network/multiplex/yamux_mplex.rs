// 引入 async_trait，用于异步 trait 实现
use async_trait::async_trait;
// 引入 futures，用于异步编程
use futures::{future, Future, TryStreamExt};
// 引入 PhantomData，用于类型占位符
use std::marker::PhantomData;
// 引入 tokio::io，用于异步读写
use tokio::io::{AsyncRead, AsyncWrite};
// 引入 tokio_util::compat，用于将 futures 的 AsyncReadExt 和 AsyncWriteExt 转换为 tokio 的 AsyncReadExt 和 AsyncWriteExt
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
// 引入 tracing::instrument，用于日志记录
use tracing::instrument;
// 引入 yamux，用于多路复用
use yamux::{Config, Connection, ConnectionError, Control, Mode, WindowUpdateMode};

// 引入 crate 中的错误类型和客户端流类型
use crate::{KvError, ProstClientStream};

// 引入 AppStream trait，用于定义应用流接口
use super::AppStream;

/// Yamux 控制结构
pub struct YamuxCtrl<S> {
    /// yamux control，用于创建新的 stream
    ctrl: Control,
    _conn: PhantomData<S>,
}

impl<S> YamuxCtrl<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    /// 创建 yamux 客户端
    pub fn new_client(stream: S, config: Option<Config>) -> Self {
        // 调用 new 方法创建 yamux 客户端
        Self::new(stream, config, true, |_stream| future::ready(Ok(())))
    }

    /// 创建 yamux 服务端，服务端我们需要具体处理 stream
    pub fn new_server<F, Fut>(stream: S, config: Option<Config>, f: F) -> Self
    where
        F: FnMut(yamux::Stream) -> Fut,
        F: Send + 'static,
        Fut: Future<Output = Result<(), ConnectionError>> + Send + 'static,
    {
        // 调用 new 方法创建 yamux 服务端
        Self::new(stream, config, false, f)
    }

    #[instrument(name = "yamux_ctrl_new", skip_all)]
    // 创建 YamuxCtrl
    fn new<F, Fut>(stream: S, config: Option<Config>, is_client: bool, f: F) -> Self
    where
        F: FnMut(yamux::Stream) -> Fut,
        F: Send + 'static,
        Fut: Future<Output = Result<(), ConnectionError>> + Send + 'static,
    {
        // 根据是否为客户端，设置模式
        let mode = if is_client {
            Mode::Client
        } else {
            Mode::Server
        };

        // 创建或更新配置
        let mut config = config.unwrap_or_default();
        config.set_window_update_mode(WindowUpdateMode::OnRead);

        // 创建 config，yamux::Stream 使用的是 futures 的 trait 所以需要 compat() 到 tokio 的 trait
        let conn = Connection::new(stream.compat(), config, mode);

        // 创建 yamux ctrl
        let ctrl = conn.control();

        // pull 所有 stream 下的数据
        tokio::spawn(yamux::into_stream(conn).try_for_each_concurrent(None, f));

        Self {
            ctrl,
            _conn: PhantomData::default(),
        }
    }
}

#[async_trait]
impl<S> AppStream for YamuxCtrl<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type InnerStream = Compat<yamux::Stream>;

    #[instrument(skip_all)]
    async fn open_stream(&mut self) -> Result<ProstClientStream<Self::InnerStream>, KvError> {
        // 从 yamux ctrl 中打开一个新的 stream
        let stream = self.ctrl.open_stream().await?;
        Ok(ProstClientStream::new(stream.compat()))
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::*;
    use crate::{
        assert_res_ok,
        network::tls::tls_utils::{tls_acceptor, tls_connector},
        utils::DummyStream,
        CommandRequest, KvError, MemTable, ProstServerStream, Service, ServiceInner, Storage,
        TlsServerAcceptor,
    };
    use anyhow::Result;
    use tokio::net::{TcpListener, TcpStream};
    use tokio_rustls::server;
    use tracing::warn;

    pub async fn start_server_with<Store>(
        addr: &str,
        tls: TlsServerAcceptor,
        store: Store,
        f: impl Fn(server::TlsStream<TcpStream>, Service) + Send + Sync + 'static,
    ) -> Result<SocketAddr, KvError>
    where
        Store: Storage,
        Service: From<ServiceInner<Store>>,
    {
        let listener = TcpListener::bind(addr).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let service: Service = ServiceInner::new(store).into();

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _addr)) => match tls.accept(stream).await {
                        Ok(stream) => f(stream, service.clone()),
                        Err(e) => warn!("Failed to process TLS: {:?}", e),
                    },
                    Err(e) => warn!("Failed to process TCP: {:?}", e),
                }
            }
        });

        Ok(addr)
    }

    /// 创建 ymaux server
    pub async fn start_yamux_server<Store>(
        addr: &str,
        tls: TlsServerAcceptor,
        store: Store,
    ) -> Result<SocketAddr, KvError>
    where
        Store: Storage,
        Service: From<ServiceInner<Store>>,
    {
        let f = |stream, service: Service| {
            YamuxCtrl::new_server(stream, None, move |s| {
                let svc = service.clone();
                async move {
                    let stream = ProstServerStream::new(s.compat(), svc);
                    stream.process().await.unwrap();
                    Ok(())
                }
            });
        };
        start_server_with(addr, tls, store, f).await
    }

    #[tokio::test]
    async fn yamux_ctrl_creation_should_work() -> Result<()> {
        let s = DummyStream::default();
        let mut ctrl = YamuxCtrl::new_client(s, None);
        let stream = ctrl.open_stream().await;

        assert!(stream.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn yamux_ctrl_client_server_should_work() -> Result<()> {
        // 创建使用了 TLS 的 yamux server
        let acceptor = tls_acceptor(false)?;
        let addr = start_yamux_server("127.0.0.1:0", acceptor, MemTable::new()).await?;

        let connector = tls_connector(false)?;
        let stream = TcpStream::connect(addr).await?;
        let stream = connector.connect(stream).await?;
        // 创建使用了 TLS 的 yamux client
        let mut ctrl = YamuxCtrl::new_client(stream, None);

        // 从 client ctrl 中打开一个新的 yamux stream
        let mut stream = ctrl.open_stream().await?;

        let cmd = CommandRequest::new_hset("t1", "k1", "v1".into());
        stream.execute_unary(&cmd).await.unwrap();

        let cmd = CommandRequest::new_hget("t1", "k1");
        let res = stream.execute_unary(&cmd).await.unwrap();
        assert_res_ok(&res, &["v1".into()], &[]);

        Ok(())
    }
}
