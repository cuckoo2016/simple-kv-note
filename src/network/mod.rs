mod frame; // 定义了frame模块
mod multiplex; // 定义了multiplex模块
mod stream; // 定义了stream模块
mod stream_result; // 定义了stream_result模块
mod tls; // 定义了tls模块

// 导出frame模块中的read_frame和FrameCoder
pub use frame::{read_frame, FrameCoder};
// 导出multiplex模块中的AppStream, QuicCtrl, YamuxCtrl
pub use multiplex::{AppStream, QuicCtrl, YamuxCtrl};
// 导出stream模块中的ProstStream
pub use stream::ProstStream;
// 导出stream_result模块中的StreamResult
pub use stream_result::StreamResult;
// 导出tls模块中的TlsClientConnector, TlsServerAcceptor
pub use tls::{TlsClientConnector, TlsServerAcceptor};

// 导入必要的依赖项
use crate::{CommandRequest, CommandResponse, KvError, Service, Storage};
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{info, warn};

/// 处理服务器端的某个 accept 下来的 socket 的读写
pub struct ProstServerStream<S, Store> {
    inner: ProstStream<S, CommandRequest, CommandResponse>, // 内部流处理器
    service: Service<Store>, // 服务实例
}

/// 处理客户端 socket 的读写
pub struct ProstClientStream<S> {
    inner: ProstStream<S, CommandResponse, CommandRequest>, // 内部流处理器
}

impl<S, Store> ProstServerStream<S, Store>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static, // 定义S的trait约束
    Store: Storage, // 定义Store的trait约束
{
    pub fn new(stream: S, service: Service<Store>) -> Self {
        Self {
            inner: ProstStream::new(stream), // 创建流处理器
            service, // 保存服务实例
        }
    }

    pub async fn process(mut self) -> Result<(), KvError> {
        let stream = &mut self.inner; // 获取流处理器的可变引用
        while let Some(Ok(cmd)) = stream.next().await { // 从流中读取命令
            info!("Got a new command: {:?}", cmd); // 打印接收到的命令
            let mut res = self.service.execute(cmd); // 执行服务
            while let Some(data) = res.next().await { // 从服务执行结果中读取数据
                if let Err(e) = stream.send(&data).await { // 发送数据
                    warn!("Failed to send response: {e:?}"); // 发送失败时打印警告
                }
            }
        }
        // info!("Client {:?} disconnected", self.addr); // 客户端断开连接时打印信息
        Ok(())
    }
}

impl<S> ProstClientStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static, // 定义S的trait约束
{
    pub fn new(stream: S) -> Self {
        Self {
            inner: ProstStream::new(stream), // 创建流处理器
        }
    }

    pub async fn execute_unary(
        &mut self,
        cmd: &CommandRequest,
    ) -> Result<CommandResponse, KvError> {
        let stream = &mut self.inner; // 获取流处理器的可变引用
        stream.send(cmd).await?; // 发送命令

        match stream.next().await { // 从流中读取响应
            Some(v) => v, // 成功返回响应
            None => Err(KvError::Internal("Didn't get any response".into())), // 失败返回错误
        }
    }

    pub async fn execute_streaming(self, cmd: &CommandRequest) -> Result<StreamResult, KvError> {
        let mut stream = self.inner; // 获取流处理器的可变引用
        stream.send(cmd).await?; // 发送命令
        stream.close().await?; // 关闭流

        StreamResult::new(stream).await // 创建并返回流结果
    }
}

#[cfg(test)]
pub mod utils {
    use anyhow::Result;
    use bytes::{BufMut, BytesMut};
    use std::{cmp::min, task::Poll};
    use tokio::io::{AsyncRead, AsyncWrite};

    #[derive(Default)]
    pub struct DummyStream {
        pub buf: BytesMut, // 内部缓冲区
    }

    impl AsyncRead for DummyStream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let this = self.get_mut(); // 获取DummyStream的可变引用
            let len = min(buf.capacity(), this.buf.len()); // 计算读取的最大长度
            let data = this.buf.split_to(len); // 从缓冲区中分割出数据
            buf.put_slice(&data); // 将数据写入读取缓冲区
            Poll::Ready(Ok(())) // 返回读取成功
        }
    }

    impl AsyncWrite for DummyStream {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, std::io::Error>> {
            self.get_mut().buf.put_slice(buf); // 将数据写入缓冲区
            Poll::Ready(Ok(buf.len())) // 返回写入的字节数
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Poll::Ready(Ok(())) // 返回刷新成功
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Poll::Ready(Ok(())) // 返回关闭成功
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::*;
    use crate::{assert_res_ok, MemTable, ServiceInner, Value};
    use anyhow::Result;
    use bytes::Bytes;
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn client_server_basic_communication_should_work() -> anyhow::Result<()> {
        let addr = start_server().await?;

        let stream = TcpStream::connect(addr).await?;
        let mut client = ProstClientStream::new(stream);

        // 发送 HSET，等待回应

        let cmd = CommandRequest::new_hset("t1", "k1", "v1".into());
        let res = client.execute_unary(&cmd).await.unwrap();

        // 第一次 HSET 服务器应该返回 None
        assert_res_ok(&res, &[Value::default()], &[]);

        // 再发一个 HGET
        let cmd = CommandRequest::new_hget("t1", "k1");
        let res = client.execute_unary(&cmd).await?;

        // 服务器应该返回上一次的结果
        assert_res_ok(&res, &["v1".into()], &[]);

        // 发一个 SUBSCRIBE
        let cmd = CommandRequest::new_subscribe("chat");
        let res = client.execute_streaming(&cmd).await?;
        let id = res.id;
        assert!(id > 0);

        Ok(())
    }

    #[tokio::test]
    async fn client_server_compression_should_work() -> anyhow::Result<()> {
        let addr = start_server().await?;

        let stream = TcpStream::connect(addr).await?;
        let mut client = ProstClientStream::new(stream);

        let v: Value = Bytes::from(vec![0u8; 16384]).into();
        let cmd = CommandRequest::new_hset("t2", "k2", v.clone());
        let res = client.execute_unary(&cmd).await?;

        assert_res_ok(&res, &[Value::default()], &[]);

        let cmd = CommandRequest::new_hget("t2", "k2");
        let res = client.execute_unary(&cmd).await?;

        assert_res_ok(&res, &[v], &[]);

        Ok(())
    }

    async fn start_server() -> Result<SocketAddr> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let service: Service = ServiceInner::new(MemTable::new()).into();
                let server = ProstServerStream::new(stream, service);
                tokio::spawn(server.process());
            }
        });

        Ok(addr)
    }
}
