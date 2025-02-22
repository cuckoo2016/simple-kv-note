use std::io::Cursor; // 使用标准库的Cursor来读取PEM文件
use std::sync::Arc; // 使用Arc来共享配置

use tokio::io::{AsyncRead, AsyncWrite}; // 使用tokio的异步读写trait
use tokio_rustls::rustls::{internal::pemfile, Certificate, ClientConfig, ServerConfig}; // 使用tokio_rustls的证书和配置
use tokio_rustls::rustls::{AllowAnyAuthenticatedClient, NoClientAuth, PrivateKey, RootCertStore}; // 使用tokio_rustls的客户端认证和根证书存储
use tokio_rustls::webpki::DNSNameRef; // 使用webpki的DNS名称引用
use tokio_rustls::TlsConnector; // 使用tokio_rustls的TLS连接器
use tokio_rustls::{
    client::TlsStream as ClientTlsStream, server::TlsStream as ServerTlsStream, TlsAcceptor,
}; // 使用tokio_rustls的TLS流
use tracing::instrument; // 使用tracing库的instrument宏

use crate::KvError; // 使用自定义的错误类型

/// KV Server 自己的 ALPN (Application-Layer Protocol Negotiation)
const ALPN_KV: &str = "kv"; // 定义KV Server的ALPN

/// 存放 TLS ServerConfig 并提供方法 accept 把底层的协议转换成 TLS
#[derive(Clone)]
pub struct TlsServerAcceptor {
    inner: Arc<ServerConfig>, // 使用Arc来共享ServerConfig
}

/// 存放 TLS Client 并提供方法 connect 把底层的协议转换成 TLS
#[derive(Clone)]
pub struct TlsClientConnector {
    pub config: Arc<ClientConfig>, // 使用Arc来共享ClientConfig
    pub domain: Arc<String>, // 使用Arc来共享域名
}

impl TlsClientConnector {
    /// 加载 client cert / CA cert，生成 ClientConfig
    #[instrument(name = "tls_connector_new", skip_all)]
    pub fn new(
        domain: impl Into<String> + std::fmt::Debug, // 接受域名
        identity: Option<(&str, &str)>, // 接受客户端证书和密钥
        server_ca: Option<&str>, // 接受服务器CA证书
    ) -> Result<Self, KvError> {
        // config可理解为证书链
        let mut config = ClientConfig::new(); // 创建新的ClientConfig

        // 如果有客户端证书，加载之
        if let Some((cert, key)) = identity {
            let certs = load_certs(cert)?; // 加载客户端证书
            let key = load_key(key)?; // 加载客户端密钥
            config.set_single_client_cert(certs, key)?; // 设置客户端证书和密钥
        }

        // 如果有签署服务器的 CA 证书，则加载它，这样服务器证书不在根证书链
        // 但是这个 CA 证书能验证它，也可以
        if let Some(cert) = server_ca {
            let mut buf = Cursor::new(cert); // 创建Cursor来读取CA证书
            config.root_store.add_pem_file(&mut buf).unwrap(); // 添加CA证书到根证书存储
        } else {
            // 加载本地信任的根证书链
            config.root_store = match rustls_native_certs::load_native_certs() {
                Ok(store) | Err((Some(store), _)) => store,
                Err((None, error)) => return Err(error.into()),
            };
        }

        Ok(Self {
            config: Arc::new(config), // 使用Arc来共享配置
            domain: Arc::new(domain.into()), // 使用Arc来共享域名
        })
    }

    #[instrument(name = "tls_client_connect", skip_all)]
    /// 触发 TLS 协议，把底层的 stream 转换成 TLS stream
    pub async fn connect<S>(&self, stream: S) -> Result<ClientTlsStream<S>, KvError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let dns = DNSNameRef::try_from_ascii_str(self.domain.as_str())
            .map_err(|_| KvError::Internal("Invalid DNS name".into()))?; // 尝试从域名字符串创建DNS名称引用

        let stream = TlsConnector::from(self.config.clone())
            .connect(dns, stream)
            .await?; // 使用TlsConnector连接到服务器

        Ok(stream) // 返回TLS流
    }
}

impl TlsServerAcceptor {
    /// 加载 server cert / CA cert，生成 ServerConfig
    #[instrument(name = "tls_acceptor_new", skip_all)]
    pub fn new(cert: &str, key: &str, client_ca: Option<&str>) -> Result<Self, KvError> {
        let certs = load_certs(cert)?; // 加载服务器证书
        let key = load_key(key)?; // 加载服务器密钥

        let mut config = match client_ca {
            None => ServerConfig::new(NoClientAuth::new()), // 如果没有客户端CA证书，则不需要客户端认证
            Some(client_cert) => {
                let mut client_root_cert_store = RootCertStore::empty(); // 创建空的根证书存储
                // 如果客户端证书是某个 CA 证书签发的，则把这个 CA 证书加载到信任链中
                let mut cert = Cursor::new(client_cert); // 创建Cursor来读取CA证书
                client_root_cert_store
                    .add_pem_file(&mut cert)
                    .map_err(|_| KvError::CertifcateParseError("CA", "cert"))?;

                // 创建允许任何已认证的客户端的认证策略
                let client_auth = AllowAnyAuthenticatedClient::new(client_root_cert_store); 
                ServerConfig::new(client_auth) // 创建ServerConfig
            }
        };

        // 加载服务器证书
        config
            .set_single_cert(certs, key)
            .map_err(|_| KvError::CertifcateParseError("server", "cert"))?;
        config.set_protocols(&[Vec::from(ALPN_KV)]); // 设置ALPN协议

        Ok(Self {
            inner: Arc::new(config), // 使用Arc来共享配置
        })
    }

    #[instrument(name = "tls_server_accept", skip_all)]
    /// 触发 TLS 协议，把底层的 stream 转换成 TLS stream
    pub async fn accept<S>(&self, stream: S) -> Result<ServerTlsStream<S>, KvError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let acceptor = TlsAcceptor::from(self.inner.clone()); // 创建TlsAcceptor
        Ok(acceptor.accept(stream).await?) // 接受TLS流
    }
}

fn load_certs(cert: &str) -> Result<Vec<Certificate>, KvError> {
    let mut cert = Cursor::new(cert); // 创建Cursor来读取证书
    pemfile::certs(&mut cert).map_err(|_| KvError::CertifcateParseError("server", "cert")) // 加载证书
}

fn load_key(key: &str) -> Result<PrivateKey, KvError> {
    let mut cursor = Cursor::new(key); // 创建Cursor来读取密钥

    // 先尝试用 PKCS8 加载私钥
    if let Ok(mut keys) = pemfile::pkcs8_private_keys(&mut cursor) {
        if !keys.is_empty() {
            return Ok(keys.remove(0)); // 返回私钥
        }
    }

    // 再尝试加载 RSA key
    cursor.set_position(0); // 重置Cursor位置
    if let Ok(mut keys) = pemfile::rsa_private_keys(&mut cursor) {
        if !keys.is_empty() {
            return Ok(keys.remove(0)); // 返回私钥
        }
    }

    // 不支持的私钥类型
    Err(KvError::CertifcateParseError("private", "key")) // 返回错误
}

#[cfg(test)]
pub mod tls_utils {
    use crate::{KvError, TlsClientConnector, TlsServerAcceptor};

    const CA_CERT: &str = include_str!("../../fixtures/ca.cert"); // 定义CA证书
    const CLIENT_CERT: &str = include_str!("../../fixtures/client.cert"); // 定义客户端证书
    const CLIENT_KEY: &str = include_str!("../../fixtures/client.key"); // 定义客户端密钥
    const SERVER_CERT: &str = include_str!("../../fixtures/server.cert"); // 定义服务器证书
    const SERVER_KEY: &str = include_str!("../../fixtures/server.key"); // 定义服务器密钥

    pub fn tls_connector(client_cert: bool) -> Result<TlsClientConnector, KvError> {
        let ca = Some(CA_CERT); // 定义CA证书
        let client_identity = Some((CLIENT_CERT, CLIENT_KEY)); // 定义客户端证书和密钥

        match client_cert {
            false => TlsClientConnector::new("kvserver.acme.inc", None, ca), // 创建不带客户端证书的连接器
            true => TlsClientConnector::new("kvserver.acme.inc", client_identity, ca), // 创建带客户端证书的连接器
        }
    }

    pub fn tls_acceptor(client_cert: bool) -> Result<TlsServerAcceptor, KvError> {
        let ca = Some(CA_CERT); // 定义CA证书
        match client_cert {
            true => TlsServerAcceptor::new(SERVER_CERT, SERVER_KEY, ca), // 创建带客户端CA证书的接受器
            false => TlsServerAcceptor::new(SERVER_CERT, SERVER_KEY, None), // 创建不带客户端CA证书的接受器
        }
    }
}

#[cfg(test)]
mod tests {
    use super::tls_utils::tls_acceptor;
    use crate::network::tls::tls_utils::tls_connector;
    use anyhow::Result;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    #[tokio::test]
    async fn tls_should_work() -> Result<()> {
        let addr = start_server(false).await?;
        let connector = tls_connector(false)?;
        let stream = TcpStream::connect(addr).await?;
        let mut stream = connector.connect(stream).await?;
        stream.write_all(b"hello world!").await?;
        let mut buf = [0; 12];
        stream.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"hello world!");

        Ok(())
    }

    #[tokio::test]
    async fn tls_with_client_cert_should_work() -> Result<()> {
        let addr = start_server(true).await?;
        let connector = tls_connector(true)?;
        let stream = TcpStream::connect(addr).await?;
        let mut stream = connector.connect(stream).await?;
        stream.write_all(b"hello world!").await?;
        let mut buf = [0; 12];
        stream.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"hello world!");

        Ok(())
    }

    #[tokio::test]
    async fn tls_with_bad_domain_should_not_work() -> Result<()> {
        let addr = start_server(false).await?;

        let mut connector = tls_connector(false)?;
        connector.domain = Arc::new("kvserver1.acme.inc".into());
        let stream = TcpStream::connect(addr).await?;
        let result = connector.connect(stream).await;

        assert!(result.is_err());

        Ok(())
    }

    async fn start_server(client_cert: bool) -> Result<SocketAddr> {
        let acceptor = tls_acceptor(client_cert)?;

        let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = echo.local_addr().unwrap();

        tokio::spawn(async move {
            let (stream, _) = echo.accept().await.unwrap();
            let mut stream = acceptor.accept(stream).await.unwrap();
            let mut buf = [0; 12];
            stream.read_exact(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();
        });

        Ok(addr)
    }
}
