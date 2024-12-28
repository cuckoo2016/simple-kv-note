// 导入必要的库和类型
use std::{env, str::FromStr};

// 导入错误处理库
use anyhow::Result;
// 导入简单键值存储库
use simple_kv::{start_server_with_config, RotationConfig, ServerConfig};
// 导入异步文件操作库
use tokio::fs;
// 导入日志跟踪库
use tracing::span;
// 导入日志订阅者库
use tracing_subscriber::{
    filter,
    fmt::{self, format},
    layer::SubscriberExt,
    prelude::*,
    EnvFilter,
};

// 定义异步主函数
#[tokio::main]
async fn main() -> Result<()> {
    // 从环境变量读取配置文件路径，如果不存在则使用默认配置
    let config = match env::var("KV_SERVER_CONFIG") {
        Ok(path) => fs::read_to_string(&path).await?, // 从文件读取配置
        Err(_) => include_str!("../fixtures/quic_server.conf").to_string(), // 使用默认配置
    };

    // 将配置字符串反序列化为ServerConfig结构
    let config: ServerConfig = toml::from_str(&config)?;

    // 设置日志级别
    let log = &config.log;
    env::set_var("RUST_LOG", &log.log_level); // 设置环境变量中的日志级别

    // ------------控制台日志层
    // 创建日志输出到标准输出的层
    let stdout_log = fmt::layer().compact();
    // 使用环境变量中的日志过滤器
    let env_level = EnvFilter::from_default_env();

    // ------------文件日志层
    // 根据日志轮转配置创建文件日志追加器
    let file_appender = match log.rotation {
        RotationConfig::Hourly => tracing_appender::rolling::hourly(&log.path, "server.log"), // 每小时轮转
        RotationConfig::Daily => tracing_appender::rolling::daily(&log.path, "server.log"), // 每天轮转
        RotationConfig::Never => tracing_appender::rolling::never(&log.path, "server.log"), // 从不轮转
    };

    // 创建非阻塞的文件日志追加器
    let (non_blocking, _guard1) = tracing_appender::non_blocking(file_appender);
    // 创建格式化日志层
    let fmt_layer = fmt::layer()
        .event_format(format().compact())
        .with_writer(non_blocking);

    // 根据日志级别和是否启用文件日志，创建日志过滤器
    let level = filter::LevelFilter::from_str(&log.log_level)?;
    let log_file_level = match log.enable_log_file {
        true => level,                     // 如果启用文件日志，则使用日志级别
        false => filter::LevelFilter::OFF, // 如果不启用文件日志，则不记录日志
    };

    // ------------opentelemetry日志层
    // 创建Jaeger追踪器
    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_service_name("kv-server")
        .install_simple()?;
    // 创建OpenTelemetry追踪器层
    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let jaeger_level = match log.enable_log_file {
        true => level,                     // 如果启用文件日志，则使用日志级别
        false => filter::LevelFilter::OFF, // 如果不启用文件日志，则不记录日志
    };

    // 初始化日志订阅者
    tracing_subscriber::registry()
        .with(stdout_log.with_filter(env_level)) // 添加标准输出日志层
        .with(fmt_layer.with_filter(log_file_level)) // 添加文件日志层
        .with(opentelemetry.with_filter(jaeger_level)) // 添加OpenTelemetry追踪器层
        .init();

    // 创建应用程序启动的跟踪Span
    let root = span!(tracing::Level::INFO, "app_start");
    let _enter = root.enter();

    // 使用配置启动服务器
    start_server_with_config(&config).await?;

    Ok(())
}
