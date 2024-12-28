// 导入必要的模块和类型
use crate::{
    command_request::RequestData, CommandRequest, CommandResponse, KvError, MemTable, Storage,
};
// 导入futures库中的stream模块，用于处理异步流
use futures::stream;
// 导入Arc，用于共享所有权
use std::sync::Arc;
// 导入tracing库中的debug和instrument宏，用于日志记录和函数调用跟踪
use tracing::{debug, instrument};

// 定义子模块
mod command_service;
mod topic;
mod topic_service;

// 公开使用topic模块中的类型
pub use topic::{Broadcaster, Topic};
// 公开使用topic_service模块中的类型
pub use topic_service::{StreamingResponse, TopicService};

/// 定义一个trait，用于处理Command请求
pub trait CommandService {
    /// 处理Command请求，返回Response
    fn execute(self, store: &impl Storage) -> CommandResponse;
}

/// 定义一个trait，用于不可变事件通知(这里Arg不可修改)
pub trait Notify<Arg> {
    /// 发送不可变事件通知
    fn notify(&self, arg: &Arg);
}

/// 定义一个trait，用于可变事件通知(这里Arg可修改)
pub trait NotifyMut<Arg> {
    /// 发送可变事件通知
    fn notify(&self, arg: &mut Arg);
}

// 实现Notify trait，用于不可变事件通知
impl<Arg> Notify<Arg> for Vec<fn(&Arg)> {
    #[inline]
    fn notify(&self, arg: &Arg) {
        // 遍历所有注册的回调函数，并将事件参数传递给它们
        for f in self {
            f(arg)
        }
    }
}

// 实现NotifyMut trait，用于可变事件通知
impl<Arg> NotifyMut<Arg> for Vec<fn(&mut Arg)> {
    #[inline]
    fn notify(&self, arg: &mut Arg) {
        // 遍历所有注册的回调函数，并将可变事件参数传递给它们
        for f in self {
            f(arg)
        }
    }
}

/// 定义Service数据结构
/// 定义的时候，无需声明泛型的约束，但可指定默认实现
pub struct Service<Store = MemTable> {
    // 使用Arc来共享ServiceInner实例的所有权
    inner: Arc<ServiceInner<Store>>,
    // 使用Arc来共享Broadcaster实例的所有权
    broadcaster: Arc<Broadcaster>,
}

// 实现Clone trait，用于克隆Service实例
impl<Store> Clone for Service<Store> {
    fn clone(&self) -> Self {
        // 克隆Service实例，包括ServiceInner和Broadcaster
        Self {
            inner: Arc::clone(&self.inner),
            broadcaster: Arc::clone(&self.broadcaster),
        }
    }
}

/// 定义Service内部数据结构
/// 定义的时候，无需声明泛型的约束，但可指定默认实现
pub struct ServiceInner<Store> {
    // 存储实例
    store: Store,
    // 保存处理请求时的回调函数
    on_received: Vec<fn(&CommandRequest)>,
    // 保存处理响应时的回调函数
    on_executed: Vec<fn(&CommandResponse)>,
    // 保存在发送响应前修改响应的回调函数
    on_before_send: Vec<fn(&mut CommandResponse)>,
    // 保存在发送响应后执行的回调函数
    on_after_send: Vec<fn()>,
}

// 实现ServiceInner的方法
impl<Store: Storage> ServiceInner<Store> {
    // 创建ServiceInner实例
    pub fn new(store: Store) -> Self {
        Self {
            store,
            on_received: Vec::new(),
            on_executed: Vec::new(),
            on_before_send: Vec::new(),
            on_after_send: Vec::new(),
        }
    }

    // 注册处理请求时的回调函数
    pub fn fn_received(mut self, f: fn(&CommandRequest)) -> Self {
        self.on_received.push(f);
        self
    }

    // 注册处理响应时的回调函数
    pub fn fn_executed(mut self, f: fn(&CommandResponse)) -> Self {
        self.on_executed.push(f);
        self
    }

    // 注册在发送响应前修改响应的回调函数
    pub fn fn_before_send(mut self, f: fn(&mut CommandResponse)) -> Self {
        self.on_before_send.push(f);
        self
    }

    // 注册在发送响应后执行的回调函数
    pub fn fn_after_send(mut self, f: fn()) -> Self {
        self.on_after_send.push(f);
        self
    }
}

// 实现From trait，用于从ServiceInner转换为Service
impl<Store: Storage> From<ServiceInner<Store>> for Service<Store> {
    fn from(inner: ServiceInner<Store>) -> Self {
        // 创建Service实例，使用Arc共享ServiceInner和Broadcaster的所有权
        Self {
            inner: Arc::new(inner),
            broadcaster: Default::default(),
        }
    }
}

// 实现Service的方法
impl<Store: Storage> Service<Store> {
    #[instrument(name = "service_execute", skip_all)]
    pub fn execute(&self, cmd: CommandRequest) -> StreamingResponse {
        // 调试日志，记录接收到的请求
        debug!("Got request: {:?}", cmd);
        // 通知所有注册的处理请求时的回调函数
        self.inner.on_received.notify(&cmd);
        // 调用dispatch函数处理请求，得到响应
        let mut res: CommandResponse = dispatch(cmd.clone(), &self.inner.store);

        // 如果响应为空，则调用dispatch_stream处理流
        if res == CommandResponse::default() {
            dispatch_stream(cmd, Arc::clone(&self.broadcaster))
        } else {
            // 调试日志，记录执行的响应
            debug!("Executed response: {:?}", res);
            // 通知所有注册的处理响应时的回调函数
            self.inner.on_executed.notify(&res);
            // 通知所有注册的在发送响应前修改响应的回调函数
            self.inner.on_before_send.notify(&mut res);
            // 如果有回调函数修改了响应，则记录调试日志
            if !self.inner.on_before_send.is_empty() {
                debug!("Modified response: {:?}", res);
            }

            // 返回响应的异步流
            Box::pin(stream::once(async { Arc::new(res) }))
        }
    }
}

/// 从Request中得到Response，处理HGET/HSET/HDEL/HEXIST等命令
pub fn dispatch(cmd: CommandRequest, store: &impl Storage) -> CommandResponse {
    // 根据请求数据的类型，调用对应的execute方法处理请求
    match cmd.request_data {
        Some(RequestData::Hget(param)) => param.execute(store),
        Some(RequestData::Hgetall(param)) => param.execute(store),
        Some(RequestData::Hmget(param)) => param.execute(store),
        Some(RequestData::Hset(param)) => param.execute(store),
        Some(RequestData::Hmset(param)) => param.execute(store),
        Some(RequestData::Hdel(param)) => param.execute(store),
        Some(RequestData::Hmdel(param)) => param.execute(store),
        Some(RequestData::Hexist(param)) => param.execute(store),
        Some(RequestData::Hmexist(param)) => param.execute(store),
        None => KvError::InvalidCommand("Request has no data".into()).into(),
        // 如果请求数据类型不匹配任何已知的命令，则返回默认的Response
        _ => CommandResponse::default(),
    }
}

/// 从Request中得到Response，处理PUBLISH/SUBSCRIBE/UNSUBSCRIBE等命令
pub fn dispatch_stream(cmd: CommandRequest, topic: impl Topic) -> StreamingResponse {
    // 根据请求数据的类型，调用对应的execute方法处理请求
    match cmd.request_data {
        Some(RequestData::Publish(param)) => param.execute(topic),
        Some(RequestData::Subscribe(param)) => param.execute(topic),
        Some(RequestData::Unsubscribe(param)) => param.execute(topic),
        // 如果请求数据类型不匹配任何已知的流命令，则直接panic
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use http::StatusCode;
    use tokio_stream::StreamExt;
    use tracing::info;

    use super::*;
    use crate::{MemTable, Value};

    #[tokio::test]
    async fn service_should_works() {
        // 创建一个Service实例，至少包含一个Storage
        let service: Service = ServiceInner::new(MemTable::default()).into();

        // 测试Service实例的克隆是否是轻量级的
        let cloned = service.clone();

        // 创建一个线程，在table t1 中写入 k1, v1
        tokio::spawn(async move {
            let mut res = cloned.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
            let data = res.next().await.unwrap();
            assert_res_ok(&data, &[Value::default()], &[]);
        })
        .await
        .unwrap();

        // 在当前线程下读取 table t1 的 k1，应该返回 v1
        let mut res = service.execute(CommandRequest::new_hget("t1", "k1"));
        let data = res.next().await.unwrap();
        assert_res_ok(&data, &["v1".into()], &[]);
    }

    #[tokio::test]
    async fn event_registration_should_work() {
        // 定义回调函数
        fn b(cmd: &CommandRequest) {
            info!("Got {:?}", cmd);
        }
        fn c(res: &CommandResponse) {
            info!("{:?}", res);
        }
        fn d(res: &mut CommandResponse) {
            res.status = StatusCode::CREATED.as_u16() as _;
        }
        fn e() {
            info!("Data is sent");
        }

        // 创建一个Service实例，并注册回调函数
        let service: Service = ServiceInner::new(MemTable::default())
            .fn_received(|_: &CommandRequest| {})
            .fn_received(b)
            .fn_executed(c)
            .fn_before_send(d)
            .fn_after_send(e)
            .into();

        // 执行一个请求，并断言响应的状态码
        let mut res = service.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
        let data = res.next().await.unwrap();
        assert_eq!(data.status, StatusCode::CREATED.as_u16() as u32);
        assert_eq!(data.message, "");
        assert_eq!(data.values, vec![Value::default()]);
    }
}

#[cfg(test)]
use crate::{Kvpair, Value};

// 测试成功返回的结果
#[cfg(test)]
pub fn assert_res_ok(res: &CommandResponse, values: &[Value], pairs: &[Kvpair]) {
    let mut sorted_pairs = res.pairs.clone();
    sorted_pairs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(res.status, 200);
    assert_eq!(res.message, "");
    assert_eq!(res.values, values);
    assert_eq!(sorted_pairs, pairs);
}

// 测试失败返回的结果
#[cfg(test)]
pub fn assert_res_error(res: &CommandResponse, code: u32, msg: &str) {
    assert_eq!(res.status, code);
    assert!(res.message.contains(msg));
    assert_eq!(res.values, &[]);
    assert_eq!(res.pairs, &[]);
}
