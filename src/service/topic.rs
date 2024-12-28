use dashmap::{DashMap, DashSet}; // 导入dashmap库中的DashMap和DashSet
use std::sync::{ // 导入标准库中的sync模块
    atomic::{AtomicU32, Ordering}, // 导入原子类型AtomicU32和Ordering
    Arc, // 导入原子引用计数类型Arc
};
use tokio::sync::mpsc; // 导入tokio库中的sync模块中的mpsc
use tracing::{debug, info, instrument, warn}; // 导入tracing库中的debug、info、instrument和warn

use crate::{CommandResponse, KvError, Value}; // 导入当前crate中的CommandResponse、KvError和Value

/// topic 里最大存放的数据
const BROADCAST_CAPACITY: usize = 128; // 定义常量BROADCAST_CAPACITY，表示topic中最大存放的数据量

/// 下一个 subscription id
static NEXT_ID: AtomicU32 = AtomicU32::new(1); // 定义静态变量NEXT_ID，表示下一个subscription id

/// 获取下一个 subscription id
fn get_next_subscription_id() -> u32 { // 定义函数get_next_subscription_id，用于获取下一个subscription id
    NEXT_ID.fetch_add(1, Ordering::Relaxed) // 使用原子操作获取下一个subscription id
}

pub trait Topic: Send + Sync + 'static { // 定义一个Topic trait，要求实现Send、Sync和'static特性
    /// 订阅某个主题
    fn subscribe(self, name: String) -> mpsc::Receiver<Arc<CommandResponse>>; // 定义订阅方法，返回一个mpsc::Receiver
    /// 取消对主题的订阅
    fn unsubscribe(self, name: String, id: u32) -> Result<u32, KvError>; // 定义取消订阅方法，返回一个Result
    /// 往主题里发布一个数据
    fn publish(self, name: String, value: Arc<CommandResponse>); // 定义发布方法
}

/// 用于主题发布和订阅的数据结构
#[derive(Default)] // 定义一个默认的数据结构Broadcaster
pub struct Broadcaster {
    /// 所有的主题列表
    topics: DashMap<String, DashSet<u32>>, // 定义一个DashMap，用于存储所有的主题列表
    /// 所有的订阅列表
    subscriptions: DashMap<u32, mpsc::Sender<Arc<CommandResponse>>>, // 定义一个DashMap，用于存储所有的订阅列表
}

impl Topic for Arc<Broadcaster> { // 实现Topic trait for Arc<Broadcaster>
    #[instrument(name = "topic_subscribe", skip_all)] // 使用instrument宏来自动记录函数的调用和返回
    fn subscribe(self, name: String) -> mpsc::Receiver<Arc<CommandResponse>> { // 实现订阅方法
        let id = { // 生成一个新的subscription id
            let entry = self.topics.entry(name).or_default(); // 在topics表中查找或创建一个新的entry
            let id = get_next_subscription_id(); // 获取下一个subscription id
            entry.value().insert(id); // 将id插入entry的value中
            id // 返回id
        };

        // 生成一个 mpsc channel
        let (tx, rx) = mpsc::channel(BROADCAST_CAPACITY); // 创建一个mpsc channel

        let v: Value = (id as i64).into(); // 将id转换为i64，然后转换为Value

        // 立刻发送 subscription id 到 rx
        let tx1 = tx.clone(); // 克隆tx
        tokio::spawn(async move { // 在新的任务中执行
            if let Err(e) = tx1.send(Arc::new(v.into())).await { // 发送subscription id
                // TODO: 这个很小概率发生，但目前我们没有善后
                warn!("Failed to send subscription id: {}. Error: {:?}", id, e); // 如果发送失败，打印警告
            }
        });

        // 把 tx 存入 subscription table
        self.subscriptions.insert(id, tx); // 将tx插入subscriptions表中
        debug!("Subscription {} is added", id); // 打印调试信息

        // 返回 rx 给网络处理的上下文
        rx // 返回rx
    }

    #[instrument(name = "topic_unsubscribe", skip_all)] // 使用instrument宏来自动记录函数的调用和返回
    fn unsubscribe(self, name: String, id: u32) -> Result<u32, KvError> { // 实现取消订阅方法
        match self.remove_subscription(name, id) { // 调用remove_subscription方法
            Some(id) => Ok(id), // 如果成功，返回Ok
            None => Err(KvError::NotFound(format!("subscription {}", id))), // 如果失败，返回错误
        }
    }

    #[instrument(name = "topic_publish", skip_all)] // 使用instrument宏来自动记录函数的调用和返回
    fn publish(self, name: String, value: Arc<CommandResponse>) { // 实现发布方法
        tokio::spawn(async move { // 在新的任务中执行
            let mut ids = vec![]; // 定义一个空的ids数组
            if let Some(topic) = self.topics.get(&name) { // 在topics表中查找主题
                // 复制整个 topic 下所有的 subscription id
                // 这里我们每个 id 是 u32，如果一个 topic 下有 10k 订阅，复制的成本
                // 也就是 40k 堆内存（外加一些控制结构），所以效率不算差
                // 这也是为什么我们用 NEXT_ID 来控制 subscription id 的生成

                let subscriptions = topic.value().clone(); // 复制topic的value
                // 尽快释放锁
                drop(topic); // 释放topic的锁，topic的任务完成了

                // 循环发送
                for id in subscriptions.into_iter() { // 遍历subscriptions
                    if let Some(tx) = self.subscriptions.get(&id) { // 在subscriptions表中查找tx
                        if let Err(e) = tx.send(value.clone()).await { // 发送数据
                            warn!("Publish to {} failed! error: {:?}", id, e); // 如果发送失败，打印警告
                            // client 中断连接
                            ids.push(id); // 将id添加到ids数组中
                        }
                    }
                }
            }

            // 处理失效的订阅者
            for id in ids { // 遍历ids
                self.remove_subscription(name.clone(), id); // 调用remove_subscription方法
            }
        });
    }
}

impl Broadcaster { // 实现Broadcaster
    pub fn remove_subscription(&self, name: String, id: u32) -> Option<u32> { // 定义remove_subscription方法
        if let Some(v) = self.topics.get_mut(&name) { // 在topics表中查找主题
            // 在 topics 表里找到 topic 的 subscription id，删除
            v.remove(&id); // 删除id

            // 如果这个 topic 为空，则也删除 topic
            if v.is_empty() { // 如果v为空
                info!("Topic: {:?} is deleted", &name); // 打印信息
                drop(v); // 释放v
                self.topics.remove(&name); // 删除主题
            }
        }

        debug!("Subscription {} is removed!", id); // 打印调试信息
        // 在 subscription 表中同样删除
        self.subscriptions.remove(&id).map(|(id, _)| id) // 删除subscriptions表中的id
    }
}

#[cfg(test)] // 如果是测试模式
mod tests { // 定义测试模块
    use std::convert::TryInto; // 导入标准库中的convert模块中的TryInto

    use tokio::sync::mpsc::Receiver; // 导入tokio库中的sync模块中的mpsc::Receiver

    use crate::assert_res_ok; // 导入当前crate中的assert_res_ok

    use super::*; // 导入当前模块中的所有内容

    #[tokio::test] // 使用tokio的测试宏
    async fn pub_sub_should_work() { // 定义一个测试函数
        let b = Arc::new(Broadcaster::default()); // 创建一个新的Broadcaster
        let lobby = "lobby".to_string(); // 定义一个lobby字符串

        // subscribe
        let mut stream1 = b.clone().subscribe(lobby.clone()); // 订阅lobby
        let mut stream2 = b.clone().subscribe(lobby.clone()); // 订阅lobby

        // publish
        let v: Value = "hello".into(); // 定义一个新的Value
        b.clone().publish(lobby.clone(), Arc::new(v.clone().into())); // 发布数据

        // subscribers 应该能收到 publish 的数据
        let id1 = get_id(&mut stream1).await; // 获取stream1的id
        let id2 = get_id(&mut stream2).await; // 获取stream2的id

        assert!(id1 != id2); // 断言id1不等于id2

        let res1 = stream1.recv().await.unwrap(); // 获取stream1的结果
        let res2 = stream2.recv().await.unwrap(); // 获取stream2的结果

        assert_eq!(res1, res2); // 断言res1等于res2
        assert_res_ok(&res1, &[v.clone()], &[]); // 断言res1是Ok，且数据正确

        // 如果 subscriber 取消订阅，则收不到新数据
        let result = b.clone().unsubscribe(lobby.clone(), id1 as _).unwrap(); // 取消订阅
        assert_eq!(result, id1 as _); // 断言取消订阅成功

        // publish
        let v: Value = "world".into(); // 定义一个新的Value
        b.clone().publish(lobby.clone(), Arc::new(v.clone().into())); // 发布数据

        assert!(stream1.recv().await.is_none()); // 断言stream1收不到新数据
        let res2 = stream2.recv().await.unwrap(); // 获取stream2的结果
        assert_res_ok(&res2, &[v.clone()], &[]); // 断言res2是Ok，且数据正确
    }

    pub async fn get_id(res: &mut Receiver<Arc<CommandResponse>>) -> u32 { // 定义一个异步函数，用于获取id
        let id: i64 = res.recv().await.unwrap().as_ref().try_into().unwrap(); // 从res中获取id
        id as u32 // 返回id
    }
}
