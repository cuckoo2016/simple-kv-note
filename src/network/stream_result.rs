use std::{
    convert::TryInto, // 使用TryInto trait来进行类型转换
    ops::{Deref, DerefMut}, // 使用Deref和DerefMut trait来实现对StreamResult的引用和可变引用
    pin::Pin, // 使用Pin来固定所有权
};

use futures::{Stream, StreamExt}; // 使用Stream和StreamExt trait来处理异步流

use crate::{CommandResponse, KvError}; // 使用CommandResponse和KvError类型

/// 创建时之间取得 subscription id，并使用 Deref/DerefMut 使其用起来和 Stream 一致
pub struct StreamResult {
    pub id: u32, // 订阅ID
    inner: Pin<Box<dyn Stream<Item = Result<CommandResponse, KvError>> + Send>>, // 内部流处理器
}

impl StreamResult {
    pub async fn new<T>(mut stream: T) -> Result<Self, KvError>
    where
        T: Stream<Item = Result<CommandResponse, KvError>> + Send + Unpin + 'static, // 定义T的trait约束
    {
        // 当前流分解:ID+剩余流
        let id = match stream.next().await {
            Some(Ok(CommandResponse {
                status: 200,
                values: v,
                ..
            })) => {
                if v.is_empty() {
                    return Err(KvError::Internal("Invalid stream".into())); // 如果流的值为空，则返回错误
                }
                let id: i64 = (&v[0]).try_into().unwrap(); // 尝试将流的第一个值转换为i64
                Ok(id as u32) // 将i64转换为u32并返回
            }
            _ => Err(KvError::Internal("Invalid stream".into())), // 如果流的状态不是200或流的值不是Ok，则返回错误
        };

        Ok(StreamResult {
            inner: Box::pin(stream), // 将流转换为Pin<Box<dyn Stream>>并存储
            id: id?, // 存储订阅ID
        })
    }
}

impl Deref for StreamResult {
    type Target = Pin<Box<dyn Stream<Item = Result<CommandResponse, KvError>> + Send>>;

    fn deref(&self) -> &Self::Target {
        &self.inner // 返回内部流处理器的引用
    }
}

impl DerefMut for StreamResult {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner // 返回内部流处理器的可变引用
    }
}
