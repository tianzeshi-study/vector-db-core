## usage 

```

use chrono::{DateTime, Local, Utc};
use serde::{
    Deserialize,
    Serialize,
};
use std::time::Instant;
use vector_db_core::*;

const COUNT: usize = 1000000;

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct ChatMessage {
    pub message_id: i32,            
    pub sender: String,         
    pub content: String, 
    receiver: String,
    #[serde(with = "chrono::serde::ts_seconds")]
timestamp: DateTime<Utc>,
}


fn main() {
    let mut objs = Vec::new();
    // let db: ReadableCache<WritableCache<DynamicVectorManageService<ChatMessage>, ChatMessage>, ChatMessage> =
    // let db: ReadableCache<DynamicVectorManageService<ChatMessage>, ChatMessage> =
    // let db: WritableCache<DynamicVectorManageService<ChatMessage>, ChatMessage> =
    let db: DynamicVectorManageService<ChatMessage> =
                VectorEngine::<ChatMessage>::new(
                    "index.bin".to_string(),
                    "data.bin".to_string(),
                    1024* 1024,
                );
    for i in 0..COUNT {
        let my_obj = ChatMessage {
            message_id: i as i32,
            sender: format!("sender {}", i).to_string(),
            content: format!("hello, world!  这是地{}条消息", i+1).to_string(),
            receiver: format!("receiver {}", i).to_string(),
            timestamp: Local::now().with_timezone(&Utc),
        };

        objs.push(my_obj);
    }
    let start = Instant::now(); // 记录开始时间
    db.pushx(objs);
    let duration = start.elapsed(); 
    println!("extend   {} items   took: {:?}", COUNT, duration);
    
    let start = Instant::now();
    let objs = db.getx(0, COUNT as u64).unwrap();
let getx_duration = start.elapsed();
    println!("load {} items   took: {:?}", objs.len(), getx_duration);

let start = Instant::now();
    let objs = db.getall().unwrap();
let getall_duration = start.elapsed();
    println!("get all {} items   took: {:?}", objs.len(), getall_duration);

let last_obj = db.get(objs.len() as u64 -1 ).unwrap();
dbg!(last_obj);
}

```
