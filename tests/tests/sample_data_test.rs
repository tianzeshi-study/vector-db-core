
use serde::{
    Deserialize,
    Serialize,
};
use std::time::Instant;
use vector_db_core::*;

const COUNT: usize = 1000000;

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct SampleData {
    pub my_number1: i32,            // 整数类型
    pub my_string1: String,         // 字符串类型，默认为空字符串
    pub my_number2: i32,            // 整数类型
    pub my_boolean1: bool,          // 布尔类型
    pub my_string2: Option<String>, // 可选字符串，可以为空
}

#[test]
fn test_save_sample_one() {
    let i = 4399;

    let my_obj = SampleData {
        my_number1: i as i32,
        my_string1: format!("Hello, World! 你好世界 {}", i).to_string(),
        my_number2: i as i32 * 10,
        my_boolean1: i % 2 == 0,
        my_string2: Some(format!("This is another longer string. {}", i).to_string()),
    };
    let mut my_service = DynamicVectorManageService::<SampleData>::new(
        "sampleData.bin".to_string(),
        "StringDynamicsampleData.bin".to_string(),
        1024,
    )
    .unwrap();
    my_service.save(my_obj);
}

#[test]
fn test_load_sample_one() {
    test_save_sample_one();
    let my_service = DynamicVectorManageService::<SampleData>::new(
        "sampleData.bin".to_string(),
        "StringDynamicsampleData.bin".to_string(),
        1024,
    )
    .unwrap();
    let i = 0;
    // let obj  =  my_service.load(COUNT as u64 );
    let obj = my_service.load(i as u64);
    println!("read one obj at {}: {:?}", i, obj);
}

#[test]
fn test_save_sample_bulk() {
    let mut objs = Vec::new();
    let mut my_service = DynamicVectorManageService::<SampleData>::new(
        "sampleData.bin".to_string(),
        "StringDynamicsampleData.bin".to_string(),
        1024,
    )
    .unwrap();
    for i in 0..COUNT {
        let my_obj = SampleData {
            my_number1: i as i32,
            my_string1: format!("Hello, World! 你好世界 {}", i).to_string(),
            my_number2: i as i32 * 10,
            my_boolean1: i % 2 == 0,
            my_string2: Some(format!("This is another longer string. {}", i).to_string()),
        };

        objs.push(my_obj.clone());
    }
    let start = Instant::now(); // 记录开始时间
    my_service.save_bulk(objs);
    let duration = start.elapsed(); // 计算时间差
    println!("save  {} items   took: {:?}", COUNT, duration);
}

#[test]
fn test_load_sample_bulk() {
    test_save_sample_bulk();
    let my_service = DynamicVectorManageService::<SampleData>::new(
        "sampleData.bin".to_string(),
        "StringDynamicsampleData.bin".to_string(),
        1024,
    )
    .unwrap();
    let start = Instant::now(); // 记录开始时间
    let objs = my_service.load_bulk(0, COUNT as u64);
    println!(
        "obj at index:{}:\n {:?}",
        objs.len() - 1,
        objs[objs.len() - 1]
    );
    let duration = start.elapsed(); // 计算时间差
    println!("load {} items   took: {:?}", COUNT, duration);
}

#[test]
fn test_io_sample_bulk() {
    let mut objs = Vec::new();
    let mut my_service = DynamicVectorManageService::<SampleData>::new(
        "sampleDataIo.bin".to_string(),
        "StringDynamicsampleDataIo.bin".to_string(),
        1024,
    )
    .unwrap();
    for i in 0..COUNT {
        let my_obj = SampleData {
            my_number1: i as i32,
            my_string1: format!("Hello, World! 你好世界 {}", i).to_string(),
            my_number2: i as i32 * 10,
            my_boolean1: i % 2 == 0,
            my_string2: Some(format!("This is another longer string. {}", i).to_string()),
        };

        objs.push(my_obj.clone());
    }
    let start = Instant::now(); // 记录开始时间
    my_service.save_bulk(objs);
    let duration = start.elapsed(); // 计算时间差
    println!("save  {} items   took: {:?}", COUNT, duration);
    my_service.load_bulk(0, COUNT as u64);
    let load_duration = start.elapsed(); // 计算时间差
    println!(
        "load  {} items   took: {:?}",
        COUNT,
        load_duration - duration
    );
}
