use serde::{
    Deserialize,
    Serialize,
};
use std::time::Instant;
use vector_db_core::*;

const COUNT: usize = 10000;

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct DynamicStruct {
    my_usize: usize,
    my_u64: u64,
    my_u32: u32,
    my_u16: u16,
    my_u8: u8,
    my_boolean: bool,
    my_usize_vec: Vec<usize>,
    my_64_vec: Vec<u64>,
    my_32_vec: Vec<u32>,
    my_string: String,
}

fn remove_file(path: &str) {
    // let path = path.to_string();
    if std::path::Path::new(&path).exists() {
        std::fs::remove_file(&path).expect("Unable to remove file");
    }
}

#[test]
fn test_dynamic_vector_one() {
    remove_file("vector.bin");
    remove_file("StringDynamicvector.bin");
    let i = COUNT;
    let my_obj: DynamicStruct = DynamicStruct {
        my_usize: 443 + i,
        my_u64: 53 + i as u64,
        my_u32: 4399,
        my_u16: 3306,
        my_u8: 22,
        my_boolean: true,
        my_usize_vec: vec![i],
        my_32_vec: vec![i as u32],
        my_64_vec: vec![i as u64],
        my_string: format!("hello, {} world", i),
    };
    let my_service = DynamicVectorManageService::<DynamicStruct>::new(
        "vector.bin".to_string(),
        "StringDynamicvector.bin".to_string(),
        1024,
    )
    .unwrap();
    my_service.save(my_obj);
    my_service.load(0);
}

#[test]
fn test_dynamic_vector_bulk() {
    remove_file("vector11.bin");
    remove_file("StringDynamicvector11.bin");
    let mut my_vec = Vec::new();
    let mut objs = Vec::new();
    let my_service = DynamicVectorManageService::<DynamicStruct>::new(
        "vector11.bin".to_string(),
        "StringDynamicvector11.bin".to_string(),
        1024,
    )
    .unwrap();
    for i in 0..COUNT {
        let my_obj: DynamicStruct = DynamicStruct {
            my_usize: 443,
            my_u64: 53,
            my_u32: 4399,
            my_u16: 3306,
            my_u8: 22,
            my_boolean: true,
            my_usize_vec: my_vec.clone(),
            my_32_vec: vec![i as u32],
            my_64_vec: vec![i as u64],
            my_string: format!("hello, {} world", i),
        };

        objs.push(my_obj.clone());
        my_vec.push(i + 1);
    }
    let start = Instant::now(); // 记录开始时间
    my_service.save_bulk(objs);
    let duration = start.elapsed(); // 计算时间差
    println!("add  {} items   took: {:?}", COUNT, duration);
    let start = Instant::now(); // 记录开始时间
    my_service.load_bulk(0, COUNT as u64);
    let duration = start.elapsed(); // 计算时间差
    println!("read {} items   took: {:?}", COUNT, duration);
}
