//  Deprecated DatabaseWithCache, multiservice   is on going 
use rand::Rng;
use std::{
    sync::{
        Arc,
        Mutex,
    },
    time::Instant,
};
use serde::{
    Deserialize,
    Serialize,
};
use vector_db_core::*;

const COUNT: usize = 1000000;
const TURNS : usize =50;  

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct SampleData {
    pub my_number1: i32,            // 整数类型
    pub my_string1: String,         // 字符串类型，默认为空字符串
    pub my_number2: i32,            // 整数类型
    pub my_boolean1: bool,          // 布尔类型
    pub my_string2: Option<String>, // 可选字符串，可以为空
}

impl SampleData {
    pub fn new(&self, i: usize) -> Self {
        Self {
            my_number1: i as i32,
            my_string1: format!("Hello, World! 你好世界 {}", i).to_string(),
            my_number2: i as i32 * 10,
            my_boolean1: i % 2 == 0,
            my_string2: Some(format!("This is another longer string. {}", i).to_string()),
        } 
    }
}

fn get_sample_objs() -> Vec<SampleData> {
    let mut objs = Vec::new();
for i in 0..COUNT {
        let my_obj = SampleData {
            my_number1: i as i32,
            my_string1: format!("Hello, World! 你好世界 {}", i).to_string(),
            my_number2: i as i32 * 10,
            my_boolean1: i % 2 == 0,
            my_string2: Some(format!("This is another longer string. {}", i).to_string()),
        };

        objs.push(my_obj);
    }
objs
}

fn remove_file(path: &str) {
// let path = path.to_string();
        if std::path::Path::new(&path).exists() {
            std::fs::remove_file(&path).expect("Unable to remove file");
        }
}
#[test]
fn test_engine_sample_one() {
    remove_file("cacheS4.bin");
    let i = 0;

    let my_obj = SampleData {
        my_number1: i as i32,
        my_string1: format!("Hello, World! 你好世界 {}", i).to_string(),
        my_number2: i as i32 * 10,
        my_boolean1: i % 2 == 0,
        my_string2: Some(format!("This is another longer string. {}", i).to_string()),
    };
    
    let my_service: DatabaseWithCache<DynamicVectorManageService<SampleData>, SampleData> =
                VectorEngine::<SampleData>::new(
                    "cacheS4.bin".to_string(),
                    "cacheSD4.bin".to_string(),
                    1024,
                );
                
    my_service.push(my_obj);
    let obj = my_service.pull(i as u64);
    println!("read one obj at {}: {:?}", i, obj);
    assert_eq!(i, obj.my_number1);
}


#[test]
fn test_engine_sample_one_by_one() {
    remove_file("cacheSO.bin");
    remove_file("cacheSDO.bin");

    let my_service: DatabaseWithCache<DynamicVectorManageService<SampleData>, SampleData> =
                VectorEngine::<SampleData>::new(
                    "cacheSO.bin".to_string(),
                    "cacheSDO.bin".to_string(),
                    1024,
                );
                    let start = Instant::now();
    for i in 0..COUNT/100 {
        let my_obj = SampleData {
            my_number1: i as i32,
            my_string1: format!("Hello, World! 你好世界 {}", i).to_string(),
            my_number2: i as i32 * 10,
            my_boolean1: i % 2 == 0,
            my_string2: Some(format!("This is another longer string. {}", i).to_string()),
        };

        my_service.push(my_obj);
    }
    let duration = start.elapsed(); 
    println!("one by one save  {} items   took: {:?}", COUNT, duration);
    assert_eq!(COUNT, my_service.len());
    let objs = my_service.pullx(0, COUNT as u64 /100);
}

#[test]
fn test_engine_sample_bulk() {
    remove_file("cacheS4.bin");
    remove_file("cacheSD4.bin");
    let mut objs = Vec::new();
    let my_service: DatabaseWithCache<DynamicVectorManageService<SampleData>, SampleData> =
                VectorEngine::<SampleData>::new(
                    "cacheS4.bin".to_string(),
                    "cacheSD4.bin".to_string(),
                    1024,
                );
    for i in 0..COUNT {
        let my_obj = SampleData {
            my_number1: i as i32,
            my_string1: format!("Hello, World! 你好世界 {}", i).to_string(),
            my_number2: i as i32 * 10,
            my_boolean1: i % 2 == 0,
            my_string2: Some(format!("This is another longer string. {}", i).to_string()),
        };

        objs.push(my_obj);
    }
    let start = Instant::now(); // 记录开始时间
    my_service.extend(objs);
    let duration = start.elapsed(); 
    println!("save  {} items   took: {:?}", COUNT, duration);
    assert_eq!(COUNT, my_service.len());
}

#[test]
fn test_engine_read_sample_bulk() {
    remove_file("SampleData.bin");
    remove_file("dynamicSampleData.bin");
    let objs = get_sample_objs();
    let my_service: DatabaseWithCache<DynamicVectorManageService<SampleData>, SampleData> =
                VectorEngine::<SampleData>::new(
                    "SampleData.bin".to_string(),
                    "dynamicSampleData.bin".to_string(),
                    1024,
                );
    let start = Instant::now();
    my_service.extend(objs);
    let extend_duration = start.elapsed();
    println!("extend {} items  took: {:?}", COUNT, extend_duration);
    let objs = my_service.pullx(0, COUNT as u64);
    let read_bulk_duration = start.elapsed(); // 计算时间差
    assert_eq!(objs.len(), COUNT);
    assert_eq!(my_service.len(), COUNT);
    println!("load {} items   took: {:?}", COUNT, read_bulk_duration - extend_duration);
}


#[test]
fn test_engine_getting_sample_multi_thread() {
    const COUNT:usize = 1000; 
remove_file("SampleDataM.bin");
    remove_file("dynamicSampleDataM.bin");
    let objs = get_sample_objs();
    let read_cache_service_origin: Arc<DatabaseWithCache<DynamicVectorManageService<SampleData>, SampleData>> =
                Arc::new(VectorEngine::<SampleData>::new(
                    "SampleDataM.bin".to_string(),
                    "dynamicSampleDataM.bin".to_string(),
                    1024,
                ));

    let start = Instant::now();
    read_cache_service_origin.extend(objs);
    let extend_duration = start.elapsed();
    println!("extend {} items  took: {:?}", COUNT, extend_duration);


    let read_cache_service = Arc::clone(&read_cache_service_origin);

    // let read_cache_service = Arc::clone(&read_cache_service);
    let objs = read_cache_service.getting_lot(0, COUNT as u64);
    let pull_lot_cache_duration = start.elapsed();
    println!(
        "pull lot duration: {:?}",
        pull_lot_cache_duration - extend_duration
    );
    let handles = (0..TURNS)
        .map(|i| {
            let read_cache_service = Arc::clone(&read_cache_service);
            std::thread::spawn(move || {
                let mut rng = rand::thread_rng();
                for i in 0..(COUNT / TURNS) {
                    // let obj = read_cache_service.getting(i as u64);
                    let random_int: u64 = rng.gen_range(0..(COUNT / TURNS) as u64);
                    let obj1 = read_cache_service.getting(random_int);
                }
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        // handle.join().expect("Thread panicked");
        handle.join().unwrap();
    }

    let get_from_cache_duration = start.elapsed();
    println!(
        "get from  cache duration: {:?}",
        get_from_cache_duration - pull_lot_cache_duration
    );
}


#[test]
fn test_engine_compare_getting_sample_multi_thread() {
    const COUNT:usize = 1000; 
remove_file("SampleDataM1.bin");
    remove_file("dynamicSampleDataM1.bin");
    let objs = get_sample_objs();
    let read_cache_service_origin: Arc<DynamicVectorManageService<SampleData>> =
                Arc::new(DynamicVectorManageService::<SampleData>::new(
                    "SampleDataM1.bin".to_string(),
                    "dynamicSampleDataM1.bin".to_string(),
                    1024,
                ).unwrap());

    let start = Instant::now();
    read_cache_service_origin.extend(objs);
    let extend_duration = start.elapsed();
    println!("extend {} items  took: {:?}", COUNT, extend_duration);


    let read_cache_service = Arc::clone(&read_cache_service_origin);

    // let read_cache_service = Arc::clone(&read_cache_service);
    let objs = read_cache_service.pullx(0, COUNT as u64);
    let pull_lot_cache_duration = start.elapsed();
    println!(
        "pull lot duration: {:?}",
        pull_lot_cache_duration - extend_duration
    );
    let handles = (0..TURNS)
        .map(|i| {
            let read_cache_service = Arc::clone(&read_cache_service);
            std::thread::spawn(move || {
                let mut rng = rand::thread_rng();
                for i in 0..(COUNT / TURNS) {
                    // let obj = read_cache_service.getting(i as u64);
                    let random_int: u64 = rng.gen_range(0..(COUNT / TURNS) as u64);
                    let obj1 = read_cache_service.pull(random_int);
                }
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        // handle.join().expect("Thread panicked");
        handle.join().unwrap();
    }

    let get_from_cache_duration = start.elapsed();
    println!(
        "get from  cache duration: {:?}",
        get_from_cache_duration - pull_lot_cache_duration
    );
}
