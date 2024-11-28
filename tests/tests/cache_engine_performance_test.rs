use rayon::prelude::*; 
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
const TURNS : usize =3;  

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

fn get_specific_sample_objs(count: usize) -> Vec<SampleData> {
    let mut objs = Vec::new();
for i in 0..count {
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
fn test_cache_engine_sample_one() {
    remove_file("cacheS4.bin");
    let i = 0;

    let my_obj = SampleData {
        my_number1: i as i32,
        my_string1: format!("Hello, World! 你好世界 {}", i).to_string(),
        my_number2: i as i32 * 10,
        my_boolean1: i % 2 == 0,
        my_string2: Some(format!("This is another longer string. {}", i).to_string()),
    };
    
    let my_service: ReadableCache<WritableCache<DynamicVectorManageService<SampleData>, SampleData>, SampleData> =
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
fn test_one_by_one_cache_engine_sample() {
    const COUNT: usize = 1000; 
    remove_file("cacheSO.bin");
    remove_file("cacheSDO.bin");

    let my_service: ReadableCache<WritableCache<DynamicVectorManageService<SampleData>, SampleData>, SampleData> =
                VectorEngine::<SampleData>::new(
                    "cacheSO.bin".to_string(),
                    "cacheSDO.bin".to_string(),
                    1024,
                );
                    let start = Instant::now();
    for i in 0..COUNT {
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
    for i in 0..COUNT {
    let obj = my_service.pull( i as u64);
    }
    let one_by_one_pull_duration = start.elapsed(); 
    println!("one by one pull  {} items   took: {:?}", COUNT,  one_by_one_pull_duration -duration);
    assert_eq!(my_service.len(), COUNT as usize);
}

#[test]
fn test_cache_engine_sample_bulk() {
    remove_file("cacheS4.bin");
    remove_file("cacheSD4.bin");
    let mut objs = Vec::new();
    let my_service: ReadableCache<WritableCache<DynamicVectorManageService<SampleData>, SampleData>, SampleData> =
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
    println!("extend   {} items   took: {:?}", COUNT, duration);
    assert_eq!(COUNT, my_service.len());
}

#[test]
fn test_cache_engine_read_sample_bulk() {
    remove_file("SampleData.bin");
    remove_file("dynamicSampleData.bin");
    let objs = get_sample_objs();
    let my_service: ReadableCache<WritableCache<DynamicVectorManageService<SampleData>, SampleData>, SampleData> =
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
    println!("load {} items   took: {:?}", COUNT, read_bulk_duration - extend_duration);
    assert_eq!(objs.len(), COUNT);
    assert_eq!(my_service.len(), COUNT);
}

#[test]
fn test_cache_engine_mix_sample_bulk() {
    remove_file("SampleDataMix.bin");
    remove_file("dynamicSampleDataMix.bin");
    let objs = get_specific_sample_objs(COUNT /2 +1);
    let objs1 = get_specific_sample_objs(COUNT /2 -1);
    let my_service: ReadableCache<WritableCache<DynamicVectorManageService<SampleData>, SampleData>, SampleData> =
                VectorEngine::<SampleData>::new(
                    "SampleDataMix.bin".to_string(),
                    "dynamicSampleDataMix.bin".to_string(),
                    1024,
                );
    let start = Instant::now();
    let objs_len = objs.len();
    let objs1_len = objs1.len();
    my_service.extend(objs);
    let extend_duration = start.elapsed();
    println!("extend {} items  took: {:?}", objs_len, extend_duration);
    my_service.pullx(0, objs_len as u64);
    let pull_duration = start.elapsed();
    println!("pull {} items  took: {:?}", objs_len, pull_duration - extend_duration);
    my_service.extend(objs1);
    let extend_duration1 = start.elapsed();
    println!("second  extend {} items  took: {:?}", objs1_len, extend_duration1 - pull_duration);
    let objs = my_service.pullx(0, COUNT as u64);
    let read_bulk_duration = start.elapsed(); // 计算时间差
    println!("load {} items   took: {:?}", COUNT, read_bulk_duration - extend_duration1);
    assert_eq!(objs.len(), COUNT);
    assert_eq!(my_service.len(), COUNT);
}


#[test]
fn test_cache_engine_getting_sample_multi_thread() {
    const COUNT: usize = 1000; 
remove_file("SampleDataM.bin");
    remove_file("dynamicSampleDataM.bin");
    let objs = get_sample_objs();
    let read_cache_service_origin: Arc<ReadableCache<WritableCache<DynamicVectorManageService<SampleData>, SampleData>, SampleData>> =
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
fn test_cache_engine_compare_getting_sample_multi_thread() {
    const COUNT: usize = 1000; 
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


fn test_cache_engine_getting_sample_parallel() {
    const COUNT: usize = 1000; 
remove_file("SampleDataMP.bin");
    remove_file("dynamicSampleDataMP.bin");
    let objs = get_sample_objs();
    let read_cache_service_origin: Arc<ReadableCache<WritableCache<DynamicVectorManageService<SampleData>, SampleData>, SampleData>> =
                Arc::new(VectorEngine::<SampleData>::new(
                    "SampleDataMP.bin".to_string(),
                    "dynamicSampleDataMP.bin".to_string(),
                    1024,
                ));

    let start = Instant::now();
    read_cache_service_origin.extend(objs);
    let extend_duration = start.elapsed();
    println!("extend {} items  took: {:?}", COUNT, extend_duration);


    let read_cache_service = Arc::clone(&read_cache_service_origin);


    let objs = read_cache_service.getting_lot(0, COUNT as u64);
    dbg!(&objs.len());
    let pull_lot_cache_duration = start.elapsed();
    println!(
        "pull lot duration: {:?}",
        pull_lot_cache_duration - extend_duration
    );
    let handles = (0..TURNS)
    .collect::<Vec<usize>>()
    .par_iter()
        .map(|i| {
            let read_cache_service = Arc::clone(&read_cache_service);

                let mut rng = rand::thread_rng();
                for i in 0..(COUNT / TURNS) {

                    let random_int: u64 = rng.gen_range(0..(COUNT / TURNS) as u64);
                    let obj1 = read_cache_service.getting(random_int);
                    dbg!(obj1.my_number1);
                }

        })
        .collect::<Vec<_>>();





    let get_from_cache_duration = start.elapsed();
    println!(
        "get from  cache duration: {:?}",
        get_from_cache_duration - pull_lot_cache_duration
    );
}

#[test]
fn test_parallel_cache_engine_sample() {
    const COUNT: usize = 1000; 
    remove_file("cacheSP.bin");
    remove_file("cacheSDP.bin");

    let my_service: Arc<ReadableCache<WritableCache<DynamicVectorManageService<SampleData>, SampleData>, SampleData>> =
                Arc::new(VectorEngine::<SampleData>::new(
                    "cacheSP.bin".to_string(),
                    "cacheSDP.bin".to_string(),
                    1024,
                ));
                    let start = Instant::now();

    (0..COUNT).collect::<Vec<usize>>()
.par_iter()
.enumerate()
.for_each(|(i, o)| {
    let my_service= Arc::clone(&my_service);
        let my_obj = SampleData {
            my_number1: i as i32,
            my_string1: format!("Hello, World! 你好世界 {}", i).to_string(),
            my_number2: i as i32 * 10,
            my_boolean1: i % 2 == 0,
            my_string2: Some(format!("This is another longer string. {}", i).to_string()),
        };

        my_service.push(my_obj);
    });
    let duration = start.elapsed(); 
    println!("one by one save  {} items   took: {:?}", COUNT, duration);
    assert_eq!(COUNT, my_service.len());
    (0..COUNT)
    .collect::<Vec<usize>>()
    .par_iter()
    .for_each(|i| {
        let my_service = Arc::clone(&my_service);

    let obj = my_service.pull( *i as u64);
    });
    let one_by_one_pull_duration = start.elapsed(); 
    println!("one by one pull  {} items   took: {:?}", COUNT,  one_by_one_pull_duration -duration);
    assert_eq!(my_service.len(), COUNT as usize);
}