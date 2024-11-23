use rand::Rng;
use std::sync::{Arc, Mutex};
use serde::{
    Deserialize,
    Serialize,
};
use std::time::Instant;
use vector_db_core::{
    DynamicVectorManageService,
    ReadableCache,
    StaticVectorManageService,
    WritableCache,
};

const COUNT: usize = 37400;
const TURNS: usize = 10;

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct StaticStruct {
    my_usize: usize,
    my_u64: u64,
    my_u32: u32,
    my_u16: u16,
    my_u8: u8,
    my_boolean: bool,
}

#[test]
fn test_push_static_one() {
    let my_obj: StaticStruct = StaticStruct {
        my_usize: 443,
        my_u64: 53,
        my_u32: 4399,
        my_u16: 3306,
        my_u8: 22,
        my_boolean: true,
        // my_string: "good luck!".to_string(),
        // my_vec: vec!["hello".to_string(), "world".to_string()],
        // my_vec: vec![1,2,3,4,5],
        // my_array: [1,2,3,4,5],
    };
    let my_service =
        WritableCache::<StaticVectorManageService<StaticStruct>, StaticStruct>::new(
            "cacheS.bin".to_string(),
            "cacheSD.bin".to_string(),
            1024,
        );

    my_service.push(my_obj);
}

#[test]
fn test_one_by_one_push_static() {
    // let mut objs = Vec::new();
    let my_service =
        WritableCache::<StaticVectorManageService<StaticStruct>, StaticStruct>::new(
            "cacheS.bin".to_string(),
            "cacheSD.bin".to_string(),
            1024,
        );
    for i in 0..COUNT {
        let my_obj: StaticStruct = StaticStruct {
            my_usize: 443 + i,
            my_u64: 53,
            my_u32: 4399,
            my_u16: 3306,
            my_u8: 22,
            my_boolean: true,
        };
        // my_vec.push(i as u64 *1000);

        my_service.push(my_obj);
    }
    // my_service.add_bulk(objs);
}

#[test]
fn test_one_by_one_getting_static() {
    let my_service =
        ReadableCache::<StaticVectorManageService<StaticStruct>, StaticStruct>::new(
            "cacheS.bin".to_string(),
            "cacheSD.bin".to_string(),
            1024,
        );
    for i in 0..COUNT {
        let my_obj: StaticStruct = StaticStruct {
            my_usize: 443 + i,
            my_u64: 53,
            my_u32: 4399,
            my_u16: 3306,
            my_u8: 22,
            my_boolean: true,
        };
        // my_vec.push(i as u64 *1000);

        // my_service.push(my_obj);
        my_service.getting(i as u64);
    }
}

#[test]
fn test_one_by_one_push_dynamic() {
    // let mut objs = Vec::new();
    let my_service = WritableCache::<
        DynamicVectorManageService<StaticStruct>,
        StaticStruct,
    >::new("cacheD.bin".to_string(), "cacheDD.bin".to_string(), 1024);
    for i in 0..COUNT {
        let my_obj: StaticStruct = StaticStruct {
            my_usize: 443 + i,
            my_u64: 53,
            my_u32: 4399,
            my_u16: 3306,
            my_u8: 22,
            my_boolean: true,
        };
        // my_vec.push(i as u64 *1000);

        my_service.push(my_obj);
    }
    // my_service.add_bulk(objs);
}

#[test]
fn test_one_by_one_getting_dynamic() {
    let my_service = ReadableCache::<
        DynamicVectorManageService<StaticStruct>,
        StaticStruct,
    >::new("cacheD.bin".to_string(), "cacheDD.bin".to_string(), 1024);
    for i in 0..COUNT {
        let my_obj: StaticStruct = StaticStruct {
            my_usize: 443 + i,
            my_u64: 53,
            my_u32: 4399,
            my_u16: 3306,
            my_u8: 22,
            my_boolean: true,
        };
        // my_vec.push(i as u64 *1000);

        my_service.getting(i as u64);
    }
}

#[test]
fn test_extend_static() {
    let mut objs = Vec::new();
    let my_service =
        WritableCache::<StaticVectorManageService<StaticStruct>, StaticStruct>::new(
            "cacheS.bin".to_string(),
            "cacheSD.bin".to_string(),
            1024,
        );
    for i in 0..COUNT {
        let my_obj: StaticStruct = StaticStruct {
            my_usize: 443 + i,
            my_u64: 53,
            my_u32: 4399,
            my_u16: 3306,
            my_u8: 22,
            my_boolean: true,
        };

        objs.push(my_obj);
    }
    my_service.extend(objs);
}

#[test]
fn test_pull_lot_static_from_cache() {
    let mut objs = Vec::new();
    let my_service =
        WritableCache::<StaticVectorManageService<StaticStruct>, StaticStruct>::new(
            "cacheS.bin".to_string(),
            "cacheSD.bin".to_string(),
            1024,
        );
    let read_cache_service = ReadableCache::<
        StaticVectorManageService<StaticStruct>,
        StaticStruct,
    >::new(
        "cacheS.bin".to_string(), "cacheSD.bin".to_string(), 1024
    );
    for i in 0..COUNT {
        let my_obj: StaticStruct = StaticStruct {
            my_usize: 443 + i,
            my_u64: 53,
            my_u32: 4399,
            my_u16: 3306,
            my_u8: 22,
            my_boolean: true,
        };

        objs.push(my_obj);
    }
    let start = Instant::now();
    my_service.extend(objs);
    let extend_cache_duration = start.elapsed();
    println!("extend cache duration: {:?}", extend_cache_duration);
    read_cache_service.getting_lot(0, COUNT as u64);
    let pull_lot_cache_duration = start.elapsed();
    println!(
        "pull lot cache duration: {:?}",
        pull_lot_cache_duration - extend_cache_duration
    );
}

#[test]
fn test_getting_lot_static_from_cache() {
    let mut objs = Vec::new();
    let my_service =
        WritableCache::<StaticVectorManageService<StaticStruct>, StaticStruct>::new(
            "cacheS.bin".to_string(),
            "cacheSD.bin".to_string(),
            1024,
        );
    let read_cache_service = ReadableCache::<
        StaticVectorManageService<StaticStruct>,
        StaticStruct,
    >::new(
        "cacheS.bin".to_string(), "cacheSD.bin".to_string(), 1024
    );
    for i in 0..COUNT {
        let my_obj: StaticStruct = StaticStruct {
            my_usize: 443 + i,
            my_u64: 53,
            my_u32: 4399,
            my_u16: 3306,
            my_u8: 22,
            my_boolean: true,
        };

        objs.push(my_obj);
    }
    let start = Instant::now();
    my_service.extend(objs);
    let extend_cache_duration = start.elapsed();
    println!("extend cache duration: {:?}", extend_cache_duration);
    read_cache_service.getting_lot(0, COUNT as u64);
    let getting_lot_cache_duration = start.elapsed();
    println!(
        "get lot cache duration: {:?}",
        getting_lot_cache_duration - extend_cache_duration
    );
}

#[test]
fn test_getting_static_multi_turns() {
    let mut objs = Vec::new();
    let my_service =
        WritableCache::<StaticVectorManageService<StaticStruct>, StaticStruct>::new(
            "cacheS.bin".to_string(),
            "cacheSD.bin".to_string(),
            1024,
        );
    let read_cache_service = ReadableCache::<
        StaticVectorManageService<StaticStruct>,
        StaticStruct,
    >::new(
        "cacheS.bin".to_string(), "cacheSD.bin".to_string(), 1024
    );
    for i in 0..COUNT {
        let my_obj: StaticStruct = StaticStruct {
            my_usize: 443 + i,
            my_u64: 53,
            my_u32: 4399,
            my_u16: 3306,
            my_u8: 22,
            my_boolean: true,
        };

        objs.push(my_obj);
    }
    let mut rng = rand::thread_rng();
    let start = Instant::now();
    my_service.extend(objs);
    let extend_cache_duration = start.elapsed();
    println!("extend cache duration: {:?}", extend_cache_duration);
    let objs = read_cache_service.getting_lot(0, COUNT as u64);
    let pull_lot_cache_duration = start.elapsed();
    println!(
        "pull lot duration: {:?}",
        pull_lot_cache_duration - extend_cache_duration
    );
    for turn in 0..TURNS {
        for i in 0..COUNT {
            // let obj = read_cache_service.getting(i as u64);
            let random_int: u64 = rng.gen_range(0..COUNT as u64 / 10);
            let obj1 = read_cache_service.getting(random_int);
        }
    }
    let get_from_cache_duration = start.elapsed();
    println!(
        "get from  cache duration: {:?}",
        get_from_cache_duration - pull_lot_cache_duration
    );
}
/*
// #[test]
// fn test_read_static_one() {
// let my_service = StaticVectorManageService::<StaticStruct>::new(
// "TestDynamicData.bin".to_string(),
// "TestDynamicDataDynamic.bin".to_string(),
// 1024,
// )
// .unwrap();
// my_service.read(COUNT);
// }
*/
#[test]
fn test_read_static_bulk_in_pushed() {
    let my_service = StaticVectorManageService::<StaticStruct>::new(
        "cacheS.bin".to_string(),
        "cacheSD.bin".to_string(),
        1024,
    )
    .unwrap();
    my_service.read_bulk(0, COUNT as u64);
    dbg!(my_service.get_length());
}

#[test]
fn test_add_bulk_compare() {
    let mut objs = Vec::new();
    let my_service = StaticVectorManageService::<StaticStruct>::new(
        "cacheS.bin".to_string(),
        "cacheSD.bin".to_string(),
        1024,
    )
    .unwrap();
    for i in 0..COUNT {
        let my_obj: StaticStruct = StaticStruct {
            my_usize: 443 + i,
            my_u64: 53,
            my_u32: 4399,
            my_u16: 3306,
            my_u8: 22,
            my_boolean: true,
        };
        // my_vec.push(i as u64 *1000);
        objs.push(my_obj);

        // my_service.push(&my_obj);
        // my_service.add(my_obj);
    }
    my_service.add_bulk(objs);
}

#[test]
fn test_compare_static_multi_turns() {
    let mut objs = Vec::new();
    let my_service =
        WritableCache::<StaticVectorManageService<StaticStruct>, StaticStruct>::new(
            "cacheS.bin".to_string(),
            "cacheSD.bin".to_string(),
            1024,
        );
    let read_service = StaticVectorManageService::<StaticStruct>::new(
        "cacheS.bin".to_string(),
        "cacheSD.bin".to_string(),
        1024,
    )
    .unwrap();
    for i in 0..COUNT {
        let my_obj: StaticStruct = StaticStruct {
            my_usize: 443 + i,
            my_u64: 53,
            my_u32: 4399,
            my_u16: 3306,
            my_u8: 22,
            my_boolean: true,
        };

        objs.push(my_obj);
    }
    let mut rng = rand::thread_rng();
    let start = Instant::now();
    my_service.extend(objs);
    let extend_cache_duration = start.elapsed();
    println!("extend cache duration: {:?}", extend_cache_duration);
    let objs = read_service.read_bulk(0, COUNT as u64);
    let pull_lot_cache_duration = start.elapsed();
    println!(
        "pull lot duration: {:?}",
        pull_lot_cache_duration - extend_cache_duration
    );
    for turn in 0..TURNS {
        for i in 0..COUNT {
            // let obj = read_cache_service.getting(i as u64);
            let random_int: u64 = rng.gen_range(0..COUNT as u64 / 10);
            let obj1 = read_service.read(random_int);
        }
    }
    let get_from_cache_duration = start.elapsed();
    println!(
        "get from  cache duration: {:?}",
        get_from_cache_duration - pull_lot_cache_duration
    );
}

#[test]
fn test_getting_static_multi_thread() {
    let mut objs = Vec::new();
    let my_service =
        WritableCache::<StaticVectorManageService<StaticStruct>, StaticStruct>::new(
            "cacheS.bin".to_string(),
            "cacheSD.bin".to_string(),
            1024,
        );
    let read_cache_service_origin = Arc::new(ReadableCache::<
        StaticVectorManageService<StaticStruct>,
        StaticStruct,
    >::new(
        "cacheS.bin".to_string(), "cacheSD.bin".to_string(), 1024
    ));
    let read_cache_service = Arc::clone(&read_cache_service_origin);
    for i in 0..COUNT {
        let my_obj: StaticStruct = StaticStruct {
            my_usize: 443 + i,
            my_u64: 53,
            my_u32: 4399,
            my_u16: 3306,
            my_u8: 22,
            my_boolean: true,
        };

        objs.push(my_obj);
    }
    // let mut rng = rand::thread_rng();
    let start = Instant::now();
    my_service.extend(objs);
    let extend_cache_duration = start.elapsed();
    println!("extend cache duration: {:?}", extend_cache_duration);
    let read_cache_service = Arc::clone(&read_cache_service);
    let objs = read_cache_service.getting_lot(0, COUNT as u64);
    let pull_lot_cache_duration = start.elapsed();
    println!(
        "pull lot duration: {:?}",
        pull_lot_cache_duration - extend_cache_duration
    );
    let handles =  (0..TURNS)
.map(|i|    {
        let read_cache_service = Arc::clone(&read_cache_service);
        std::thread::spawn(
        move  || {
            let mut rng = rand::thread_rng(); 
        for i in 0..(COUNT/TURNS) {
            // let obj = read_cache_service.getting(i as u64);
            let random_int: u64 = rng.gen_range(0..(COUNT/ TURNS) as u64 / 10);
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
fn test_compare_static_multi_thread() {
    let mut objs = Vec::new();
    let my_service =
        WritableCache::<StaticVectorManageService<StaticStruct>, StaticStruct>::new(
            "cacheS.bin".to_string(),
            "cacheSD.bin".to_string(),
            1024,
        );
    let read_cache_service_origin = Arc::new(StaticVectorManageService::<StaticStruct>::new(
        "cacheS.bin".to_string(), "cacheSD.bin".to_string(), 1024
    ).unwrap());
    let read_cache_service = Arc::clone(&read_cache_service_origin);
    for i in 0..COUNT {
        let my_obj: StaticStruct = StaticStruct {
            my_usize: 443 + i,
            my_u64: 53,
            my_u32: 4399,
            my_u16: 3306,
            my_u8: 22,
            my_boolean: true,
        };

        objs.push(my_obj);
    }
    // let mut rng = rand::thread_rng();
    let start = Instant::now();
    my_service.extend(objs);
    let extend_cache_duration = start.elapsed();
    println!("extend cache duration: {:?}", extend_cache_duration);
    let read_cache_service = Arc::clone(&read_cache_service);
    let objs = read_cache_service.read_bulk(0, COUNT as u64);
    let pull_lot_cache_duration = start.elapsed();
    println!(
        "pull lot duration: {:?}",
        pull_lot_cache_duration - extend_cache_duration
    );
    let handles =  (0..TURNS)
.map(|i|    {
        let read_cache_service = Arc::clone(&read_cache_service);
        std::thread::spawn(
        move  || {
            let mut rng = rand::thread_rng(); 
        for i in 0..COUNT {
            // let obj = read_cache_service.getting(i as u64);
            let random_int: u64 = rng.gen_range(0..COUNT as u64 / 10);
            let obj1 = &read_cache_service.read(random_int);
        }
        })
    })
    .collect::<Vec<_>>();
    for handle in handles {
        handle.join().expect("Thread panicked");
    }
    
    let get_from_cache_duration = start.elapsed();
    println!(
        "get from  cache duration: {:?}",
        get_from_cache_duration - pull_lot_cache_duration
    );
}

#[test]
fn test_safe_getting_static_multi_thread() {
    let mut objs = Vec::new();
    let my_service =
        WritableCache::<StaticVectorManageService<StaticStruct>, StaticStruct>::new(
            "cacheS.bin".to_string(),
            "cacheSD.bin".to_string(),
            1024,
        );
    let read_cache_service_origin = Arc::new(Mutex::new(ReadableCache::<
        StaticVectorManageService<StaticStruct>,
        StaticStruct,
    >::new(
        "cacheS.bin".to_string(), "cacheSD.bin".to_string(), 1024
    )));
    let read_cache_service = Arc::clone(&read_cache_service_origin);
    for i in 0..COUNT {
        let my_obj: StaticStruct = StaticStruct {
            my_usize: 443 + i,
            my_u64: 53,
            my_u32: 4399,
            my_u16: 3306,
            my_u8: 22,
            my_boolean: true,
        };

        objs.push(my_obj);
    }
    // let mut rng = rand::thread_rng();
    let start = Instant::now();
    my_service.extend(objs);
    let extend_cache_duration = start.elapsed();
    println!("extend cache duration: {:?}", extend_cache_duration);
    let read_cache_service = Arc::clone(&read_cache_service);
    let objs = read_cache_service.lock().unwrap().getting_lot(0, COUNT as u64);
    let pull_lot_cache_duration = start.elapsed();
    println!(
        "pull lot duration: {:?}",
        pull_lot_cache_duration - extend_cache_duration
    );
    let handles =  (0..TURNS)
.map(|i|    {
        let read_cache_service = Arc::clone(&read_cache_service);
        std::thread::spawn(
        move  || {
            let mut rng = rand::thread_rng(); 
        for i in 0..(COUNT/TURNS) {
            // let obj = read_cache_service.getting(i as u64);
            let random_int: u64 = rng.gen_range(0..(COUNT/ TURNS) as u64 / 10);
            let obj1 = read_cache_service.lock().unwrap().getting(random_int);
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