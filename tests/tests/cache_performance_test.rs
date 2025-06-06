use rand::Rng;
use serde::{
    Deserialize,
    Serialize,
};
use std::{
    sync::{
        Arc,
        Mutex,
    },
    time::Instant,
};
use vector_db_core::{
    ReadableCache,
    StaticVectorManageService,
    VectorEngine,
    WritableCache,
};

const COUNT: usize = 1000;
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

fn remove_file(path: &str) {
    // let path = path.to_string();
    if std::path::Path::new(&path).exists() {
        std::fs::remove_file(&path).expect("Unable to remove file");
    }
}

fn clean(name: &str) -> (String, String) {
    let temp_dir: std::path::PathBuf = std::env::temp_dir();
    let temp_file_path = temp_dir.join(name);
    let storage = temp_file_path.to_string_lossy().to_string();
    let dataname = format!("data-{}", name);
    let temp_file_path1 = temp_dir.join(&dataname);
    let bin = temp_file_path1.to_string_lossy().to_string();

    remove_file(&storage);
    remove_file(&bin);
    (storage, bin)
}



#[test]
fn test_extend_static() {
    let (storage, bin) = clean("test_extend_static");
    let mut objs = Vec::new();
    let my_service =
        WritableCache::<StaticVectorManageService<StaticStruct>, StaticStruct>::new(
            storage,
            bin,
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
    my_service.pushx(objs);
}

#[test]
fn test_pull_lot_static_from_cache() {
    let (storage, bin) = clean("test_pull_lot_static_from_cache");
    let mut objs = Vec::new();
    let my_service =
        WritableCache::<StaticVectorManageService<StaticStruct>, StaticStruct>::new(
            storage,
            bin,
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
    let start = Instant::now();
    my_service.pushx(objs);
    let extend_cache_duration = start.elapsed();
    println!("extend cache duration: {:?}", extend_cache_duration);
    my_service.pullx(0, COUNT as u64);
    let pull_lot_cache_duration = start.elapsed();
    println!(
        "pull lot cache duration: {:?}",
        pull_lot_cache_duration - extend_cache_duration
    );
}

#[test]
fn test_getting_lot_static_from_cache() {
    let (storage, bin) = clean("test_getting_lot_static_from_cache");
    let mut objs = Vec::new();
    let my_service =
        WritableCache::<StaticVectorManageService<StaticStruct>, StaticStruct>::new(
            storage,
            bin,
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
    let start = Instant::now();
    my_service.pushx(objs);
    let extend_cache_duration = start.elapsed();
    println!("extend cache duration: {:?}", extend_cache_duration);
    my_service.pullx(0, COUNT as u64);
    let getting_lot_cache_duration = start.elapsed();
    println!(
        "get lot cache duration: {:?}",
        getting_lot_cache_duration - extend_cache_duration
    );
}

#[test]
fn test_getting_static_multi_turns() {
    let (storage, bin) = clean("test_getting_static_multi_turns");
    let mut objs = Vec::new();
    
    let read_cache_service = ReadableCache::<
        StaticVectorManageService<StaticStruct>,
        StaticStruct,
    >::new(
        storage, 
        bin, 
        1024
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
    read_cache_service.pushx(objs);
    let extend_cache_duration = start.elapsed();
    println!("extend cache duration: {:?}", extend_cache_duration);
    let _objs = read_cache_service.getting_lot(0, COUNT as u64);
    let pull_lot_cache_duration = start.elapsed();
    println!(
        "pull lot duration: {:?}",
        pull_lot_cache_duration - extend_cache_duration
    );
    for _turn in 0..TURNS {
        for _i in 0..COUNT {
            // let obj = read_cache_service.getting(i as u64);
            let random_int: u64 = rng.gen_range(0..COUNT as u64 / 10);
            let _obj1 = read_cache_service.getting(random_int);
        }
    }
    let get_from_cache_duration = start.elapsed();
    println!(
        "get from  cache duration: {:?}",
        get_from_cache_duration - pull_lot_cache_duration
    );
}

#[test]
fn test_read_static_bulk_in_pushed() {
    let (storage, bin) = clean("test_read_static_bulk_in_pushed");
    
    let mut objs = Vec::new();
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
    
    let my_service = StaticVectorManageService::<StaticStruct>::new(
        storage,
        bin,
        1024,
    )
    .unwrap();
    my_service.add_bulk(objs);
    my_service.read_bulk(0, COUNT as u64);
    dbg!(my_service.get_length());
}

#[test]
fn test_add_bulk_compare() {
    let (storage, bin) = clean("test_add_bulk_compare");
    let mut objs = Vec::new();
    let my_service = StaticVectorManageService::<StaticStruct>::new(
        storage,
        bin,
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
    let (storage, bin) = clean("test_compare_static_multi_turns");
    let mut objs = Vec::new();
    
    let read_service = StaticVectorManageService::<StaticStruct>::new(
        storage,
        bin,
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
    read_service.pushx(objs);
    let extend_cache_duration = start.elapsed();
    println!("extend cache duration: {:?}", extend_cache_duration);
    let _objs = read_service.read_bulk(0, COUNT as u64);
    let pull_lot_cache_duration = start.elapsed();
    println!(
        "pull lot duration: {:?}",
        pull_lot_cache_duration - extend_cache_duration
    );
    for _turn in 0..TURNS {
        for _i in 0..COUNT {
            // let obj = read_cache_service.getting(i as u64);
            let random_int: u64 = rng.gen_range(0..COUNT as u64 / 10);
            let _obj1 = read_service.read(random_int);
        }
    }
    let get_from_cache_duration = start.elapsed();
    println!(
        "get from  cache duration: {:?}",
        get_from_cache_duration - pull_lot_cache_duration
    );
}

#[test]
fn test_writable_cache_getting_static_multi_thread() {
    let (storage, bin) = clean("test_writable_cache_getting_static_multi_thread");
    
    const COUNT: usize = 1000;
    let mut objs = Vec::new();
    let write_cache_service: Arc<
        WritableCache<StaticVectorManageService<StaticStruct>, StaticStruct>,
    > = Arc::new(VectorEngine::<StaticStruct>::new(
        storage,
        bin,
        1024,
    ));

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
    write_cache_service.pushx(objs);
    let extend_cache_duration = start.elapsed();
    println!("extend cache duration: {:?}", extend_cache_duration);
    // let read_cache_service = Arc::clone(&write_cache_service);
    let _objs = write_cache_service.pullx(0, COUNT as u64);
    let pull_lot_cache_duration = start.elapsed();
    println!(
        "pull lot duration: {:?}",
        pull_lot_cache_duration - extend_cache_duration
    );
    let handles = (0..TURNS)
        .map(|_i| {
            let read_cache_service = Arc::clone(&write_cache_service);
            std::thread::spawn(move || {
                let mut rng = rand::thread_rng();
                for _i in 0..(COUNT / TURNS) {
                    // let obj = read_cache_service.getting(i as u64);
                    let random_int: u64 = rng.gen_range(0..(COUNT / TURNS) as u64 / 10);
                    let _obj1 = read_cache_service.pull(random_int);
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
fn test_readable_cache_getting_static_multi_thread() {
    let (storage, bin) = clean("test_readable_cache_getting_static_multi_thread");
    const COUNT: usize = 1000;
    let mut objs = Vec::new();
    let read_cache_service_origin: Arc<
        ReadableCache<StaticVectorManageService<StaticStruct>, StaticStruct>,
    > = Arc::new(VectorEngine::<StaticStruct>::new(
        storage,
        bin,
        1024,
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
    read_cache_service.pushx(objs);
    let extend_cache_duration = start.elapsed();
    println!("extend cache duration: {:?}", extend_cache_duration);
    let read_cache_service = Arc::clone(&read_cache_service);
    let _objs = read_cache_service.getting_lot(0, COUNT as u64);
    let pull_lot_cache_duration = start.elapsed();
    println!(
        "pull lot duration: {:?}",
        pull_lot_cache_duration - extend_cache_duration
    );
    let handles = (0..TURNS)
        .map(|_i| {
            let read_cache_service = Arc::clone(&read_cache_service);
            std::thread::spawn(move || {
                let mut rng = rand::thread_rng();
                for _i in 0..(COUNT / TURNS) {
                    // let obj = read_cache_service.getting(i as u64);
                    let random_int: u64 = rng.gen_range(0..(COUNT / TURNS) as u64 / 10);
                    let _obj1 = read_cache_service.getting(random_int);
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
    let (storage, bin) = clean("test_compare_static_multi_thread");
    const COUNT: usize = 1000;
    let mut objs = Vec::new();
        let read_cache_service_origin = Arc::new(
        StaticVectorManageService::<StaticStruct>::new(
            storage,
            bin,
            1024,
        )
        .unwrap(),
    );
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
    read_cache_service_origin.pushx(objs);
    let extend_cache_duration = start.elapsed();
    println!("extend cache duration: {:?}", extend_cache_duration);
    let read_cache_service = Arc::clone(&read_cache_service);
    let _objs = read_cache_service.read_bulk(0, COUNT as u64);
    let pull_lot_cache_duration = start.elapsed();
    println!(
        "pull lot duration: {:?}",
        pull_lot_cache_duration - extend_cache_duration
    );
    let handles = (0..TURNS)
        .map(|_i| {
            let read_cache_service = Arc::clone(&read_cache_service);
            std::thread::spawn(move || {
                let mut rng = rand::thread_rng();
                for _i in 0..COUNT {
                    // let obj = read_cache_service.getting(i as u64);
                    let random_int: u64 = rng.gen_range(0..COUNT as u64 / 10);
                    let _obj1 = &read_cache_service.read(random_int);
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
    let (storage, bin) = clean("test_safe_getting_static_multi_thread");
    const COUNT: usize = 1000;
    let mut objs = Vec::new();
    let read_cache_service_origin = Arc::new(Mutex::new(ReadableCache::<
        WritableCache<StaticVectorManageService<StaticStruct>, StaticStruct>,
        StaticStruct,
    >::new(
        storage,
        bin,
        1024,
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
    read_cache_service_origin.lock().unwrap().add_bulk(objs);
    let extend_cache_duration = start.elapsed();
    println!("extend cache duration: {:?}", extend_cache_duration);
    let read_cache_service = Arc::clone(&read_cache_service);
    let _objs = read_cache_service
        .lock()
        .unwrap()
        .getting_lot(0, COUNT as u64);
    let pull_lot_cache_duration = start.elapsed();
    println!(
        "pull lot duration: {:?}",
        pull_lot_cache_duration - extend_cache_duration
    );
    let handles = (0..TURNS)
        .map(|_i| {
            let read_cache_service = Arc::clone(&read_cache_service);
            std::thread::spawn(move || {
                let mut rng = rand::thread_rng();
                for _i in 0..(COUNT / TURNS) {
                    // let obj = read_cache_service.getting(i as u64);
                    let random_int: u64 = rng.gen_range(0..(COUNT / TURNS) as u64 / 10);
                    let _obj1 = read_cache_service.lock().unwrap().getting(random_int);
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
