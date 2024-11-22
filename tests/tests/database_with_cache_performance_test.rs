    use std::sync::{Arc, Mutex};
    use std::thread;
    use rand::Rng;
    use std::time::{Instant};
    use vector_db_core::{
    DynamicVectorManageService,
    ReadableCache,
    StaticVectorManageService,
    WritableCache,
    DatabaseWithCache,
};
use serde::{Serialize,Deserialize};
const   COUNT: i32 = 1000000;


    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct TestData {
        value: i32,
    }

    #[test]
    fn single_thread_performance() {
        let db = DatabaseWithCache::<StaticVectorManageService<TestData>, TestData>::new(
            "static_repo.bin".to_string(),
            "dynamic_repo.bin".to_string(),
            100,
        );

        let start_time = Instant::now();
        // for i in 0..10_000 {
        for i in 0..COUNT  {
        // for i in 0..100 {
            db.push(TestData { value: i });
        }
        let duration = start_time.elapsed();
        println!("Single-thread push: {:?}", duration);
        
        // assert_eq!(db.len(), 10_000);
        assert_eq!(db.len(), COUNT as usize);
    }

    #[test]
    fn multi_thread_write_performance() {
        let db = Arc::new(DatabaseWithCache::<StaticVectorManageService<TestData>, TestData>::new(
            "static_repo1.bin".to_string(),
            "dynamic_repo1.bin".to_string(),
            100,
        ));

        let start_time = Instant::now();
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let db_clone = Arc::clone(&db);
                thread::spawn(move || {
                    for i in 0..2_500 {
                        db_clone.push(TestData { value: i });
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start_time.elapsed();
        println!("Multi-thread push: {:?}", duration);
        
        assert_eq!(db.len(), 10_000);
    }

    #[test]
    fn random_access_performance() {
        let db = DatabaseWithCache::<StaticVectorManageService<TestData>, TestData>::new(
            "static_repo2.bin".to_string(),
            "dynamic_repo2.bin".to_string(),
            100,
        );

        // 插入数据
        for i in 0..10_000 {
            db.push(TestData { value: i });
        }

        // 随机读取
        let mut rng = rand::thread_rng();
        let start_time = Instant::now();
        for _ in 0..1_000 {
            let idx = rng.gen_range(0..10_000);
            let _ = db.getting(idx);
        }
        let duration = start_time.elapsed();
        println!("Random access: {:?}", duration);
    }

    #[test]
    fn multi_thread_random_read() {
        let db = Arc::new(DatabaseWithCache::<StaticVectorManageService<TestData>, TestData>::new(
            "static_repo3.bin".to_string(),
            "dynamic_repo3.bin".to_string(),
            100,
        ));

        // 插入数据
        for i in 0..10_000 {
            db.push(TestData { value: i });
        }

        let start_time = Instant::now();
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let db_clone = Arc::clone(&db);
                thread::spawn(move || {
                    let mut rng = rand::thread_rng();
                    for _ in 0..2_500 {
                        let idx = rng.gen_range(0..10_000);
                        let _ = db_clone.getting(idx);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start_time.elapsed();
        println!("Multi-thread random read: {:?}", duration);
    }


#[test]
    fn test_get_total_len() {
        let db = DatabaseWithCache::<StaticVectorManageService<TestData>, TestData>::new(
            "static_repo.bin".to_string(),
            "dynamic_repo.bin".to_string(),
            100,
        );

        let start_time = Instant::now();
        
        dbg!(db.len());
        let duration = start_time.elapsed();
        println!("Single-thread push: {:?}", duration);
        
    }
    
    

    fn test_getting_one_by_one() {
        let db = DatabaseWithCache::<StaticVectorManageService<TestData>, TestData>::new(
            "static_repo.bin".to_string(),
            "dynamic_repo.bin".to_string(),
            100,
        );
        let mut rng = rand::thread_rng();
        let start_time = Instant::now();
        // for _ in 0..1_000 {
        for i in 0..20 {
            // let idx = rng.gen_range(0..10_000);
            // let _ = db.getting(idx);
            let obj  = db.getting(i as u64);
            dbg!(i,  obj);
        }
        let duration = start_time.elapsed();
        println!("Random access: {:?}", duration);
    }