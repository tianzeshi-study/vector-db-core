use blake2::{
    Blake2b,
    Blake2s,
};
use chrono::{
    DateTime,
    Local,
    Utc,
};
use serde::{
    Deserialize,
    Serialize,
};
use sha2::{
    Digest,
    Sha256,
    Sha512,
};
use std::{
    mem::size_of,
    time::{
        Duration,
        Instant,
        SystemTime,
        UNIX_EPOCH,
    },
};
use uuid::Uuid;
use vector_db_core::StaticVectorManageService;

const COUNT: u64 = 1000;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct TestStruct {
    // 时间类型
    time_duration: Duration,
    system_time: SystemTime,
    // instant: Instant,
    unix_epoch: Duration,
    // chrono_utc: DateTime<Utc>,
    // chrono_local: DateTime<Local>,

    // UUID 类型
    // uuid: Uuid,

    // 哈希类型
    sha256_hash: [u8; 32],
    // sha512_hash: [u8; 64],
    // blake2b_hash: [u8; 64],
    blake2s_hash: [u8; 32],
}

impl TestStruct {
    fn new(i: u64) -> Self {
        // 时间类型初始化，动态添加偏移
        let time_duration = Duration::new(i, (i % 1_000_000) as u32);
        let system_time = SystemTime::now() + Duration::new(i, 0);
        let _instant = Instant::now() + Duration::from_secs(i);
        let unix_epoch =
            UNIX_EPOCH.elapsed().unwrap_or_default() + Duration::from_secs(i);
        let _chrono_utc = Utc::now() + chrono::Duration::seconds(i as i64);
        let _chrono_local = Local::now() + chrono::Duration::seconds(i as i64);

        // UUID 初始化，使用动态种子
        let _uuid = Uuid::from_u128(i as u128 | 0x1234_5678_1234_5678);

        // 哈希初始化，基于索引值生成数据
        // let data = format!("dynamic_data_{}", i.clone()).as_bytes().clone();
        let formatted = format!("dynamic_data_{}", i);
        let data = formatted.as_bytes();

        let sha256_hash = {
            let mut hasher = Sha256::new();
            hasher.update(data);
            hasher.finalize()
        };

        let _sha512_hash = {
            let mut hasher = Sha512::new();
            hasher.update(data);
            hasher.finalize()
        };

        let _blake2b_hash: [u8; 64] = {
            let mut hasher = Blake2b::new();
            hasher.update(data);
            hasher.finalize()
        }
        .into();

        let blake2s_hash = {
            let mut hasher = Blake2s::new();
            hasher.update(data);
            hasher.finalize()
        };

        Self {
            time_duration,
            system_time,
            // instant,
            unix_epoch,
            // chrono_utc,
            // chrono_local,
            // uuid,
            sha256_hash: sha256_hash.into(),
            // sha512_hash: sha512_hash.into(),
            // blake2b_hash: blake2b_hash.into(),
            blake2s_hash: blake2s_hash.into(),
        }
    }
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
fn test_add_mix_data() {
    let (storage, bin) = clean("test_add_mix_data");
    // 打印结构体的大小
    println!("Size of TestStruct: {} bytes", size_of::<TestStruct>());
    dbg!(
        size_of::<TestStruct>(),
        size_of::<[u8; 32]>(),
        size_of::<Uuid>(),
        size_of::<DateTime<Local>>(),
        size_of::<DateTime<Utc>>(),
        size_of::<Duration>(),
        size_of::<SystemTime>(),
        size_of::<Duration>()
    );
    let my_service = StaticVectorManageService::<TestStruct>::new(
        storage,
        bin,
        1024,
    )
    .unwrap();
    let mut objs = Vec::new();
    // 模拟循环生成不同实例
    for i in 0..COUNT {
        let instance = TestStruct::new(i);
        objs.push(instance.clone());
        // 前 5 次打印实例，后续略过以提高性能
        if i < 1 {
            println!("Instance {}: {:?}", i, instance);
        }
    }
    my_service.add_bulk(objs);
    // my_service.read_bulk(0, COUNT);
}

#[test]
fn test_read_mix_data() {
    let (storage, bin) = clean("test_read_mix_data");
    let my_service = StaticVectorManageService::<TestStruct>::new(
        storage,
        bin,
        1024,
    )
    .unwrap();
    dbg!(&my_service.get_length());
    let mut objs = Vec::new();
    // 模拟循环生成不同实例
    for i in 0..COUNT {
        let instance = TestStruct::new(i);
        objs.push(instance.clone());
        // 前 5 次打印实例，后续略过以提高性能
        if i < 1 {
            println!("Instance {}: {:?}", i, instance);
        }
    }
    my_service.add_bulk(objs);
    my_service.read_bulk(0, COUNT);
}

#[test]
fn test_controled_io_mix_data() {
    let (storage, bin) = clean("test_controled_io_mix_data");
    // 打印结构体的大小
    println!("Size of TestStruct: {} bytes", size_of::<TestStruct>());
    dbg!(
        size_of::<TestStruct>(),
        size_of::<[u8; 32]>(),
        size_of::<Uuid>(),
        size_of::<DateTime<Local>>(),
        size_of::<DateTime<Utc>>(),
        size_of::<Duration>(),
        size_of::<SystemTime>(),
        size_of::<Duration>()
    );
    let my_service = StaticVectorManageService::<TestStruct>::new(
        storage,
        bin,
        1024,
    )
    .unwrap();
    let mut objs = Vec::new();
    // 模拟循环生成不同实例
    for i in 0..COUNT {
        let instance = TestStruct::new(i);
        objs.push(instance.clone());
        // 前 5 次打印实例，后续略过以提高性能
        if i < 1 {
            println!("Instance {}: {:?}", i, instance);
        }
    }
    my_service.add_bulk(objs);
    my_service.read_bulk(0, COUNT);
}

#[test]
fn test_io_mix_data() {
    let (storage, bin) = clean("test_io_mix_data");
    // 打印结构体的大小
    println!("Size of TestStruct: {} bytes", size_of::<TestStruct>());
    dbg!(
        size_of::<TestStruct>(),
        size_of::<[u8; 32]>(),
        size_of::<Uuid>(),
        size_of::<DateTime<Local>>(),
        size_of::<DateTime<Utc>>(),
        size_of::<Duration>(),
        size_of::<SystemTime>(),
        size_of::<Duration>()
    );
    let my_service = StaticVectorManageService::<TestStruct>::new(
        storage,
        bin,
        1024,
    )
    .unwrap();
    let mut objs = Vec::new();

    for i in 0..COUNT {
        let instance = TestStruct::new(i);
        objs.push(instance.clone());
        // 前 5 次打印实例，后续略过以提高性能
        if i < 1 {
            println!("Instance {}: {:?}", i, instance);
        }
    }
    my_service.add_bulk(objs);
    my_service.read_bulk(0, COUNT);
}
