use lru::LruCache;
use rayon::prelude::*;
use serde::{
    Deserialize,
    Serialize,
};
use std::sync::{
    Arc,
    Mutex,
};

use crate::vector_engine::VectorEngine;

const MAX_CACHE_ITEMS: usize = 1024000;

pub struct ReadableCache<D, T>
where
    D: VectorEngine<T> + 'static + Send,
    T: Serialize
        + for<'de> Deserialize<'de>
        + 'static
        + std::fmt::Debug
        + Clone
        + Send
        + Sync,
{
    database: D,
    // 使用 LruCache 来同时维护数据与 LRU 顺序，容量设置为 MAX_CACHE_ITEMS
    cache: Arc<Mutex<LruCache<u64, T>>>,
}

impl<D, T> ReadableCache<D, T>
where
    D: VectorEngine<T> + 'static + Send,
    T: Serialize
        + for<'de> Deserialize<'de>
        + 'static
        + std::fmt::Debug
        + Clone
        + Send
        + Sync,
{
    pub fn new(
        static_repository: String,
        dynamic_repository: String,
        initial_size_if_not_exists: u64,
    ) -> Self {
        Self {
            database: VectorEngine::new(
                static_repository,
                dynamic_repository,
                initial_size_if_not_exists,
            ),
            cache: Arc::new(Mutex::new(LruCache::new(
                std::num::NonZero::new(
                    std::env::var("MAX_RECACHE_ITEMS")
                        .unwrap_or_else(|_| MAX_CACHE_ITEMS.to_string())
                        .parse::<usize>()
                        .expect("MAX_RECACHE_ITEMS must be a number"),
                )
                .unwrap(),
            ))),
        }
    }

    /// 同步从缓存或数据库中获取数据。
    /// 如果命中缓存，则 LruCache 内部会自动将该 key 标记为最近使用。
    pub fn getting(&self, index: u64) -> T {
        {
            // 读取缓存，如果命中则返回，同时更新 recency（get_mut 会更新 recency）
            let mut cache = self.cache.lock().unwrap();
            if let Some(page_data) = cache.get_mut(&index) {
                return page_data.clone();
            }
        }
        // 缓存未命中时，同步从数据库中拉取数据
        let page_data = self.database.pull(index);
        self.add_to_cache(index, page_data.clone());
        page_data
    }

    /// 批量获取数据，仅通过数据库拉取，不更新缓存
    pub fn getting_lot(&self, index: u64, count: u64) -> Vec<T> {
        self.database.pullx(index, count)
    }

    /// 同步添加单个数据到缓存
    pub fn add_to_cache(&self, index: u64, data: T) {
        let mut cache = self.cache.lock().unwrap();
        // put 方法会自动将该 key 插入或更新 recency，
        // 若容量超过上限会自动淘汰最旧的条目
        cache.put(index, data);
    }

    /// 同步批量添加数据到缓存
    /// 这里利用 Rayon 并行计算每个元素的 key，然后在当前线程中将所有数据插入 LruCache
    pub fn add_bulk_to_cache(&self, index: u64, objs: Vec<T>) {
        // 并行生成键值对
        let entries: Vec<(u64, T)> = objs
            .into_par_iter()
            .enumerate()
            .map(|(i, obj)| ((i as u64 + index), obj))
            .collect();
        let mut cache = self.cache.lock().unwrap();
        for (k, v) in entries {
            cache.put(k, v);
        }
    }

    pub fn get_length(&self) -> usize {
        self.database.len()
    }

    pub fn add(&self, obj: T) {
        self.database.push(obj);
    }

    pub fn add_bulk(&self, objs: Vec<T>) {
        self.database.pushx(objs);
    }
}

impl<D, T> VectorEngine<T> for ReadableCache<D, T>
where
    D: VectorEngine<T> + 'static + Send + Sync,
    T: Serialize
        + for<'de> Deserialize<'de>
        + 'static
        + std::fmt::Debug
        + Clone
        + Send
        + Sync,
{
    fn new(
        static_repository: String,
        dynamic_repository: String,
        initial_size_if_not_exists: u64,
    ) -> Self {
        Self::new(
            static_repository,
            dynamic_repository,
            initial_size_if_not_exists,
        )
    }

    fn len(&self) -> usize {
        self.get_length()
    }

    fn push(&self, obj: T) {
        self.database.push(obj);
    }

    fn pushx(&self, objs: Vec<T>) {
        self.database.pushx(objs);
    }

    fn pull(&self, index: u64) -> T {
        self.getting(index)
    }

    fn pullx(&self, index: u64, count: u64) -> Vec<T> {
        self.getting_lot(index, count)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::services::{
        dynamic_vector_manage_service::DynamicVectorManageService,
        static_vector_manage_service::StaticVectorManageService,
    };
    use std::time::Instant;
    const COUNT: usize = 100;
    const TURNS: usize = 5;

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
    fn test_one_by_one_getting_static() {
        let my_service = ReadableCache::<
            StaticVectorManageService<StaticStruct>,
            StaticStruct,
        >::new(
            "cacheS.bin".to_string(), "cacheSD.bin".to_string(), 1024
        );
        for i in 0..COUNT {
            my_service.getting(i as u64);
        }
    }

    #[test]
    fn test_one_by_one_push_dynamic() {
        let my_service: DynamicVectorManageService<StaticStruct> =
            VectorEngine::<StaticStruct>::new(
                "cacheD1.bin".to_string(),
                "cacheDD1.bin".to_string(),
                1024,
            );

        for i in 0..COUNT {
            let my_obj = StaticStruct {
                my_usize: 443 + i,
                my_u64: 53,
                my_u32: 4399,
                my_u16: 3306,
                my_u8: 22,
                my_boolean: true,
            };
            my_service.push(my_obj);
        }
    }

    #[test]
    fn test_extend_dynamic_engine() {
        let my_service: DynamicVectorManageService<StaticStruct> =
            VectorEngine::<StaticStruct>::new(
                "cacheD1.bin".to_string(),
                "cacheDD1.bin".to_string(),
                1024,
            );
        let objs: Vec<_> = (0..COUNT)
            .map(|i| StaticStruct {
                my_usize: 443 + i,
                my_u64: 53,
                my_u32: 4399,
                my_u16: 3306,
                my_u8: 22,
                my_boolean: true,
            })
            .collect();
        my_service.pushx(objs);
    }

    #[test]
    fn test_one_by_one_getting_dynamic() {
        let my_service: DynamicVectorManageService<StaticStruct> =
            VectorEngine::<StaticStruct>::new(
                "cacheD2.bin".to_string(),
                "cacheDD2.bin".to_string(),
                1024,
            );
        let objs: Vec<_> = (0..COUNT)
            .map(|i| StaticStruct {
                my_usize: 443 + i,
                my_u64: 53,
                my_u32: 4399,
                my_u16: 3306,
                my_u8: 22,
                my_boolean: true,
            })
            .collect();
        my_service.pushx(objs);

        let read_service = ReadableCache::<
            DynamicVectorManageService<StaticStruct>,
            StaticStruct,
        >::new(
            "cacheD2.bin".to_string(), "cacheDD2.bin".to_string(), 1024
        );

        for i in 0..COUNT {
            let obj = read_service.getting(i as u64);
            assert_eq!(443 + i, obj.my_usize);
        }
    }

    #[test]
    fn test_getting_lot_static() {
        let path = "cacheS3.bin".to_string();
        if std::path::Path::new(&path).exists() {
            std::fs::remove_file(&path).expect("Unable to remove file");
        }
        {
            let objs: Vec<_> = (0..COUNT)
                .map(|i| StaticStruct {
                    my_usize: 443 + i,
                    my_u64: 53,
                    my_u32: 4399,
                    my_u16: 3306,
                    my_u8: 22,
                    my_boolean: true,
                })
                .collect();
            let my_service: StaticVectorManageService<StaticStruct> =
                VectorEngine::<StaticStruct>::new(
                    "cacheS3.bin".to_string(),
                    "cacheSD3.bin".to_string(),
                    1024,
                );
            let start = Instant::now();
            my_service.pushx(objs);
            dbg!(my_service.len());
            let os = my_service.pullx(0, COUNT as u64);
            dbg!(&os[os.len() - 1]);
            println!("extend cache duration: {:?}", start.elapsed());
        }
        let read_cache_service =
            ReadableCache::<StaticVectorManageService<StaticStruct>, StaticStruct>::new(
                "cacheS3.bin".to_string(),
                "cacheSD3.bin".to_string(),
                1024,
            );
        let awake = Instant::now();
        let objs = read_cache_service.getting_lot(0, COUNT as u64);
        println!("get lot cache duration: {:?}", awake.elapsed());
        assert_eq!(442 + COUNT, objs[COUNT as usize - 1].my_usize);
        assert_eq!(COUNT, objs.len());
        assert_eq!(COUNT, read_cache_service.get_length());
    }

    #[test]
    fn test_getting_static_multi_turns() {
        let path = "cacheS4.bin".to_string();
        if std::path::Path::new(&path).exists() {
            std::fs::remove_file(&path).expect("Unable to remove file");
        }
        {
            let objs: Vec<_> = (0..COUNT)
                .map(|i| StaticStruct {
                    my_usize: 443 + i,
                    my_u64: 53,
                    my_u32: 4399,
                    my_u16: 3306,
                    my_u8: 22,
                    my_boolean: true,
                })
                .collect();
            let my_service: StaticVectorManageService<StaticStruct> =
                VectorEngine::<StaticStruct>::new(
                    "cacheS4.bin".to_string(),
                    "cacheSD4.bin".to_string(),
                    1024,
                );
            let start = Instant::now();
            my_service.pushx(objs);
            println!("extend cache duration: {:?}", start.elapsed());
        }
        let read_cache_service =
            ReadableCache::<StaticVectorManageService<StaticStruct>, StaticStruct>::new(
                "cacheS4.bin".to_string(),
                "cacheSD4.bin".to_string(),
                1024,
            );
        let start = Instant::now();
        for _ in 0..TURNS {
            for i in 0..COUNT {
                let obj = read_cache_service.getting(i as u64);
                assert_eq!(443 + i, obj.my_usize);
            }
        }
        println!("get from cache duration: {:?}", start.elapsed());
        assert_eq!(COUNT, read_cache_service.get_length());
    }

    #[test]
    fn test_read_static_bulk_compare() {
        let my_service = StaticVectorManageService::<StaticStruct>::new(
            "cacheS.bin".to_string(),
            "cacheSD.bin".to_string(),
            1024,
        )
        .unwrap();
        my_service.read_bulk(0, COUNT as u64);
    }

    #[test]
    fn test_add_bulk_compare() {
        let objs: Vec<_> = (0..COUNT)
            .map(|i| StaticStruct {
                my_usize: 443 + i,
                my_u64: 53,
                my_u32: 4399,
                my_u16: 3306,
                my_u8: 22,
                my_boolean: true,
            })
            .collect();
        let my_service = StaticVectorManageService::<StaticStruct>::new(
            "cacheS.bin".to_string(),
            "cacheSD.bin".to_string(),
            1024,
        )
        .unwrap();
        my_service.add_bulk(objs);
    }
}
