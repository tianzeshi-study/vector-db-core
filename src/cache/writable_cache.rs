use serde::{
    Deserialize,
    Serialize,
};
use std::{
    sync::{
        atomic::{
            AtomicUsize,
            Ordering,
        },
        Arc,
        Mutex,
    },
    thread,
    time::Duration,
};

use crate::vector_engine::VectorEngine;

const DEFAULT_MAX_CACHE_ITEMS: usize = 500000;

pub struct WritableCache<D, T>
where
    D: VectorEngine<T> + Sync + Send + 'static,
    T: Serialize
        + for<'de> Deserialize<'de>
        + 'static
        + std::fmt::Debug
        + Clone
        + Send
        + Sync,
{
    database: Arc<Mutex<D>>,
    cache: Arc<Mutex<Vec<T>>>,
    max_cache_items: Arc<AtomicUsize>,
}

impl<D, T> WritableCache<D, T>
where
    D: VectorEngine<T> + Sync + Send + 'static,
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
        let database = Arc::new(Mutex::<D>::new(VectorEngine::new(
            static_repository,
            dynamic_repository,
            initial_size_if_not_exists,
        )));
        let cache = Arc::new(Mutex::new(Vec::with_capacity(
            std::env::var("MAX_WRCACHE_ITEMS")
            .unwrap_or_else(|_| DEFAULT_MAX_CACHE_ITEMS.to_string())
            .parse::<usize>()
            .expect("MAX_WRCACHE_ITEMS must be a number")
        )));
        let max_cache_items = Arc::new(AtomicUsize::new(
            std::env::var("MAX_WRCACHE_ITEMS")
            .unwrap_or_else(|_| DEFAULT_MAX_CACHE_ITEMS.to_string())
            .parse::<usize>()
            .expect("MAX_WRCACHE_ITEMS must be a number")
        ));

        // 启动后台监控线程，该线程一直检测缓存状态，满足条件时将缓存数据 flush 到数据库
        {
            let cache_clone = Arc::clone(&cache);
            let database_clone = Arc::clone(&database);
            let max_cache_items_clone = Arc::clone(&max_cache_items);

            thread::spawn(move || loop {
                {
                    let cur_max = max_cache_items_clone.load(Ordering::Relaxed);
                    let mut cache_guard = cache_clone.lock().unwrap();
                    if cache_guard.len() >= cur_max {
                        println!(
                            "Cache is full with {} items, flushing...",
                            cache_guard.len()
                        );
                        let mut objs = Vec::with_capacity(cur_max);
                        objs.append(&mut *cache_guard);
                        database_clone.lock().unwrap().pushx(objs);
                        drop(cache_guard); // 释放锁后再写入数据库
                        println!("Flushed to database");
                    }
                }
                thread::sleep(Duration::from_millis(10)); // 控制检查频率，避免 CPU 占用过高
            });
        }

        Self {
            database,
            cache,
            max_cache_items,
        }
    }

    /// 单个对象写入：仅将对象追加到缓存，由后台线程负责 flush 数据
    pub fn push(&self, obj: T) {
        let mut cache = self.cache.lock().unwrap();
        cache.push(obj);
        // 不再额外启动线程，而是依靠后台线程定时检测
    }

    /// 批量写入对象：追加到缓存，由后台线程负责 flush 数据
    pub fn pushx(&self, mut objs: Vec<T>) {
        let mut cache = self.cache.lock().unwrap();
        cache.append(&mut objs);
        // 同样，不启用新的线程进行 flush
    }

    pub fn get_base_len(&self) -> usize {
        self.database.lock().unwrap().len()
    }

    pub fn get_cache_len(&self) -> usize {
        self.cache.lock().unwrap().len()
    }

    pub fn getting_obj_from_cache(&self, index: u64) -> T {
        self.cache.lock().unwrap()[index as usize].clone()
    }

    pub fn getting_objs_from_cache(&self, index: u64, count: u64) -> Vec<T> {
        let end_offset = (index + count) as usize;
        dbg!(index, count, end_offset);

        let cache = self.cache.lock().unwrap();
        dbg!(cache.len());
        cache[index as usize..end_offset].to_vec()
    }

    pub fn get_each_len(&self) -> (u64, u64, u64) {
        let cache_len = self.get_cache_len();
        let base_len = self.get_base_len();
        let total = cache_len + base_len;
        (cache_len as u64, base_len as u64, total as u64)
    }

    pub fn get_obj_from_cache(&self, index: u64) -> Option<T> {
        self.cache.lock().unwrap().get(index as usize).cloned()
    }

    pub fn get_objs_from_cache(&self, index: u64, count: u64) -> Option<Vec<T>> {
        let cache = self.cache.lock().unwrap();
        let end_offset = (index + count) as usize;
        dbg!(index, count, end_offset);
        dbg!(cache.len());
        cache
            .get(index as usize..end_offset)
            .map(|slice| slice.to_vec())
    }
}

impl<D, T> Drop for WritableCache<D, T>
where
    D: VectorEngine<T> + Sync + Send + 'static,
    T: Serialize
        + for<'de> Deserialize<'de>
        + 'static
        + std::fmt::Debug
        + Clone
        + Send
        + Sync,
{
    fn drop(&mut self) {
        let mut cache = self.cache.lock().unwrap();
        let cur_max = self.max_cache_items.load(Ordering::Relaxed);
        let cache_len = cache.len();
        println!("Preparing to drop {} items from cache", cache_len);
        if !cache.is_empty() {
            println!("Dropping {} items from cache", cache_len);
            let mut objs = Vec::with_capacity(cur_max);
            objs.append(&mut *cache);
            self.database.lock().unwrap().pushx(objs);
        }
    }
}

impl<D, T> VectorEngine<T> for WritableCache<D, T>
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
        self.get_cache_len() + self.get_base_len()
    }

    fn push(&self, obj: T) {
        self.push(obj);
    }

    fn pushx(&self, objs: Vec<T>) {
        self.pushx(objs);
    }

    fn pull(&self, index: u64) -> T {
        let cache = self.cache.lock().unwrap();
        let db = self.database.lock().unwrap();
        if index < db.len() as u64 {
            db.pull(index)
        } else if index < (db.len() + cache.len()) as u64 {
            if let Some(obj) = cache.get(index as usize - db.len()) {
                obj.clone()
            } else {
                panic!(
                    "index {} out of bounds! Database length: {}, cache length: {}",
                    index,
                    db.len(),
                    cache.len()
                );
            }
        } else {
            panic!(
                "index {} out of bounds! Database length: {}, cache length: {}",
                index,
                db.len(),
                cache.len()
            );
        }
    }

    fn pullx(&self, index: u64, count: u64) -> Vec<T> {
        let db = self.database.lock().unwrap();
        let cache = self.cache.lock().unwrap();
        let end_index = index + count - 1;
        if end_index < db.len() as u64 {
            println!("Reading from database");
            db.pullx(index, count)
        } else if index < db.len() as u64 && end_index < (db.len() + cache.len()) as u64 {
            println!("Reading from database and cache");
            let mut front = db.pullx(index, db.len() as u64 - index);
            let mut back = if let Some(objs) =
                cache.get(0..(end_index as usize - db.len() + 1))
            {
                objs.to_vec()
            } else {
                panic!(
                    "Failed to get from cache! Cache len: {}, index: {}, end_index: {}, db len: {}",
                    cache.len(),
                    index,
                    end_index,
                    db.len()
                );
            };
            front.append(&mut back);
            front
        } else if index >= db.len() as u64 && end_index < (db.len() + cache.len()) as u64
        {
            println!("Reading from cache");
            if let Some(objs) = cache
                .get((index as usize - db.len())..(end_index as usize - db.len() + 1))
            {
                objs.to_vec()
            } else {
                panic!(
                    "Failed to get from cache! Cache len: {}, index: {}, end_index: {}, db len: {}",
                    cache.len(),
                    index,
                    end_index,
                    db.len()
                );
            }
        } else {
            panic!(
                "index {} out of bounds! Database len: {}, cache len: {}, end_index: {}",
                index,
                db.len(),
                cache.len(),
                end_index
            );
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::services::{
        dynamic_vector_manage_service::DynamicVectorManageService,
        static_vector_manage_service::StaticVectorManageService,
    };

    const COUNT: usize = 1000;

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
        let my_obj = StaticStruct {
            my_usize: 443,
            my_u64: 53,
            my_u32: 4399,
            my_u16: 3306,
            my_u8: 22,
            my_boolean: true,
        };
        let my_service = WritableCache::<
            StaticVectorManageService<StaticStruct>,
            StaticStruct,
        >::new(
            "cacheS.bin".to_string(), "cacheSD.bin".to_string(), 1024
        );
        my_service.push(my_obj);
    }

    #[test]
    fn test_one_by_one_push_static() {
        let my_service = WritableCache::<
            StaticVectorManageService<StaticStruct>,
            StaticStruct,
        >::new(
            "cacheS.bin".to_string(), "cacheSD.bin".to_string(), 1024
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
    fn test_one_by_one_push_dynamic() {
        let my_service = WritableCache::<
            DynamicVectorManageService<StaticStruct>,
            StaticStruct,
        >::new(
            "cacheD.bin".to_string(), "cacheDD.bin".to_string(), 1024
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
    fn test_extend_static() {
        let mut objs = Vec::new();
        let my_service = WritableCache::<
            StaticVectorManageService<StaticStruct>,
            StaticStruct,
        >::new(
            "cacheS.bin".to_string(), "cacheSD.bin".to_string(), 1024
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
            objs.push(my_obj);
        }
        my_service.pushx(objs);
    }
}
