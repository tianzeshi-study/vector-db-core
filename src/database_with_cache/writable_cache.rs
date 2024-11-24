use rayon::prelude::*;
use serde::{
    Deserialize,
    Serialize,
};
use std::{
    collections::{
        HashMap,
        LinkedList,
    },
    sync::{
        Arc,
        Mutex,
    },
    time::Instant,
};

use crate::vector_engine::VectorEngine;

const PAGE_SIZE: u64 = 5000;
const MAX_CACHE_ITEMS: usize = 1024000;

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
    database: Arc<Mutex<D>>, // 底层数据库
    cache: Arc<Mutex<Vec<T>>>,
    max_cache_items: usize,
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
        Self {
            database: Arc::new(Mutex::new(VectorEngine::new(
                static_repository,
                dynamic_repository,
                initial_size_if_not_exists,
            ))),
            // cache: Arc::new(Mutex::new(Vec::new())),
            cache: Arc::new(Mutex::new(Vec::with_capacity(MAX_CACHE_ITEMS))),
            max_cache_items: MAX_CACHE_ITEMS,
        }
    }

    pub fn push(&self, obj: T) {
        let mut cache = self.cache.lock().unwrap();
        cache.push(obj);
        let cache_len = cache.len();
        let max_cache_items = self.max_cache_items;
        if cache.len() >= self.max_cache_items {
            // if cache_len >= self.max_cache_items {
            let cache_clone = Arc::clone(&self.cache);
            let database_clone = Arc::clone(&self.database);
            std::thread::spawn(move || {
                // self.database.extend(cache.to_vec());
                // *cache = Vec::new();
                // dbg!(cache_clone.lock().unwrap().len());
                // database_clone.lock().unwrap().extend(cache_clone.lock().unwrap().to_vec());
                // *cache_clone.lock().unwrap() = Vec::new();
                let mut objs = Vec::with_capacity(max_cache_items);
                objs.append(&mut *cache_clone.lock().unwrap());
                database_clone.lock().unwrap().extend(objs);
            });
        }
    }

    pub fn extend(&self, objs: Vec<T>) {
        let mut cache = self.cache.lock().unwrap();
        let mut objs = objs;
        cache.append(&mut objs);

        let cache_len = cache.len();
        // if cache_len >= self.max_cache_items {
        if cache.len() >= self.max_cache_items {
            let cache_clone = Arc::clone(&self.cache);
            let database_clone = Arc::clone(&self.database);
            let max_cache_items = self.max_cache_items;
            std::thread::spawn(move || {
                // database_clone.lock().unwrap().extend(cache_clone.lock().unwrap().to_vec());
                // *cache_clone.lock().unwrap() = Vec::new();
                let mut objs = Vec::with_capacity(max_cache_items);
                objs.append(&mut *cache_clone.lock().unwrap());
                database_clone.lock().unwrap().extend(objs);
            });
        }
    }

    pub fn get_base_len(&self) -> usize {
        self.database.lock().unwrap().len()
    }

    pub fn get_cache_len(&self) -> usize {
        self.cache.lock().unwrap().len()
    }
    pub fn len(&self) -> usize {
        // dbg!(self.get_cache_len()  , self.get_base_len());
        let (cache_len, base_len) = (self.get_cache_len(), self.get_base_len());
        // dbg!(&cache_len, &base_len);
        let length = cache_len + base_len;
        // dbg!(&length);
        length
    }

    pub fn getting_obj_from_cache(&self, index: u64) -> T {
        self.cache.lock().unwrap()[index as usize].clone()
    }

    pub fn getting_objs_from_cache(&self, index: u64, count: u64) -> Vec<T> {
        let end_offset = (index + count) as usize;
        // dbg!(index, count, end_offset);
        self.cache.lock().unwrap()[index as usize..end_offset].into()
    }

    pub fn get_obj_from_cache(&self, index: u64) -> Option<T> {
        self.cache.lock().unwrap().get(index as usize).cloned()
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
        let max_cache_items = self.max_cache_items;
        let cache_len = cache.len();
        println!("prepare to drop  {} item of cache ", cache_len);
        if cache.len() != 0 {
            println!("dropping {} item of cache ", cache_len);
            // self.database.lock().unwrap().extend(cache.to_vec());
            // *cache = Vec::with_capacity(max_cache_items);
            let mut objs = Vec::with_capacity(max_cache_items);
            objs.append(&mut *cache);
            self.database.lock().unwrap().extend(objs);
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
        self.len()
    }

    fn push(&self, obj: T) {
        self.push(obj);
    }

    fn extend(&self, objs: Vec<T>) {
        self.extend(objs);
    }

    fn pull(&self, index: u64) -> T {
        let obj = self.database.lock().unwrap().pull(index);

        obj
    }

    fn pullx(&self, index: u64, count: u64) -> Vec<T> {
        let objs = self.database.lock().unwrap().pullx(index, count);

        objs
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
    fn test_push_static_one() {
        let my_obj: StaticStruct = StaticStruct {
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
        // let mut objs = Vec::new();
        let my_service = WritableCache::<
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
            // my_vec.push(i as u64 *1000);

            my_service.push(my_obj);
        }
        // my_service.add_bulk(objs);
    }

    #[test]
    fn test_one_by_one_push_dynamic() {
        // let mut objs = Vec::new();
        let my_service = WritableCache::<
            DynamicVectorManageService<StaticStruct>,
            StaticStruct,
        >::new(
            "cacheD.bin".to_string(), "cacheDD.bin".to_string(), 1024
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
    fn test_extend_static() {
        let mut objs = Vec::new();
        let my_service = WritableCache::<
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
        my_service.extend(objs);
    }

    // #[test]
    // fn test_getting_static_multi_turns() {
    // let mut objs = Vec::new();
    // let my_service = WritableCache::<
    // StaticVectorManageService<StaticStruct>,
    // StaticStruct,
    // >::new(
    // "cacheS.bin".to_string(), "cacheSD.bin".to_string(), 1024
    // );
    // let read_cache_service =
    // ReadableCache::<StaticVectorManageService<StaticStruct>, StaticStruct>::new(
    // "cacheS.bin".to_string(),
    // "cacheSD.bin".to_string(),
    // 1024,
    // );
    // for i in 0..COUNT {
    // let my_obj: StaticStruct = StaticStruct {
    // my_usize: 443 + i,
    // my_u64: 53,
    // my_u32: 4399,
    // my_u16: 3306,
    // my_u8: 22,
    // my_boolean: true,
    // };
    //
    // objs.push(my_obj);
    // }
    // let start = Instant::now();
    // my_service.extend(objs);
    // let extend_cache_duration = start.elapsed();
    // println!("extend cache duration: {:?}", extend_cache_duration);
    // for turn  in  0..TURNS {
    // for i in 0..COUNT {
    // let obj = read_cache_service.getting(i as u64);
    // dbg!(obj);
    // }
    // }
    // let get_from_cache_duration = start.elapsed();
    // println!(
    // "get from  cache duration: {:?}",
    // get_from_cache_duration - extend_cache_duration
    // );
    // }
    //
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
    //
    // #[test]
    // fn test_read_static_bulk_in_pushed() {
    // let my_service = StaticVectorManageService::<StaticStruct>::new(
    // "cacheS.bin".to_string(),
    // "cacheSD.bin".to_string(),
    // 1024,
    // )
    // .unwrap();
    // my_service.read_bulk(0, COUNT as u64);
    // dbg!(my_service.get_length());
    // }
    //
    // #[test]
    // fn test_add_bulk_compare() {
    // let mut objs = Vec::new();
    // let my_service = StaticVectorManageService::<StaticStruct>::new(
    // "cacheS.bin".to_string(),
    // "cacheSD.bin".to_string(),
    // 1024,
    // )
    // .unwrap();
    // for i in 0..COUNT {
    // let my_obj: StaticStruct = StaticStruct {
    // my_usize: 443 + i,
    // my_u64: 53,
    // my_u32: 4399,
    // my_u16: 3306,
    // my_u8: 22,
    // my_boolean: true,
    // };
    // my_vec.push(i as u64 *1000);
    // objs.push(my_obj);
    //
    // my_service.push(&my_obj);
    // my_service.add(my_obj);
    // }
    // my_service.add_bulk(objs);
    // }
}
