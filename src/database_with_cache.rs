use serde::{Deserialize, Serialize};
use std::collections::{HashMap, LinkedList};
use std::sync::{Arc, Mutex};

use crate::file_access_service::FileAccessService;
use crate::vector_engine::VectorDatabase;

const PAGE_SIZE: usize = 2;
const MAX_CACHE_ITEMS: usize = 900000;

/*
pub struct DatabaseWithCache<D<T>: VectorDatabase<T>>
where
T: Serialize + for<'de> Deserialize<'de> + 'static + std::fmt::Debug + Clone + Send + Sync,
{
    database: D<T>,
}
*/

pub struct WritableCache<D, T>
where
    D: VectorDatabase<T>,
    T: Serialize + for<'de> Deserialize<'de> + 'static + std::fmt::Debug + Clone + Send + Sync,
{
    database: D,                     // 底层数据库
    cached_data: Arc<Mutex<Vec<T>>>, // 缓存数据
    max_cache_items: usize,
}

pub struct ReadableCache<D, T>
where
    D: VectorDatabase<T>,
    T: Serialize + for<'de> Deserialize<'de> + 'static + std::fmt::Debug + Clone + Send + Sync,
{
    writable_cache: WritableCache<D, T>,
    cache: Arc<Mutex<HashMap<u64, T>>>,
    lru_list: Arc<Mutex<LinkedList<u64>>>,
    page_size: usize,
    max_cache_items: usize,
}

impl<D, T> WritableCache<D, T>
where
    D: VectorDatabase<T>,
    T: Serialize + for<'de> Deserialize<'de> + 'static + std::fmt::Debug + Clone + Send + Sync,
{
    pub fn new(
        static_repository: String,
        dynamic_repository: String,
        initial_size_if_not_exists: u64,
    ) -> Self {
        Self {
            database: VectorDatabase::new(
                static_repository,
                dynamic_repository,
                initial_size_if_not_exists,
            ),
            // cached_data: Arc::new(Mutex::new(Vec::new())),
            cached_data: Arc::new(Mutex::new(Vec::with_capacity(MAX_CACHE_ITEMS))),
            max_cache_items: MAX_CACHE_ITEMS,
        }
    }

    pub fn push(&self, obj: T) {
        let mut cache = self.cached_data.lock().unwrap();
        cache.push(obj);
        if cache.len() >= self.max_cache_items {
            self.database.extend(cache.to_vec());
            *cache = Vec::new();
        }
    }

    pub fn extend(&self, objs: Vec<T>) {
        let mut cache = self.cached_data.lock().unwrap();
        let mut objs = objs;
        cache.append(&mut objs);
        if cache.len() >= self.max_cache_items {
            self.database.extend(cache.to_vec());
            *cache = Vec::new();
        }
    }
}

impl<D, T> Drop for WritableCache<D, T>
where
    D: VectorDatabase<T>,
    T: Serialize + for<'de> Deserialize<'de> + 'static + std::fmt::Debug + Clone + Send + Sync,
{
    fn drop(&mut self) {
        let cache = self.cached_data.lock().unwrap();
        if cache.len() != 0 {
            self.database.extend(cache.to_vec());
        }
    }
}

/// 缓存文件访问服务，实现带缓存的文件读写，使用LRU缓存淘汰策略
pub struct CachedFileAccessService {
    file_access_service: FileAccessService,
    cache: Arc<Mutex<HashMap<u64, Vec<u8>>>>,
    lru_list: Arc<Mutex<LinkedList<u64>>>,
    page_size: usize,
    max_cache_items: usize,
}

impl CachedFileAccessService {
    /// 创建一个 `CachedFileAccessService` 实例
    pub fn new(path: String, initial_size_if_not_exists: u64) -> Self {
        Self {
            file_access_service: FileAccessService::new(path, initial_size_if_not_exists),
            cache: Arc::new(Mutex::new(HashMap::new())),
            lru_list: Arc::new(Mutex::new(LinkedList::new())),
            page_size: PAGE_SIZE,
            max_cache_items: MAX_CACHE_ITEMS,
            // page_size: 1024 * 1024, // 默认值 1MB
            // max_cache_items: 512,    // 默认值 512
        }
    }

    /// 写入数据到文件的指定偏移量，同时清除缓存中将被覆盖的页面
    pub fn write_in_file(&self, offset: u64, data: &[u8]) {
        let start_page = offset / self.page_size as u64;
        let end_page = (offset + data.len() as u64) / self.page_size as u64;

        {
            let mut cache = self.cache.lock().unwrap();
            let mut lru_list = self.lru_list.lock().unwrap();

            for page in start_page..=end_page {
                if cache.contains_key(&page) {
                    cache.remove(&page);
                    // lru_list.retain(|&x| x != page);
                    remove_item(&mut lru_list, page);
                }
            }
        }

        self.file_access_service.write_in_file(offset, data);
    }

    /// 从文件的指定偏移量读取数据，并使用缓存提高读取效率
    pub fn read_in_file(&self, offset: u64, length: usize) -> Vec<u8> {
        let result = vec![0; length];
        /*
        let mut current_offset = offset;
        let mut current_offset = length ;
        dbg!(current_offset);
        let mut result_offset = 0;
        let mut remaining_length = length;

        while remaining_length > 0 {
        let page_offset = current_offset / self.page_size;
        let page_start = current_offset % self.page_size;
        dbg!(current_offset, self.page_size);

        let bytes_to_read = std::cmp::min(remaining_length, self.page_size - page_start);

        let page_data = self.get_page_from_cache(offset, page_offset as u64);
        dbg!(&page_data.len());
        dbg!(result.len(), result_offset, bytes_to_read, page_start);
        result[result_offset..result_offset + bytes_to_read]
        .copy_from_slice(&page_data[page_start..page_start + bytes_to_read]);

        current_offset += bytes_to_read;
        result_offset += bytes_to_read;
        remaining_length -= bytes_to_read;
        }

        result
        */
        self.file_access_service
            .read_in_file(offset, length as usize)
    }

    /// 从缓存中获取页面数据，如果缓存缺失则从文件中读取并添加到缓存
    fn get_page_from_cache(&self, file_offset: u64, page_offset: u64) -> Vec<u8> {
        {
            let cache = self.cache.lock().unwrap();
            // let mut lru_list = self.lru_list.lock().unwrap();

            if let Some(page_data) = cache.get(&page_offset) {
                if self.should_update_lru(&page_offset) {
                    // lru_list.retain(|&x| x != page_offset);
                    let mut lru_list = self.lru_list.lock().unwrap();
                    remove_item(&mut lru_list, page_offset);
                    lru_list.push_back(page_offset);
                }

                return page_data.clone();
            }
        }
        dbg!(page_offset, self.page_size);
        // let page_data = self.file_access_service.read_in_file(file_offset, page_offset * self.page_size as u64);
        let read_length = page_offset * self.page_size as u64;
        dbg!(file_offset, read_length);
        let page_data = self
            .file_access_service
            .read_in_file(file_offset, read_length as usize);
        self.add_to_cache(page_offset, page_data.clone());
        page_data
    }

    /// 检查是否需要更新LRU列表中的页面访问记录
    fn should_update_lru(&self, page_offset: &u64) -> bool {
        let lru_list = self.lru_list.lock().unwrap();
        const VERY_RECENT_PAGE_ACCESS_LIMIT: usize = 0x10;

        let mut recent_access = lru_list.iter().rev().take(VERY_RECENT_PAGE_ACCESS_LIMIT);
        !recent_access.any(|&x| x == *page_offset)
    }

    /// 将页面数据添加到缓存，如果缓存满则移除最久未使用的页面
    fn add_to_cache(&self, page_offset: u64, data: Vec<u8>) {
        let mut cache = self.cache.lock().unwrap();
        let mut lru_list = self.lru_list.lock().unwrap();

        while cache.len() >= self.max_cache_items && !lru_list.is_empty() {
            if let Some(oldest_page) = lru_list.pop_front() {
                cache.remove(&oldest_page);
            }
        }

        cache.insert(page_offset, data);
        lru_list.push_back(page_offset);
    }
}

// fn remove_item(lru_list: &mut MutexGuard<'_, LinkedList<u64>>, page: u64) {
fn remove_item(lru_list: &mut LinkedList<u64>, page: u64) {
    let mut current = lru_list.front(); // 从列表的前端开始

    while let Some(&value) = current {
        // 获取当前节点的值
        if value == page {
            // 如果值与要删除的元素匹配，使用 pop_front() 删除元素
            lru_list.pop_front(); // 删除头部元素
                                  // 更新 current 为下一个元素
            current = lru_list.front(); // 更新为新头部
        } else {
            // 继续检查下一个元素
            current = lru_list.iter().nth(1).or_else(|| None); // 获取下一个元素
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::dynamic_vector_manage_service::DynamicVectorManageService;
    use crate::static_vector_manage_service::StaticVectorManageService;
    const COUNT: usize = 1000000;

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
    fn test_one_by_one_push_dynamic() {
        // let mut objs = Vec::new();
        let my_service =
            WritableCache::<DynamicVectorManageService<StaticStruct>, StaticStruct>::new(
                "cacheD.bin".to_string(),
                "cacheDD.bin".to_string(),
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
    /*

        #[test]
        fn test_read_static_one() {
            let my_service = StaticVectorManageService::<StaticStruct>::new(
                "TestDynamicData.bin".to_string(),
                "TestDynamicDataDynamic.bin".to_string(),
                1024,
            )
            .unwrap();
            my_service.read(COUNT);
        }
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
}
