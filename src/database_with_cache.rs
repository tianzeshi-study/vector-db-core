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

use crate::{
    file_access_service::FileAccessService,
    vector_engine::VectorDatabase,
};

const PAGE_SIZE: u64 = 5000;
const MAX_CACHE_ITEMS: usize = 900000;

pub struct WritableCache<D, T>
where
    D: VectorDatabase<T>,
    T: Serialize
        + for<'de> Deserialize<'de>
        + 'static
        + std::fmt::Debug
        + Clone
        + Send
        + Sync,
{
    database: D,                     // 底层数据库
    cached_data: Arc<Mutex<Vec<T>>>, // 缓存数据
    max_cache_items: usize,
}

impl<D, T> WritableCache<D, T>
where
    D: VectorDatabase<T>,
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
    T: Serialize
        + for<'de> Deserialize<'de>
        + 'static
        + std::fmt::Debug
        + Clone
        + Send
        + Sync,
{
    fn drop(&mut self) {
        let cache = self.cached_data.lock().unwrap();
        if cache.len() != 0 {
            self.database.extend(cache.to_vec());
        }
    }
}

pub struct ReadableCache<D, T>
where
    D: VectorDatabase<T>,
    T: Serialize
        + for<'de> Deserialize<'de>
        + 'static
        + std::fmt::Debug
        + Clone
        + Send
        + Sync,
{
    // writable_cache: WritableCache<D, T>,
    database: D,
    cache: Arc<Mutex<HashMap<u64, T>>>,
    lru_list: Arc<Mutex<LinkedList<u64>>>,
    page_size: u64,
    max_cache_items: usize,
}

impl<D, T> ReadableCache<D, T>
where
    D: VectorDatabase<T>,
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
            database: VectorDatabase::new(
                static_repository,
                dynamic_repository,
                initial_size_if_not_exists,
            ),
            cache: Arc::new(Mutex::new(HashMap::new())),
            lru_list: Arc::new(Mutex::new(LinkedList::new())),
            page_size: PAGE_SIZE,
            max_cache_items: MAX_CACHE_ITEMS,
        }
    }

    /// 从文件的指定偏移量读取数据，并使用缓存提高读取效率
    pub fn pull_lot(&self, index: u64, count: u64) -> Vec<T> {
        let mut result = Vec::with_capacity(count as usize);

        let mut current_offset = index;
        let mut remaining_length = count;
        let mut result_offset = 0;

        while remaining_length > 0 {
            let page_offset = current_offset / self.page_size;
            let page_start = current_offset % self.page_size;
            dbg!(current_offset, self.page_size);

            let bytes_to_read =
                std::cmp::min(remaining_length, self.page_size - page_start);

            let page_data = self.get_page_from_cache(index, page_offset as u64);
            dbg!(&page_data.len());
            dbg!(result.len(), result_offset, bytes_to_read, page_start);
            result
                [result_offset as usize..result_offset as usize + bytes_to_read as usize]
                .clone_from_slice(
                    &page_data[page_start as usize
                        ..page_start as usize + bytes_to_read as usize],
                );

            current_offset += bytes_to_read;
            result_offset += bytes_to_read;
            remaining_length -= bytes_to_read;
        }

        result
    }

    pub fn get(&self, index: u64) -> T {
        let data = self.get_obj_from_cache(index);

        data
    }
    pub fn get_lot(&self, index: u64, count : u64) -> Vec<T> {
        let data = self.get_objs_from_cache(index, count );

        data
    }

    /// 从缓存中获取页面数据，如果缓存缺失则从文件中读取并添加到缓存
    fn get_page_from_cache(&self, db_offset: u64, page_offset: u64) -> Vec<T> {
        // let cache = self.cache.lock().unwrap();

        if let Some(page_data) = self.cache.lock().unwrap().get(&page_offset) {
            if self.should_update_lru(&page_offset) {
                let mut lru_list = self.lru_list.lock().unwrap();
                // lru_list.retain(|&x| x != page_offset);
                self.remove_item(page_offset);
                lru_list.push_back(page_offset);
            }

            return vec![page_data.clone()];
        }

        // dbg!(page_offset, self.page_size);
        // let page_data = self.file_access_service.read_in_file(file_offset, page_offset * self.page_size as u64);
        let read_count = page_offset * self.page_size as u64;
        // dbg!(db_offset, read_length);
        let page_data = self.database.pullx(db_offset, read_count);

        self.add_bulk_to_cache(page_offset, page_data.clone());

        page_data
    }

    fn get_obj_from_cache(&self, index: u64) -> T {
        if let Some(page_data) = self.cache.lock().unwrap().get(&index) {
            if self.should_update_lru(&index) {
                let mut lru_list = self.lru_list.lock().unwrap();
                self.remove_item(index); // lru_list.retain(|&x| x != page_offset);
                lru_list.push_back(index);
            }

            return page_data.clone();
        }

        let page_data = self.database.pull(index);

        self.add_to_cache(index, page_data.clone());
        page_data
    }
    
    fn get_objs_from_cache(&self, index: u64, count: u64) -> Vec<T> {

        let page_data = self.database.pullx(index, count);

        self.add_bulk_to_cache(index, page_data.clone());
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
    fn add_to_cache(&self, index: u64, data: T) {
        let mut cache = self.cache.lock().unwrap();
        let mut lru_list = self.lru_list.lock().unwrap();

        while cache.len() >= self.max_cache_items && !lru_list.is_empty() {
            if let Some(oldest_page) = lru_list.pop_front() {
                cache.remove(&oldest_page);
            }
        }

        cache.insert(index, data);
        lru_list.push_back(index);
    }

    fn add_bulk_to_cache1(&self, index: u64, datas: Vec<T>) {
        let mut cache = self.cache.lock().unwrap();
        let mut lru_list = self.lru_list.lock().unwrap();

        while cache.len() >= self.max_cache_items && !lru_list.is_empty() {
            if let Some(oldest_page) = lru_list.pop_front() {
                cache.remove(&oldest_page);
            }
        }
        // let end_index = index + datas.len() as u64 -1;
        let end_index = index + datas.len() as u64;
        // [index..end_index].collect()

        let mut cache_c = Arc::clone(&self.cache);
        let mut lru_list_c = Arc::clone(&self.lru_list);

        datas.par_iter().enumerate().for_each(|(i, data)| {
            let index = i as u64 + index;
            let cache_clone = Arc::clone(&cache_c);

            let lru_list_clone = Arc::clone(&lru_list_c);

            cache_clone.lock().unwrap().insert(index, data.clone());
            lru_list_clone.lock().unwrap().push_back(index);
        });
    }
    
    fn add_bulk_to_cache2(&self, index: u64, datas: Vec<T>) {
        /*
        let mut cache = self.cache.lock().unwrap();
        let mut lru_list = self.lru_list.lock().unwrap();
        let mut cache_clone  = Arc::clone(&self.cache);
        let mut lru_list_clone = Arc::clone(&self.lru_list);
let new map = datas.par_iter()
.enumerate()
.for_each((i, obj)| {
    let cache_index = i +index;
    let cache = cache_clone.lock().unwrap();
    let lru_list = lru_list_clone.lock().unwrap();
    cache.insert(cache_index, obj);
    lru_list.push_back(cache_index);
});
*/

        let mut cache = self.cache.lock().unwrap();
        let mut lru_list = self.lru_list.lock().unwrap();
    

        while cache.len() >= self.max_cache_items && !lru_list.is_empty() {
            if let Some(oldest_page) = lru_list.pop_front() {
                cache.remove(&oldest_page);
            }
        }
        // let end_index = index + datas.len() as u64 -1;
        let end_index = index + datas.len() as u64;
        // [index..end_index].collect()

        let mut cache_c = Arc::clone(&self.cache);
        let mut lru_list_c = Arc::clone(&self.lru_list);

        datas.par_iter().enumerate().for_each(|(i, data)| {
            let index = i as u64 + index;
            let cache_clone = Arc::clone(&cache_c);
            let lru_list_clone = Arc::clone(&lru_list_c);


            cache_clone.lock().unwrap().insert(index, data.clone());
            lru_list_clone.lock().unwrap().push_back(index);
        });
    }
    
    fn add_bulk_to_cache(&self, index: u64, objs: Vec<T>) {
        let mut cache_clone  = Arc::clone(&self.cache);
        let mut lru_list_clone = Arc::clone(&self.lru_list);
        let objs_len = objs.len();
        let max_cache_items = self.max_cache_items;
        std::thread::spawn(move || {
let (cache_hashmap, mut cache_linklist): (HashMap<u64, T>, LinkedList<u64>)  = objs.par_iter()
.enumerate()
.map(|(i, obj)| {
    let cache_index = i as u64+index;
    ((cache_index,obj.clone()), (cache_index))
})
.fold(
|| (HashMap::with_capacity(objs_len), LinkedList::new()),
|mut acc, (cache_hashmap, cache_linklist)|  {
    acc.0.insert(cache_hashmap.0, cache_hashmap.1);
    acc.1.push_back(cache_linklist);
    acc
})
.reduce(
|| (HashMap::with_capacity(objs_len), LinkedList::new()),
|(mut map1, mut link1), (map2, mut link2)| {
    map1.extend(map2.into_iter());
    link1.append(&mut link2);
    (map1, link1)
});

    // let cache_clone = Arc::clone(&self.cache);
    // let lru_list_clone  = Arc::clone(&self.lru_list);
// std::thread::spawn(move ||{
    let mut cache = cache_clone.lock().unwrap();
    cache.extend(cache_hashmap.into_iter());
    let mut lru_list = lru_list_clone.lock().unwrap();
    lru_list.append(&mut cache_linklist); 
    
    while cache.len() >= max_cache_items && !lru_list.is_empty() {
            if let Some(oldest_page) = lru_list.pop_front() {
                cache.remove(&oldest_page);
            }
        }
        
});


    }

    fn remove_item(&self, page: u64) {
        let mut lru_list = self.lru_list.lock().unwrap();

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
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        dynamic_vector_manage_service::DynamicVectorManageService,
        static_vector_manage_service::StaticVectorManageService,
    };
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

    #[test]
    fn test_pull_lot_static_from_cache() {
        let mut objs = Vec::new();
        let my_service = WritableCache::<
            StaticVectorManageService<StaticStruct>,
            StaticStruct,
        >::new(
            "cacheS.bin".to_string(), "cacheSD.bin".to_string(), 1024
        );
        let read_cache_service =
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

            objs.push(my_obj);
        }
        let start = Instant::now();
        my_service.extend(objs);
        let extend_cache_duration = start.elapsed();
        println!("extend cache duration: {:?}", extend_cache_duration);
        read_cache_service.pull_lot(0, COUNT as u64);
        let pull_lot_cache_duration = start.elapsed();
        println!(
            "pull lot cache duration: {:?}",
            pull_lot_cache_duration - extend_cache_duration
        );
    }
    
    #[test]
    fn test_get_lot_static_from_cache() {
        let mut objs = Vec::new();
        let my_service = WritableCache::<
            StaticVectorManageService<StaticStruct>,
            StaticStruct,
        >::new(
            "cacheS.bin".to_string(), "cacheSD.bin".to_string(), 1024
        );
        let read_cache_service =
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

            objs.push(my_obj);
        }
        let start = Instant::now();
        my_service.extend(objs);
        let extend_cache_duration = start.elapsed();
        println!("extend cache duration: {:?}", extend_cache_duration);
        read_cache_service.get_lot(0, COUNT as u64);
        let get_lot_cache_duration = start.elapsed();
        println!(
            "get lot cache duration: {:?}",
            get_lot_cache_duration - extend_cache_duration
        );
    }

    #[test]
    fn test_get_static_from_cache() {
        let mut objs = Vec::new();
        let my_service = WritableCache::<
            StaticVectorManageService<StaticStruct>,
            StaticStruct,
        >::new(
            "cacheS.bin".to_string(), "cacheSD.bin".to_string(), 1024
        );
        let read_cache_service =
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

            objs.push(my_obj);
        }
        let start = Instant::now();
        my_service.extend(objs);
        let extend_cache_duration = start.elapsed();
        println!("extend cache duration: {:?}", extend_cache_duration);
        for turn  in  0..3 {
            for i in 0..COUNT {
                let obj = read_cache_service.get(i as u64);
                // dbg!(obj);
            }
        }
        let get_from_cache_duration = start.elapsed();
        println!(
            "get from  cache duration: {:?}",
            get_from_cache_duration - extend_cache_duration
        );
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
