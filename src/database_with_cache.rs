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
const MAX_CACHE_ITEMS: usize = 500;

pub struct WritableCache<D, T>
where
    D: VectorDatabase<T> +Sync +Send + 'static,
    T: Serialize
        + for<'de> Deserialize<'de>
        + 'static
        + std::fmt::Debug
        + Clone
        + Send
        + Sync,
{
    database: Arc<Mutex<D>>,                     // 底层数据库
    cache: Arc<Mutex<Vec<T>>>, 
    max_cache_items: usize,
}

impl<D, T> WritableCache<D, T>
where
    D: VectorDatabase<T> +Sync +Send + 'static,
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
            database: Arc::new(Mutex::new(VectorDatabase::new(
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
        // if cache.len() >= self.max_cache_items {
            if cache_len >= self.max_cache_items {
            let cache_clone =Arc::clone(&self.cache);
            let database_clone = Arc::clone(&self.database);
            std::thread::spawn(move || {
            // self.database.extend(cache.to_vec());
            // *cache = Vec::new();
            database_clone.lock().unwrap().extend(cache_clone.lock().unwrap().to_vec());
            *cache_clone.lock().unwrap() = Vec::new();
            });
        }
    }

    pub fn extend(&self, objs: Vec<T>) {
        let mut cache = self.cache.lock().unwrap();
        let mut objs = objs;
        cache.append(&mut objs);
        /*
        if cache.len() >= self.max_cache_items {
            self.database.extend(cache.to_vec());
            *cache = Vec::new();
        }

    */

    let cache_len = cache.len();
        if cache_len >= self.max_cache_items {
            let cache_clone =Arc::clone(&self.cache);
            let database_clone = Arc::clone(&self.database);
            std::thread::spawn(move || {
            database_clone.lock().unwrap().extend(cache_clone.lock().unwrap().to_vec());
            *cache_clone.lock().unwrap() = Vec::new();
            });
        }
    }
    pub fn get_base_len(&self) -> usize{
        self.cache.lock().unwrap().len()
    }
    
    pub fn get_cache_len(&self)  -> usize{
        self.cache.lock().unwrap().len()
    }
    pub fn len(&self) ->  usize {
        self.get_cache_len()  + self.get_base_len()
    }
    
    pub fn getting_obj_from_cache(&self, index: u64) -> T{
        self.cache.lock().unwrap()[index as usize].clone()
    }
    
    pub fn getting_objs_from_cache(&self, index: u64, count: u64) -> Vec<T>{
        let end_offset = (index   + count -1) as usize;
        self.cache.lock().unwrap()[index as usize..end_offset].into()
    }
    
    
    
    pub fn get_obj_from_cache(&self, index: u64) -> Option<T>{
        self.cache.lock().unwrap().get(index as usize).cloned()
    }

}

impl<D, T> Drop for WritableCache<D, T>
where
    D: VectorDatabase<T> +Sync +Send + 'static,
    T: Serialize
        + for<'de> Deserialize<'de>
        + 'static
        + std::fmt::Debug
        + Clone
        + Send
        + Sync,
{
    fn drop(&mut self) {
        let cache = self.cache.lock().unwrap();
        if cache.len() != 0 {
            self.database.lock().unwrap().extend(cache.to_vec());
        }
    }
}

pub struct ReadableCache<D, T>
where
    D: VectorDatabase<T> + 'static + Send,
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
    D: VectorDatabase<T> + 'static + Send,
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



    // pub fn getting1(&self, index: u64) -> T {
        // let data = self.get_obj_from_cache(index);

        // data
    // }
    

    
    // pub fn getting_lot(&self, index: u64, count : u64) -> Vec<T> {
        // let data = self.get_objs_from_cache(index, count );

        // data
    // }
    
    // fn get_obj_from_cache(&self, index: u64) -> T {
    pub fn getting(&self, index: u64) -> T {
        if let Some(page_data) = self.cache.lock().unwrap().get(&index) {
            // println!("checking lru list for index: {}", &index);
            self.check_lru_list(index);
            return page_data.clone();
        }

        let page_data = self.database.pull(index);

        self.add_to_cache(index, page_data.clone());
        page_data
    }
    
    // pub fn get_objs_from_cache(&self, index: u64, count: u64) -> Vec<T> {
    pub fn getting_lot(&self, index: u64, count: u64) -> Vec<T> {

        let page_data = self.database.pullx(index, count);

        self.add_bulk_to_cache(index, page_data.clone());
        page_data
    }





    pub fn add_to_cache(&self, index: u64, data: T) {
        let cache_clone = Arc::clone(&self.cache);
        let lru_list_clone = Arc::clone(&self.lru_list);
        let max_cache_items = self.max_cache_items;
        std::thread::spawn(move || {
        // let mut cache = cache_clone.lock().unwrap();
        let mut lru_list = lru_list_clone.lock().unwrap();

        // while cache.len() >= max_cache_items && !lru_list.is_empty() {
        while lru_list.len() >= max_cache_items && !lru_list.is_empty() {
            if let Some(oldest_page) = lru_list.pop_front() {
            cache_clone.lock().unwrap().remove(&oldest_page);
            }

        }

        cache_clone.lock().unwrap().insert(index, data);
        lru_list.push_back(index);
        });
    }


    

    
    pub fn add_bulk_to_cache(&self, index: u64, objs: Vec<T>) {
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
    // let mut cache = cache_clone.lock().unwrap();
    cache_clone.lock().unwrap().extend(cache_hashmap.into_iter());
    let mut lru_list = lru_list_clone.lock().unwrap();
    lru_list.append(&mut cache_linklist); 
    
    while lru_list.len() >= max_cache_items && !lru_list.is_empty() {
            if let Some(oldest_page) = lru_list.pop_front() {
                cache_clone.lock().unwrap().remove(&oldest_page);
            }
        }
        
});


    }


    

    
    fn check_lru_list(&self, index: u64) {
        let lru_list_clone = Arc::clone(&self.lru_list);
        let cache_clone = Arc::clone(&self.cache);
        std::thread::spawn(move  || {
        
        let mut lru_list = lru_list_clone.lock().unwrap();
                const VERY_RECENT_PAGE_ACCESS_LIMIT: usize = 0x10;

        let mut recent_access = lru_list.iter().rev().take(VERY_RECENT_PAGE_ACCESS_LIMIT);

        if !recent_access.any(|&x| x == index) { 
        let mut current = lru_list.front(); // 从列表的前端开始

        let mut index =0;
            let mut cursor = lru_list.cursor_front_mut();

        // while let Some(&value) = current {
            while let Some(value) = cursor.current() {
            if *value == index {
            cursor.remove_current();
            break; 
            // 更新 current 为下一个元素

            } else {
                cursor.move_next(); 
            }
        }
                lru_list.push_back(index);
        }
        });
    }
    
}

pub struct databaseWithCache<D, T>
where
    D: VectorDatabase<T> + 'static + Send +Sync,
    T: Serialize
        + for<'de> Deserialize<'de>
        + 'static
        + std::fmt::Debug
        + Clone
        + Send
        + Sync,
{
    writable_cache: WritableCache<D, T>,
    readable_cache: ReadableCache<D, T>,
    origin_database: D,
}

impl<D, T> databaseWithCache<D, T>
where
    D: VectorDatabase<T> + 'static + Send + Sync,
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
            writable_cache: WritableCache::new(
                static_repository.clone(),
                dynamic_repository.clone(),
                initial_size_if_not_exists.clone(),
            ),
            readable_cache: ReadableCache::new(
                static_repository.clone(),
                dynamic_repository.clone(),
                initial_size_if_not_exists.clone(),
            ),
            origin_database: VectorDatabase::new(
                static_repository,
                dynamic_repository,
                initial_size_if_not_exists,
            ),
        }
    }
    pub fn len(&self) -> usize{
        self.writable_cache.len()
    }  
    
    pub fn push(&self, obj:T) {
        self.writable_cache.push(obj);
    }
    
    pub fn extend(&self, objs: Vec<T> ) {
        self.extend(objs);
    }
    
    pub fn getting(&self, index: u64)  -> T{
        let total_count = self.len() as u64;
        let cache_count  = self.writable_cache.get_cache_len() as u64;
        let base_count = self.writable_cache.get_base_len() as u64; 
let obj = if index <= base_count {
    self.readable_cache.getting(index)
} else if index >base_count && index <= total_count {
    let obj = self.writable_cache.getting_obj_from_cache(index - base_count);
self.readable_cache.add_to_cache(index, obj.clone());
obj
} else {
    panic!("out of index!");
};

obj
    }
    
    pub fn get(&self, index: u64)  -> Option<T>{
        let total_count = self.len() as u64;
        let cache_count  = self.writable_cache.get_cache_len() as u64;
        let base_count = self.writable_cache.get_base_len() as u64; 
let obj = if index <= base_count {
    Some(self.readable_cache.getting(index))
} else if index >base_count && index <= total_count {
    Some(self.writable_cache.getting_obj_from_cache(index - base_count))
} else {
    None
};
if let Some(ref o) = obj { 
self.readable_cache.add_to_cache(index, o.clone());
}

obj
    }
    
    pub fn getting_lot(&self, index: u64, count: u64 )  -> Vec<T>{
        let total_count = self.len() as u64;
        let cache_count  = self.writable_cache.get_cache_len() as u64;
        let base_count = self.writable_cache.get_base_len() as u64;
        let end_offset = index + count;
let objs = if end_offset <= base_count {
    self.readable_cache.getting_lot(index, count)
} else if index >base_count && end_offset <= total_count {
    self.writable_cache.getting_objs_from_cache(index - base_count, end_offset - base_count)
    } else if index <= base_count && end_offset <= total_count {
        let mut front = self.readable_cache.getting_lot(index, base_count - index);
        let mut back =  self.writable_cache.getting_objs_from_cache(0 , end_offset - base_count);
        front.append(&mut back);
        front
        } else {
    panic!("out of index!");
};
self.readable_cache.add_bulk_to_cache(index, objs.clone());

objs
    }
    
    
}


#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        dynamic_vector_manage_service::DynamicVectorManageService,
        static_vector_manage_service::StaticVectorManageService,
    };
    const COUNT: usize = 1000;
    const TURNS: usize = 100;

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
    fn test_one_by_one_getting_static() {
        let my_service = ReadableCache::<
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
    fn test_one_by_one_getting_dynamic() {

        let my_service = ReadableCache::<
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

            my_service.getting(i as u64);
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
    fn test_getting_lot_static_from_cache() {
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
        for turn  in  0..TURNS {
            for i in 0..COUNT {
                let obj = read_cache_service.getting(i as u64);
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
