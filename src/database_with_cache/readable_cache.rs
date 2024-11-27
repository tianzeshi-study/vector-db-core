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
    
    vector_engine::VectorEngine,
};

const PAGE_SIZE: u64 = 5000;
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
    cache: Arc<Mutex<HashMap<u64, T>>>,
    lru_list: Arc<Mutex<LinkedList<u64>>>,
    page_size: u64,
    max_cache_items: usize,
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
            cache: Arc::new(Mutex::new(HashMap::new())),
            lru_list: Arc::new(Mutex::new(LinkedList::new())),
            page_size: PAGE_SIZE,
            max_cache_items: MAX_CACHE_ITEMS,
        }
    }

    pub fn getting(&self, index: u64) -> T {
        if let Some(page_data) = self.cache.lock().unwrap().get(&index) {

            self.check_lru_list(index);
            return page_data.clone();
        }

        let page_data = self.database.pull(index);

        self.add_to_cache(index, page_data.clone());
        page_data
    }

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

            let mut lru_list = lru_list_clone.lock().unwrap();


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
        let mut cache_clone = Arc::clone(&self.cache);
        let mut lru_list_clone = Arc::clone(&self.lru_list);
        let objs_len = objs.len();
        let max_cache_items = self.max_cache_items;
        std::thread::spawn(move || {
            let (cache_hashmap, mut cache_linklist): (HashMap<u64, T>, LinkedList<u64>) =
                objs.par_iter()
                    .enumerate()
                    .map(|(i, obj)| {
                        let cache_index = i as u64 + index;
                        ((cache_index, obj.clone()), (cache_index))
                    })
                    .fold(
                        || (HashMap::with_capacity(objs_len), LinkedList::new()),
                        |mut acc, (cache_hashmap, cache_linklist)| {
                            acc.0.insert(cache_hashmap.0, cache_hashmap.1);
                            acc.1.push_back(cache_linklist);
                            acc
                        },
                    )
                    .reduce(
                        || (HashMap::with_capacity(objs_len), LinkedList::new()),
                        |(mut map1, mut link1), (map2, mut link2)| {
                            map1.extend(map2.into_iter());
                            link1.append(&mut link2);
                            (map1, link1)
                        },
                    );





            cache_clone
                .lock()
                .unwrap()
                .extend(cache_hashmap.into_iter());
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
        std::thread::spawn(move || {
            let mut lru_list = lru_list_clone.lock().unwrap();
            const VERY_RECENT_PAGE_ACCESS_LIMIT: usize = 0x10;

            let mut recent_access =
                lru_list.iter().rev().take(VERY_RECENT_PAGE_ACCESS_LIMIT);

            if !recent_access.any(|&x| x == index) {
                let mut current = lru_list.front(); 

                let mut index = 0;
                let mut cursor = lru_list.cursor_front_mut();

                
                while let Some(value) = cursor.current() {
                    if *value == index {
                        cursor.remove_current();
                        break;
                    
                    } else {
                        cursor.move_next();
                    }
                }
                lru_list.push_back(index);
            }
        });
    }

    pub fn get_length(&self) -> usize {
        self.database.len()
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

    fn extend(&self, objs: Vec<T>) {
        self.database.extend(objs);
    }

    fn pull(&self, index: u64) -> T {
        let obj = self.getting(index);

        obj
    }

    fn pullx(&self, index: u64, count: u64) -> Vec<T> {
        let objs = self.getting_lot(index, count);

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
            let my_obj: StaticStruct = StaticStruct {
                my_usize: 443 + i,
                my_u64: 53,
                my_u32: 4399,
                my_u16: 3306,
                my_u8: 22,
                my_boolean: true,
            };
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
            let my_obj: StaticStruct = StaticStruct {
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
    my_service.extend(objs);
    }

    #[test]
    fn test_one_by_one_getting_dynamic() {

        let my_service: DynamicVectorManageService<StaticStruct> =
            VectorEngine::<StaticStruct>::new(
                "cacheD2.bin".to_string(),
                "cacheDD2.bin".to_string(),
                1024,
            );
        let mut objs = Vec::with_capacity(COUNT);
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
            let mut objs = Vec::new();

            let my_service: StaticVectorManageService<StaticStruct> =
                VectorEngine::<StaticStruct>::new(
                    "cacheS3.bin".to_string(),
                    "cacheSD3.bin".to_string(),
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
            dbg!(my_service.len());
            let os = my_service.pullx(0, COUNT as u64);
            dbg!(&os[os.len() - 1]);
            let extend_cache_duration = start.elapsed();
            println!("extend cache duration: {:?}", extend_cache_duration);

        }
        let read_cache_service =
            ReadableCache::<StaticVectorManageService<StaticStruct>, StaticStruct>::new(
                "cacheS3.bin".to_string(),
                "cacheSD3.bin".to_string(),
                1024,
            );

        let awake = Instant::now();
        let objs = read_cache_service.getting_lot(0, COUNT as u64);

        let getting_lot_cache_duration = awake.elapsed();
        println!("get lot cache duration: {:?}", getting_lot_cache_duration);
        assert_eq!(442 + COUNT, objs[COUNT as usize - 1].my_usize);
        assert_eq!(COUNT as usize, objs.len());
        assert_eq!(COUNT as usize, read_cache_service.get_length());
    }

    #[test]
    fn test_getting_static_multi_turns() {
        let path = "cacheS4.bin".to_string();
        if std::path::Path::new(&path).exists() {
            std::fs::remove_file(&path).expect("Unable to remove file");
        }
        {
            let mut objs = Vec::new();

            let my_service: StaticVectorManageService<StaticStruct> =
                VectorEngine::<StaticStruct>::new(
                    "cacheS4.bin".to_string(),
                    "cacheSD4.bin".to_string(),
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
        }
        let read_cache_service =
            ReadableCache::<StaticVectorManageService<StaticStruct>, StaticStruct>::new(
                "cacheS4.bin".to_string(),
                "cacheSD4.bin".to_string(),
                1024,
            );
        let start = Instant::now();
        for turn in 0..TURNS {
            for i in 0..COUNT {
                let obj = read_cache_service.getting(i as u64);
                assert_eq!(443 + i, obj.my_usize);
            }
        }
        let get_from_cache_duration = start.elapsed();
        assert_eq!(COUNT, read_cache_service.get_length());
        println!("get from  cache duration: {:?}", get_from_cache_duration);
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

            objs.push(my_obj);



        }
        my_service.add_bulk(objs);
    }
}
