//  Deprecated DatabaseWithCache, multiservice   is on going 
use serde::{
    Deserialize,
    Serialize,
};

use crate::vector_engine::VectorEngine;

pub mod writable_cache;
pub use self::writable_cache::WritableCache;

pub mod readable_cache;
pub use self::readable_cache::ReadableCache;

pub struct DatabaseWithCache<D, T>
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
    writable_cache: WritableCache<D, T>,
    readable_cache: ReadableCache<D, T>,
}

impl<D, T> DatabaseWithCache<D, T>
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
        }
    }
    pub fn len(&self) -> usize {
        self.writable_cache.len()
    }

    pub fn push(&self, obj: T) {
        self.writable_cache.push(obj);
    }

    pub fn extend(&self, objs: Vec<T>) {
        self.writable_cache.extend(objs);
    }

    pub fn getting(&self, index: u64) -> T {
        let total_count = self.len() as u64;

        let base_count = self.writable_cache.get_base_len() as u64;
        let current_count = index + 1;

        let obj = if current_count <= base_count {
            self.readable_cache.getting(index)
        } else if current_count > base_count && index <= total_count {
            let obj = self
                .writable_cache
                .getting_obj_from_cache(index - base_count);
            self.readable_cache.add_to_cache(index, obj.clone());

            obj
        } else {
            panic!(
                "index out of bounds: the len is {} but the index is {} !",
                total_count, index
            );
        };

        obj
    }

    pub fn get(&self, index: u64) -> Option<T> {
        let total_count = self.len() as u64;

        let base_count = self.writable_cache.get_base_len() as u64;
        let obj = if index <= base_count {
            Some(self.readable_cache.getting(index))
        } else if index > base_count && index <= total_count {
            let o = self
                .writable_cache
                .getting_obj_from_cache(index - base_count);
            self.readable_cache.add_to_cache(index, o.clone());
            Some(o)
        } else {
            None
        };

        obj
    }

    pub fn getting_lot1(&self, index: u64, count: u64) -> Vec<T> {
        let total_count = self.len() as u64;
        let cache_count = self.writable_cache.get_cache_len() as u64;
        let base_count = self.writable_cache.get_base_len() as u64;
        let end_offset = index + count;
        dbg!(&total_count, &cache_count, &base_count, &end_offset, &index);
        let objs = if end_offset <= base_count {
            println!("reading readable_cache");
            self.readable_cache.getting_lot(index, count)
        } else if index >= base_count && end_offset <= total_count {
            println!("reading Writable cache");
            let objs = self
                .writable_cache
                .getting_objs_from_cache(index - base_count, end_offset - index);

            self.readable_cache.add_bulk_to_cache(index, objs.clone());
            objs
        } else if index < base_count && end_offset <= total_count {
            println!("reading readable cache and Writable Cache ");
            let mut front = self.readable_cache.getting_lot(index, base_count - index);
            let mut back = self
                .writable_cache
                .getting_objs_from_cache(0, end_offset - base_count);
            self.readable_cache
                .add_bulk_to_cache(base_count, back.clone());

            front.append(&mut back);
            front
        } else {
            panic!(
                "end offset  out of bounds: the total count  is {} but the en offset   is {} !",
                total_count, end_offset
            );
        };

        objs
    }

    pub fn getting_lot2(&self, index: u64, count: u64) -> Vec<T> {
        let total_count = self.len() as u64;
        let cache_count = self.writable_cache.get_cache_len() as u64;
        let base_count = self.writable_cache.get_base_len() as u64;
        let end_offset = index + count;
        dbg!(&total_count, &cache_count, &base_count, &end_offset, &index);
        let objs = if end_offset <= base_count {
            println!("reading readable_cache");
            self.readable_cache.getting_lot(index, count)
        } else if index >= base_count && end_offset <= total_count {
            println!("reading Writable cache");
            let objs = self
                .writable_cache
                .getting_objs_from_cache(index - base_count, end_offset - index);

            self.readable_cache.add_bulk_to_cache(index, objs.clone());
            objs
        } else if index < base_count && end_offset <= total_count {
            println!("reading readable cache and Writable Cache ");
            let mut front = self.readable_cache.getting_lot(index, base_count - index);
            let mut back = self
                .writable_cache
                .getting_objs_from_cache(0, end_offset - base_count);
            self.readable_cache
                .add_bulk_to_cache(base_count, back.clone());

            front.append(&mut back);
            front
        } else {
            panic!(
                "end offset  out of bounds: the total count  is {} but the en offset   is {} !",
                total_count, end_offset
            );
        };

        objs
    }

    pub fn getting_lot(&self, index: u64, count: u64) -> Vec<T> {
        let (cache_count, base_count, total_count): (u64, u64, u64) =
            self.writable_cache.get_each_len();
        let end_offset = index + count;
        dbg!(&total_count, &cache_count, &base_count, &end_offset, &index);

        let objs = if end_offset <= self.writable_cache.get_base_len() as u64
            && self.writable_cache.get_cache_len() as u64 == total_count - base_count
        {
            println!("reading readable_cache");
            self.readable_cache.getting_lot(index, count)
        } else if index >= self.writable_cache.get_base_len() as u64
            && end_offset <= total_count
        {
            println!("reading Writable cache");
            let objs = if let Some(objs) = self
                .writable_cache
                .get_objs_from_cache(index - base_count, end_offset - index)
            {
                println!("got from cache successful!");
                objs
            } else {
                println!(" got from cache failed !");
                self.readable_cache.getting_lot(index, count)
            };

            self.readable_cache.add_bulk_to_cache(index, objs.clone());
            objs
        } else if index < self.writable_cache.get_base_len() as u64
            && end_offset <= total_count
        {
            println!("reading readable cache and Writable Cache ");
            let objs = if let Some(mut back) = self
                .writable_cache
                .get_objs_from_cache(0, end_offset - base_count)
            {
                let mut front =
                    self.readable_cache.getting_lot(index, base_count - index);

                self.readable_cache
                    .add_bulk_to_cache(base_count, back.clone());
                front.append(&mut back);
                front
            } else {
                let objs = self.readable_cache.getting_lot(index, count);
                self.readable_cache.add_bulk_to_cache(index, objs.clone());
                objs
            };
            objs
        } else {
            panic!(
                "end offset  out of bounds: the total count  is {} but the en offset   is {} !",
                total_count, end_offset
            );
        };

        objs
    }

    pub fn get_lot(&self, index: u64, count: u64) -> Option<Vec<T>> {
        let total_count = self.len() as u64;

        let base_count = self.writable_cache.get_base_len() as u64;
        let end_offset = index + count;
        dbg!(index, base_count, end_offset);
        let objs = if end_offset <= base_count {
            println!("reading readable_cache");
            Some(self.readable_cache.getting_lot(index, count))
        } else if index >= base_count && end_offset <= total_count {
            println!("reading Writable Cache ");
            let objs = self
                .writable_cache
                .getting_objs_from_cache(index - base_count, end_offset - base_count);
            self.readable_cache.add_bulk_to_cache(index, objs.clone());
            Some(objs)
        } else if index < base_count && end_offset <= total_count {
            println!("reading readable cache and Writable Cache ");
            let mut front = self.readable_cache.getting_lot(index, base_count - index);
            let mut back = self
                .writable_cache
                .getting_objs_from_cache(0, end_offset - base_count);
            self.readable_cache
                .add_bulk_to_cache(base_count, back.clone());

            front.append(&mut back);
            Some(front)
        } else {
            None
        };

        objs
    }
}

impl<D, T> VectorEngine<T> for DatabaseWithCache<D, T>
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
        }
    }
    fn len(&self) -> usize {
        self.writable_cache.len()
    }

    fn push(&self, obj: T) {
        self.writable_cache.push(obj);
    }

    fn extend(&self, objs: Vec<T>) {
        self.extend(objs);
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
mod tests {
    use super::*;
    use crate::services::static_vector_manage_service::StaticVectorManageService;

    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct TestData {
        value: i32,
    }

    fn remove_file(path: &str) {
        if std::path::Path::new(&path).exists() {
            std::fs::remove_file(&path).expect("Unable to remove file");
        }
    }
    #[test]
    fn test_new() {
        remove_file("static_repo.bin");
        let db = DatabaseWithCache::<StaticVectorManageService<TestData>, TestData>::new(
            "static_repo.bin".to_string(),
            "dynamic_repo.bin".to_string(),
            100,
        );

        assert_eq!(db.len(), 0);
    }

    #[test]
    fn test_push_and_len() {
        remove_file("static_repo1.bin");
        let db = DatabaseWithCache::<StaticVectorManageService<TestData>, TestData>::new(
            "static_repo1.bin".to_string(),
            "dynamic_repo1.bin".to_string(),
            100,
        );

        let item = TestData { value: 42 };
        db.push(item.clone());

        assert_eq!(db.len(), 1);
    }

    #[test]
    fn test_getting_one() {
        remove_file("static_repo2.bin");
        let db = DatabaseWithCache::<StaticVectorManageService<TestData>, TestData>::new(
            "static_repo2.bin".to_string(),
            "dynamic_repo2.bin".to_string(),
            100,
        );

        let item = TestData { value: 42 };
        db.push(item.clone());

        let retrieved = db.getting(0);
        assert_eq!(retrieved.value, 42);
        assert_eq!(db.len(), 1);
    }

    #[test]
    fn test_get_lot() {
        remove_file("static_repo3.bin");
        let db = DatabaseWithCache::<StaticVectorManageService<TestData>, TestData>::new(
            "static_repo3.bin".to_string(),
            "dynamic_repo3.bin".to_string(),
            100,
        );

        let item1 = TestData { value: 42 };
        let item2 = TestData { value: 84 };

        db.push(item1.clone());
        db.push(item2.clone());
        let objs = db.getting_lot(0, 2);
        assert_eq!(objs.len(), 2);
        let items = db.get_lot(0, 2).unwrap();
        dbg!(&items);
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].value, 42);
        assert_eq!(items[1].value, 84);
    }

    #[test]
    fn test_extend() {
        remove_file("static_repo4.bin");
        remove_file("dynamic_repo4.bin");
        let db = DatabaseWithCache::<StaticVectorManageService<TestData>, TestData>::new(
            "static_repo4.bin".to_string(),
            "dynamic_repo4.bin".to_string(),
            100,
        );
        let mut objs = Vec::new();
        let item1 = TestData { value: 42 };
        let item2 = TestData { value: 84 };
        objs.push(item1);
        objs.push(item2);
        db.extend(objs);

        let objs = db.getting_lot(0, 2);
        assert_eq!(objs.len(), 2);
        let items = db.get_lot(0, 2).unwrap();
        dbg!(&items);
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].value, 42);
        assert_eq!(items[1].value, 84);
        assert_eq!(db.len(), 2);
    }

    #[test]
    fn test_getting_lot() {
        remove_file("static_repo5.bin");
        remove_file("dynamic_repo5.bin");
        let db = DatabaseWithCache::<StaticVectorManageService<TestData>, TestData>::new(
            "static_repo5.bin".to_string(),
            "dynamic_repo5.bin".to_string(),
            100,
        );

        let item1 = TestData { value: 42 };
        let item2 = TestData { value: 84 };

        db.push(item1.clone());
        db.push(item2.clone());
        let objs = db.getting_lot(0, 2);
        assert_eq!(objs.len(), 2);
        let items = db.get_lot(0, 2).unwrap();
        dbg!(&items);
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].value, 42);
        assert_eq!(items[1].value, 84);
    }
}
