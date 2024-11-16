use serde::{Serialize, Deserialize};
use crate::dynamic_vector_manage_service::DynamicVectorManageService;
use  crate::object_persist_on_disk_service::ObjectPersistOnDiskService;

pub trait VectorDatabase<T> 
where
    T: Serialize + for<'de> Deserialize<'de> + Default + 'static + std::fmt::Debug + Clone + Send + Sync,
{
    fn new(structural_repository: String, dynamic_repository: String, initial_file_size: u64) -> Self;
    fn push(&self, obj: T);
    fn pushx(&self, objs: Vec<T>);
    fn pull(&self, index: u64) -> T;
    fn pullx(&self, index: u64, count: u64) -> Vec<T>;
}

impl<T> VectorDatabase<T>  for DynamicVectorManageService<T> 
where
T: Serialize + for<'de> Deserialize<'de> + Default + 'static + std::fmt::Debug + Clone + Send + Sync,
    {
    fn new(structural_repository: String, dynamic_repository: String, initial_file_size: u64) -> Self{
        let dynamic_vector_manage_service = DynamicVectorManageService::<T>::new(structural_repository, dynamic_repository, initial_file_size).unwrap();
        dynamic_vector_manage_service
    }
    
    fn push(&self, obj: T) {
    self.save(obj);
    }
    fn pushx(&self, objs: Vec<T>) {
    self.save_bulk(objs);
    }
    
    fn pull(&self, index: u64) -> T {
        self.load(index)
    }
    fn pullx(&self, index: u64, count: u64) -> Vec<T> {
        self.load_bulk(index, count)
    } 
    
}