use crate::services::{
    dynamic_vector_manage_service::DynamicVectorManageService,
    static_vector_manage_service::StaticVectorManageService,
};
use serde::{
    Deserialize,
    Serialize,
};

pub trait VectorEngine<T>
where
    T: Serialize
        + for<'de> Deserialize<'de>
        + 'static
        + std::fmt::Debug
        + Clone
        + Send
        + Sync,
{
    fn new(
        structural_repository: String,
        dynamic_repository: String,
        initial_file_size: u64,
    ) -> Self;
    fn push(&self, obj: T);
    fn extend(&self, objs: Vec<T>);
    fn pull(&self, index: u64) -> T;
    fn pullx(&self, index: u64, count: u64) -> Vec<T>;
    fn len(&self) -> usize;
}

impl<T> VectorEngine<T> for DynamicVectorManageService<T>
where
    T: Serialize
        + for<'de> Deserialize<'de>
        + 'static
        + std::fmt::Debug
        + Clone
        + Send
        + Sync,
{
    fn new(
        structural_repository: String,
        dynamic_repository: String,
        initial_file_size: u64,
    ) -> Self {
        let dynamic_vector_manage_service = DynamicVectorManageService::<T>::new(
            structural_repository,
            dynamic_repository,
            initial_file_size,
        )
        .unwrap();
        dynamic_vector_manage_service
    }

    fn push(&self, obj: T) {
        self.save(obj);
    }
    fn extend(&self, objs: Vec<T>) {
        self.save_bulk(objs);
    }

    fn pull(&self, index: u64) -> T {
        self.load(index)
    }
    fn pullx(&self, index: u64, count: u64) -> Vec<T> {
        self.load_bulk(index, count)
    }
    fn len(&self) -> usize {
        self.get_length() as usize
    }
}

impl<T> VectorEngine<T> for StaticVectorManageService<T>
where
    T: Serialize
        + for<'de> Deserialize<'de>
        + 'static
        + std::fmt::Debug
        + Clone
        + Send
        + Sync,
{
    fn new(
        structural_repository: String,
        dynamic_repository: String,
        initial_file_size: u64,
    ) -> Self {
        let static_vector_manage_service = StaticVectorManageService::<T>::new(
            structural_repository,
            dynamic_repository,
            initial_file_size,
        )
        .unwrap();
        static_vector_manage_service
    }

    fn push(&self, obj: T) {
        self.add(obj);
    }
    fn extend(&self, objs: Vec<T>) {
        self.add_bulk(objs);
    }

    fn pull(&self, index: u64) -> T {
        self.read(index)
    }
    fn pullx(&self, index: u64, count: u64) -> Vec<T> {
        self.read_bulk(index, count)
    }
    fn len(&self) -> usize {
        self.get_length() as usize
    }
}
