use rayon::prelude::*;
pub use serde::{
    Deserialize,
    Serialize,
};
use std::{
    io::{
        self,
    },
    marker::PhantomData,
    mem::size_of,
    sync::{
        Arc,
        Mutex,
    },
};

pub use dynamic_vector::VectorCandidate;

use crate::services::file_access_service::FileAccessService;

const LENGTH_MARKER_SIZE: usize = size_of::<u64>();

pub struct StaticVectorManageService<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Send,
{
    length: Arc<Mutex<u64>>,
    structure_file: Mutex<FileAccessService>,

    _marker: PhantomData<T>,
}

impl<T: Send + Sync> StaticVectorManageService<T>
where
    T: Serialize + for<'de> Deserialize<'de> + 'static + std::fmt::Debug,
{
    pub fn new(
        structure_file_path: String,
        _string_file_path: String,
        initial_size_if_not_exists: u64,
    ) -> io::Result<Self> {
        let structure_file_access =
            FileAccessService::new(structure_file_path, initial_size_if_not_exists);

        let length = {
            let buffer = structure_file_access.read_in_file(0, LENGTH_MARKER_SIZE);

            assert!(buffer.len() >= 4, "Buffer length must be at least 4 bytes.");

            let length = u64::from_le_bytes(buffer[0..8].try_into().unwrap());

            Arc::new(Mutex::new(length))
        };
        Ok(Self {
            length,
            structure_file: Mutex::new(structure_file_access),

            _marker: PhantomData,
        })
    }

    pub fn get_length(&self) -> u64 {
        let structure_file_guard = self.structure_file.lock().unwrap();
        let buffer = structure_file_guard.read_in_file(0, LENGTH_MARKER_SIZE);

        assert!(buffer.len() >= 4, "Buffer length must be at least 4 bytes.");

        

        u64::from_le_bytes(buffer[0..8].try_into().unwrap())
    }

    fn save_length(&self, length: u64) {
        let buffer: [u8; std::mem::size_of::<u64>()] = length.to_ne_bytes();
        let file_guard = self.structure_file.lock().unwrap();
        file_guard.write_in_file(0, &buffer);
    }

    fn serialize_object(obj: &T) -> Vec<u8> {
        bincode::serialize(obj).expect("Serialization failed")
    }

    fn deserialize_object(data: &[u8]) -> T {
        bincode::deserialize(data).expect("Deserialization failed")
    }

    fn write_index(&self, index: u64, obj: T) {
        let size_of_object = size_of::<T>();
        let data = Self::serialize_object(&obj);

        println!("bytes length of data to write once: {} ", &data.len());

        let offset =
            (size_of_object * index as usize + LENGTH_MARKER_SIZE) as u64;

        let file = self.structure_file.lock().unwrap();
        file.write_in_file(offset, &data);
    }

    fn bulk_write_index(&self, index: u64, objs: Vec<T>) {
        let size_of_object = size_of::<T>();
        let count = objs.len();
        let mut buffer: Vec<u8> = vec![0; size_of_object * count];

        let serialized_objs: Vec<Vec<u8>> = objs
            .par_iter()
            .map(|obj| Self::serialize_object(obj))
            .collect::<Vec<Vec<u8>>>();

        let mut current_position = 0;
        for serialized_obj in &serialized_objs {
            let serialized_size = serialized_obj.len();
            let white_space = size_of_object - serialized_size;

            buffer[current_position..current_position + serialized_size]
                .copy_from_slice(serialized_obj);
            current_position += white_space;

            current_position += serialized_size;
        }

        let offset =
            (size_of_object * index as usize + LENGTH_MARKER_SIZE) as u64;

        let file_guard = self.structure_file.lock().unwrap();

        file_guard.write_in_file(offset, &buffer);
    }

    

    pub fn add(&self, obj: T) {
        let index_to_write = {
            let mut length = self.length.lock().unwrap();
            let index = *length;
            *length += 1;
            self.save_length(*length);
            index
        };
        self.write_index(index_to_write, obj);
    }

    pub fn add_bulk(&self, objs: Vec<T>) {
        let index_to_write = {
            let mut length = self.length.lock().unwrap();
            let count = objs.len();
            let index = *length;
            *length += count as u64;

            self.save_length(*length);
            index
        };

        self.bulk_write_index(index_to_write, objs);
    }



    pub fn read(&self, index: u64) -> T {
        let size_of_object = size_of::<T>();
        let offset = (size_of_object * index as usize + LENGTH_MARKER_SIZE) as u64;

        let length = size_of_object;

        let file_guard = self.structure_file.lock().unwrap();
        let data: Vec<u8> = file_guard.read_in_file(offset, length);

        let obj: T = bincode::deserialize(&data).expect("Deserialization failed");

        obj
    }

    pub fn read_bulk(&self, index: u64, count: u64) -> Vec<T> {
        let size_of_object = size_of::<T>();
        let offset = (size_of_object * index as usize + LENGTH_MARKER_SIZE) as u64;

        let length = count as usize * size_of_object;

        let file_guard = self.structure_file.lock().unwrap();
        let data: Vec<u8> = file_guard.read_in_file(offset, length);

        let objs: Vec<T> = data
            .par_chunks(size_of_object)
            .map(|data| Self::deserialize_object(data))
            .collect();

        objs
    }
}

#[cfg(test)]
mod test {
    use super::*;
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
    fn test_static_one() {
        let my_obj: StaticStruct = StaticStruct {
            my_usize: 443,
            my_u64: 53,
            my_u32: 4399,
            my_u16: 3306,
            my_u8: 22,
            my_boolean: true,
        };
        let my_service = StaticVectorManageService::<StaticStruct>::new(
            "TestStaticData1.bin".to_string(),
            "TestStaticDataDynamic1.bin".to_string(),
            1024,
        )
        .unwrap();
        my_service.add(my_obj);
        my_service.read(0);
    }

    #[test]
    fn test_static_bulk() {
        let mut objs = Vec::new();
        let my_service = StaticVectorManageService::<StaticStruct>::new(
            "TestStaticData.bin".to_string(),
            "TestStaticDataDynamic.bin".to_string(),
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

            objs.push(my_obj.clone());
        }
        my_service.add_bulk(objs);
        my_service.read_bulk(0, COUNT as u64);
    }
}
