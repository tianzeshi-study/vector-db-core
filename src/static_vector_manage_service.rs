use rayon::prelude::*;
pub use serde::{
    Deserialize,
    Serialize,
};
use serde_json::Value;
use std::{
    io::{
        self,
    },
    marker::PhantomData,
    mem::size_of,
    sync::{
        Arc,
        Mutex,
        RwLock,
    },
    time::Instant,
};

// pub use dynamic_vector::CheckDynamicSize;
// pub use dynamic_vector::DynamicVector;
pub use dynamic_vector::VectorCandidate;

use crate::{
    file_access_service::FileAccessService,
    string_repository::StringRepository,
};

// const LENGTH_MARKER_SIZE: usize = size_of::<u64>(); // We reserve the first 4 bytes for Length
const LENGTH_MARKER_SIZE: usize = size_of::<u64>(); // We reserve the first 8 bytes for Length

/// 提供了将对象序列化和反序列化到磁盘的服务
///
/// - `T`: 要序列化的对象类型，必须实现 `Serialize` 和 `DeserializeOwned` 特征
pub struct StaticVectorManageService<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Send,
{
    length: Arc<Mutex<u64>>,
    structure_file: Mutex<FileAccessService>, // 结构文件的文件句柄
    // string_repository: StringRepository,
    // dynamic_repository_dir: String,
    initial_size_if_not_exists: u64,
    _marker: PhantomData<T>, // 用于存储对象类型的占位符
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
        // let _ = std::fs::create_dir_all(&string_file_path);
        // let string_repository = StringRepository::new(string_file_path.clone(), initial_size_if_not_exists);
        // let length = get_length();
        let length = {
            let buffer = structure_file_access.read_in_file(0, LENGTH_MARKER_SIZE);
            // 确保 buffer 的长度至少为 4
            assert!(buffer.len() >= 4, "Buffer length must be at least 4 bytes.");

            // 将前       4 个字节转换为 u64，假设使用小端字节序
            let length = u64::from_le_bytes(buffer[0..8].try_into().unwrap());

            Arc::new(Mutex::new(length))
        };
        Ok(Self {
            length,
            structure_file: Mutex::new(structure_file_access),
            // string_repository: string_repository,
            // dynamic_repository_dir: _string_file_path,
            initial_size_if_not_exists,
            _marker: PhantomData,
        })
    }

    pub fn get_length(&self) -> u64 {
        let structure_file_guard = self.structure_file.lock().unwrap();
        let buffer = structure_file_guard.read_in_file(0, LENGTH_MARKER_SIZE);
        // 确保 buffer 的长度至少为 4
        assert!(buffer.len() >= 4, "Buffer length must be at least 4 bytes.");

        // 将前 4 个字节转换为 u64，假设使用小端字节序
        // let length = u64::from_le_bytes(buffer[0..4].try_into().unwrap());
        let length = u64::from_le_bytes(buffer[0..8].try_into().unwrap());

        // *self.length.lock().unwrap()
        length
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
        // println!("length of data  to deserialize {:?}", data.len());
        // bincode::deserialize(data).expect("Deserialization failed")
        // bincode::deserialize(data).unwrap_or_else(|e| {
        // eprintln!("Deserialization failed: {}", e);
        // panic!("Deserialization error")
        // })
        bincode::deserialize(data).expect("Deserialization failed")
    }

    fn write_index(&self, index: u64, obj: T) {
        let size_of_object = size_of::<T>();
        let data = Self::serialize_object(&obj);

        println!("bytes length of data to write once: {} ", &data.len());

        let offset =
            (size_of_object * index as usize + LENGTH_MARKER_SIZE as usize) as u64;

        let file = self.structure_file.lock().unwrap();
        file.write_in_file(offset, &data);
    }

    fn bulk_write_index(&self, index: u64, objs: Vec<T>) {
        let size_of_object = size_of::<T>();
        let count = objs.len();
        let mut buffer: Vec<u8> = vec![0; size_of_object * count];
        // println!("length of buffer: {}", buffer.len());

        // let data: Vec<u8> = objs.par_iter()
        // .map(|obj| Self::serialize_object(obj))
        // .flatten()
        // .collect::<Vec<u8>>();

        let serialized_objs: Vec<Vec<u8>> = objs
            .par_iter()
            .map(|obj| Self::serialize_object(obj))
            .collect::<Vec<Vec<u8>>>();

        let mut current_position = 0;
        for serialized_obj in &serialized_objs {
            // for obj in &objs {
            // let serialized_obj = Self::serialize_object(&obj);
            let serialized_size = serialized_obj.len();
            let white_space = size_of_object - serialized_size;
            // println!("white_space: {}", white_space);
            // 将序列化对象写入缓冲区
            buffer[current_position..current_position + serialized_size]
                .copy_from_slice(&serialized_obj);
            current_position += white_space;

            current_position += serialized_size;
        }

        let vec_data: Vec<Vec<u8>> = objs
            .par_iter()
            .map(|obj| Self::serialize_object(obj))
            .collect::<Vec<Vec<u8>>>();
        // println!("bulk write size_of_object:{}", size_of_object);
        // println!("length of  vec_data:{}", vec_data.len());
        // println!("length of  item of vec_data:{}", vec_data[0].len());
        // println!("bytes of vec_data:{}", vec_data[0].len() * vec_data.len());

        // .flat_map(|vec| vec)
        // let offset = (4 + (index as usize * data.len())) as u64;
        let offset =
            (size_of_object * index as usize + LENGTH_MARKER_SIZE as usize) as u64;
        // println!("bulk write offset: {}", offset);
        // println!("bulk write data length:{}", data.len());

        let file_guard = self.structure_file.lock().unwrap();
        // file.seek(SeekFrom::Start(offset)).expect("Seek failed.");
        // file.write_all(&data).expect("Write failed.");
        // file_guard.write_in_file(offset, &data);
        file_guard.write_in_file(offset, &buffer);
    }

    fn bulk_write_index_control(&self, index: u64, objs: Vec<T>) -> io::Result<()> {
        let size_of_object = size_of::<T>();
        let count = objs.len();
        let mut buffer: Vec<u8> = vec![0; size_of_object * count];
        println!("length of buffer: {}", buffer.len());

        // let data: Vec<u8> = objs.par_iter()
        // .map(|obj| Self::serialize_object(obj))
        // .flatten()
        // .collect::<Vec<u8>>();

        let serialized_objs: Vec<Vec<u8>> = objs
            .par_iter()
            .map(|obj| Self::serialize_object(obj))
            .collect::<Vec<Vec<u8>>>();

        let mut current_position = 0;
        for serialized_obj in &serialized_objs {
            // for obj in &objs {
            // let serialized_obj = Self::serialize_object(&obj);
            let serialized_size = serialized_obj.len();
            let white_space = size_of_object - serialized_size;
            // println!("white_space: {}", white_space);
            // 将序列化对象写入缓冲区
            buffer[current_position..current_position + serialized_size]
                .copy_from_slice(&serialized_obj);
            current_position += white_space;

            current_position += serialized_size;
        }

        let vec_data: Vec<Vec<u8>> = objs
            .par_iter()
            .map(|obj| Self::serialize_object(obj))
            .collect::<Vec<Vec<u8>>>();
        println!("bulk write size_of_object:{}", size_of_object);
        println!("length of  vec_data:{}", vec_data.len());
        println!("length of  item of vec_data:{}", vec_data[0].len());
        println!("bytes of vec_data:{}", vec_data[0].len() * vec_data.len());

        // .flat_map(|vec| vec)
        // let offset = (4 + (index as usize * data.len())) as u64;
        let offset =
            (size_of_object * index as usize + LENGTH_MARKER_SIZE as usize) as u64;
        println!("bulk write offset: {}", offset);
        // println!("bulk write data length:{}", data.len());

        let file_guard = self.structure_file.lock().unwrap();
        // file.seek(SeekFrom::Start(offset)).expect("Seek failed.");
        // file.write_all(&data).expect("Write failed.");
        // file_guard.write_in_file(offset, &data);
        file_guard.write_in_file_control(offset, &buffer).unwrap();
        Ok(())
    }

    pub fn add(&self, obj: T) {
        let index_to_write = {
            let mut length = self.length.lock().unwrap();
            let index = *length;
            *length += 1;
            self.save_length(*length);
            index
        };
        self.write_index(index_to_write.into(), obj);
    }

    pub fn add_bulk(&self, objs: Vec<T>) {
        let index_to_write = {
            let mut length = self.length.lock().unwrap();
            let count = objs.len();
            let index = *length;
            *length += count as u64;
            // *length += 1;
            self.save_length(*length);
            index
        };
        // println!("add bulk index_to_write:{}", index_to_write);
        self.bulk_write_index(index_to_write.into(), objs);
    }

    pub fn add_bulk_control(&self, objs: Vec<T>) -> io::Result<()> {
        let index_to_write = {
            let mut length = self.length.lock().unwrap();
            let count = objs.len();
            let index = *length;
            *length += count as u64;
            // *length += 1;
            self.save_length(*length);
            index
        };
        println!("add bulk index_to_write:{}", index_to_write);
        self.bulk_write_index_control(index_to_write.into(), objs);
        Ok(())
    }

    pub fn read(&self, index: u64) -> T {
        let size_of_object = size_of::<T>();
        let offset = (size_of_object * index as usize + LENGTH_MARKER_SIZE) as u64;

        // println!("read offset:{}", offset);

        let length = size_of_object;

        let file_guard = self.structure_file.lock().unwrap();
        let data: Vec<u8> = file_guard.read_in_file(offset, length);
        // dbg!(&data.len());
        // let obj = Self::deserialize_object(&data);
        let obj: T = bincode::deserialize(&data).expect("Deserialization failed");
        // println!("{:?}", obj);

        // return obj;

        obj
    }

    pub fn read_bulk(&self, index: u64, count: u64) -> Vec<T> {
        let size_of_object = size_of::<T>();
        let offset = (size_of_object * index as usize + LENGTH_MARKER_SIZE) as u64;
        // println!("read offset:{}", offset);
        let length = count as usize * size_of_object;
        // println!("read length:{}", length);

        let file_guard = self.structure_file.lock().unwrap();
        let data: Vec<u8> = file_guard.read_in_file(offset, length);
        // let objs: Vec<T> = data.chunks(size_of_object)
        // .map(|data| Self::deserialize_object(data)) // one thread  deserialize
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
    fn test_add_static_one() {
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
        let my_service = StaticVectorManageService::<StaticStruct>::new(
            "TestDynamicData.bin".to_string(),
            "TestDynamicDataDynamic.bin".to_string(),
            1024,
        )
        .unwrap();
        my_service.add(my_obj);
    }

    #[test]
    fn test_add_static_bulk() {
        let mut objs = Vec::new();
        let my_service = StaticVectorManageService::<StaticStruct>::new(
            "TestDynamicData.bin".to_string(),
            "TestDynamicDataDynamic.bin".to_string(),
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

            objs.push(my_obj.clone());
        }
        my_service.add_bulk(objs);
    }

    #[test]
    fn test_read_static_one() {
        let my_service = StaticVectorManageService::<StaticStruct>::new(
            "TestDynamicData.bin".to_string(),
            "TestDynamicDataDynamic.bin".to_string(),
            1024,
        )
        .unwrap();
        my_service.read(COUNT as u64);
    }

    #[test]
    fn test_read_static_bulk() {
        let my_service = StaticVectorManageService::<StaticStruct>::new(
            "TestDynamicData.bin".to_string(),
            "TestDynamicDataDynamic.bin".to_string(),
            1024,
        )
        .unwrap();
        my_service.read_bulk(0, COUNT as u64);
    }
}
