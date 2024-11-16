use rayon::prelude::*;
pub use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{self};
use std::marker::PhantomData;
use std::mem::size_of;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

pub use dynamic_vector::CheckDynamicSize;
pub use dynamic_vector::DynamicVector;
pub use dynamic_vector::VectorCandidate;

use crate::file_access_service::FileAccessService;
use crate::string_repository::StringRepository;

// const LENGTH_MARKER_SIZE: usize = size_of::<u64>(); // We reserve the first 4 bytes for Length
const LENGTH_MARKER_SIZE: usize = size_of::<u64>(); // We reserve the first 8 bytes for Length

/// 提供了将对象序列化和反序列化到磁盘的服务
///
/// - `T`: 要序列化的对象类型，必须实现 `Serialize` 和 `DeserializeOwned` 特征
pub struct ObjectPersistOnDiskService<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Default + Send,
{
    length: Arc<Mutex<u64>>,
    structure_file: Mutex<FileAccessService>, // 结构文件的文件句柄
    // string_repository: StringRepository,
    dynamic_repository_dir: String,
    initial_size_if_not_exists: u64,
    _marker: PhantomData<T>, // 用于存储对象类型的占位符
}

impl<T: Send + Sync> ObjectPersistOnDiskService<T>
where
    // T: Serialize + for<'de> Deserialize<'de> + Default,
    T: Serialize + for<'de> Deserialize<'de> + Default + 'static + DynamicVector + std::fmt::Debug,
{
    pub fn new(
        structure_file_path: String,
        string_file_path: String,
        initial_size_if_not_exists: u64,
    ) -> io::Result<Self> {
        let structure_file_access =
            FileAccessService::new(structure_file_path, initial_size_if_not_exists);
        let _ = std::fs::create_dir_all(&string_file_path);
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
            dynamic_repository_dir: string_file_path,
            initial_size_if_not_exists: initial_size_if_not_exists,
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
            let size_of_object = get_item_size::<T>();
            let data = Self::serialize_object(&obj);

            println!("bytes length of data to write once: {} ", &data.len());

            let offset = (size_of_object * index as usize + LENGTH_MARKER_SIZE as usize) as u64;


            let file = self.structure_file.lock().unwrap();
            file.write_in_file(offset, &data);

    }

    fn bulk_write_index(&self, index: u64, objs: Vec<T>) {

            let size_of_object = get_item_size::<T>();
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
                println!("white_space: {}", white_space);
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
            let offset = (size_of_object * index as usize + LENGTH_MARKER_SIZE as usize) as u64;
            println!("bulk write offset: {}", offset);
            // println!("bulk write data length:{}", data.len());

            let file_guard = self.structure_file.lock().unwrap();
            // file.seek(SeekFrom::Start(offset)).expect("Seek failed.");
            // file.write_all(&data).expect("Write failed.");
            // file_guard.write_in_file(offset, &data);
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
        println!("add bulk index_to_write:{}", index_to_write);
        self.bulk_write_index(index_to_write.into(), objs);
    }

    pub fn read(&self, index: usize) -> T {
        let size_of_object = get_item_size::<T>();
        let offset = (size_of_object * index as usize + LENGTH_MARKER_SIZE) as u64;

        println!("read offset:{}", offset);

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

    pub fn read_bulk(&self, index: usize, count: usize) -> Vec<T> {
        let size_of_object = get_item_size::<T>();
        let offset = (size_of_object * index as usize + LENGTH_MARKER_SIZE) as u64;
        println!("read offset:{}", offset);
        let length = count * size_of_object;
        println!("read length:{}", length);

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

fn get_item_size<T>() -> usize {
    // 计算类型 T 的大小
    size_of::<T>()
}

#[cfg(test)]
mod test {
    use super::*;
    const COUNT: usize = 1000;

    #[derive(Serialize, Deserialize, Default, Debug, Clone, CheckDynamicSize)]
    pub struct ExampleStruct {
        my_number: usize,
        // my_string: String,
        my_vec: Vec<usize>,
        // my_vec: Vec<V>,
        // my_boolean: bool,
    }

    #[test]
    fn test_write_one() {
        // 创建服务实例
        let my_service = ObjectPersistOnDiskService::<ExampleStruct>::new(
            "data.bin".to_string(),
            "StringData.bin".to_string(),
            1024,
        )
        .unwrap();
        // 示例添加对象
        let i = 1;
        let my_obj = ExampleStruct {
            my_number: i,
            my_vec: vec![i],
            // my_string: format!("hello, world!{i}").to_string(),
            // my_boolean:  i %4 ==0,
        };
        my_service.add(my_obj);
    }

    #[test]
    fn test_add_static_one() {
        let my_service = ObjectPersistOnDiskService::<ExampleStruct>::new(
            "data.bin".to_string(),
            "StringData.bin".to_string(),
            1024,
        )
        .unwrap();

        let i = 2;
        let my_obj = ExampleStruct {
            my_number: i,
            my_vec: vec![1, 2, 3, 4, 5, 5, 5, 5, 5, 5],
            // my_string: format!("1hello, world!{i}").to_string(),
            // my_boolean:  i %4 ==0,
        };
        println!("size of my obj: {}", size_of_val(&my_obj));
        my_service.add(my_obj);
    }

    #[test]
    fn test_read_static_one() {
        let read_service = ObjectPersistOnDiskService::<ExampleStruct>::new(
            "data.bin".to_string(),
            "StringData.bin".to_string(),
            1024,
        )
        .unwrap();
        let result = read_service.read(0);
        println!("read one result:\n {:?}", result);
    }

    #[test]
    fn test_string_write_static_bulk() {
        // 创建服务实例
        let my_service = ObjectPersistOnDiskService::<ExampleStruct>::new(
            "data.bin".to_string(),
            "StringData.bin".to_string(),
            1024,
        )
        .unwrap();
        let mut objs_list = std::vec::Vec::new();
        for i in 0..COUNT {
            // 示例添加对象
            let my_obj = ExampleStruct {
                my_number: i,
                // my_string: format!("hello, world!{i}").to_string(),
                my_vec: vec![i],
                // my_boolean:  i %4 ==0,
            };
            objs_list.push(my_obj);
        }
        my_service.add_bulk(objs_list);
    }


    #[test]
    fn test_add_static_bulk() {
        // 创建服务实例
        let write_service = ObjectPersistOnDiskService::<ExampleStruct>::new(
            "data.bin".to_string(),
            "StringData.bin".to_string(),
            1024,
        )
        .unwrap();
        let mut objs_list = std::vec::Vec::new();
        for i in 0..COUNT {
            // let i = 1;
            let my_obj = ExampleStruct {
                my_number: i,
                // my_string: format!("hello, {i} world!").to_string(),
                my_vec: vec![i],
                // my_string: "hello, world!".to_string(),
                // my_string: "请问如果把该String写入文件,是将指针和长度写入文件吗 ChatGPT 说： ChatGPT 当你将 String 写入文件时，实际上写入的是字符串的内容，而不是指针和长度。Rust 中的 String 是一个动态大小的 UTF-8 编码字符串，它的内部结构包含指针、长度和容量，但在文件操作时只关注字符串的字节内容。 文件写入过程 当你使用 Rust 的标准库中的文件写入方法（例如 std::fs::File 和 std::io::Write trait）时，具体过程如下： 1. 获取内容：通过 String 的引用或方法获取其内容（即 UTF-8 编码的字节）。 2. 写入内容：将这些字节写入到文件中。 示例代码 下面是一个将 String 写入文件的示例： rust 复制代码 use std::fs::File; use std::io::{self, Write};  fn main() -> io::Result<()> {     // 创建一个 String     let my_string = String::from(Hello, Rust!);      // 创建或打开文件     let mut file = File::create(output.txt)?;      // 将字符串写入文件     file.write_all(my_string.as_bytes())?;      Ok(()) } 解释 1. 创建 String：在示例中，我们创建了一个包含字符串内容的 String 变量 my_string。 2. 打开文件：使用 File::create 创建一个新文件 output.txt，如果文件已经存在，将会被覆盖。 3. 写入内容： • 使用 my_string.as_bytes() 方法获取字符串内容的字节数组。 • write_all 方法将这些字节写入文件。 总结 • 写入内容：在将 String 写入文件时，仅写入字符串的字节内容，而不是指针和长度。 • 文件内容：最终生成的文件只包含字符串的实际文本，而不包含任何关于 String 结构的内部信息。".to_string(),
            };
            objs_list.push(my_obj.clone());
            println!("size of ExampleStruct:{}", size_of::<ExampleStruct>());
        }
        write_service.add_bulk(objs_list);
    }

    #[test]
    fn test_read_static_bulk() {
        let read_service = ObjectPersistOnDiskService::<ExampleStruct>::new(
            "data.bin".to_string(),
            "StringData.bin".to_string(),
            1024,
        )
        .unwrap();
        let objs = read_service.read_bulk(0, COUNT);
        println!("last read obj: {:?}", objs[objs.len() - 1]);
    }

    #[test]
    fn test_io_static_bulk() {
        // 创建服务实例
        let io_service = ObjectPersistOnDiskService::<ExampleStruct>::new(
            "data.bin".to_string(),
            "StringData.bin".to_string(),
            1024,
        )
        .unwrap();

        let mut objs_list = std::vec::Vec::new();
        for i in 0..COUNT {
            // 示例添加对象

            let my_obj = ExampleStruct {
                my_number: i,
                // my_string: format!("hello, {i} world!").to_string(),
                my_vec: vec![i],
            };
            objs_list.push(my_obj.clone());
        }
        io_service.add_bulk(objs_list);
        let objs = io_service.read_bulk(0, COUNT);
        assert_eq!(objs.len(), COUNT);
        // assert_eq!(objs[COUNT - 1].my_number as usize, COUNT - 1);
    }

    #[test]
    fn test_get_length() {
        let read_service = ObjectPersistOnDiskService::<ExampleStruct>::new(
            "data.bin".to_string(),
            "StringData.bin".to_string(),
            1024,
        )
        .unwrap();
        let length = read_service.get_length();
        println!("length: {}", length);
    }

    #[test]
    fn test_macro() {
        let i = 10;
        let example = ExampleStruct {
            my_number: i,
            // my_string: format!("hello, {i} world!").to_string(),
            my_vec: vec![i],
            // my_numbers: vec![1, 2, 3],
            // name: String::from("Example"),
            // fixed_number: 42,
        };

        assert!(example.is_dynamic_structure());

        // example.check_dynamic_fields();
        let result: Vec<String> = example.get_dynamic_fields();
        println!("{:?}", result);
    }
}
