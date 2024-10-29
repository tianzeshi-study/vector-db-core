use rayon::prelude::*;
use serde::{Serialize, Deserialize};
use std::io::{self};
use std::sync::{Arc, Mutex};
use std::any::Any;
use std::mem::size_of;
// use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::marker::PhantomData;

use crate::cached_file_access_service::CachedFileAccessService;
use crate::string_repository::StringRepository; 

const LENGTH_MARKER_SIZE: usize = size_of::<i32>(); // We reserve the first 4 bytes for Length

/// 提供了将对象序列化和反序列化到磁盘的服务
/// 
/// - `T`: 要序列化的对象类型，必须实现 `Serialize` 和 `DeserializeOwned` 特征
pub struct ObjectPersistOnDiskService<T: Send>
where
    T: Serialize + for<'de> Deserialize<'de> + Default,
{
    length: Arc<Mutex<i32>>, // 线程安全的对象数量
    structure_file: Mutex<CachedFileAccessService>, // 结构文件的文件句柄
    string_repository: StringRepository,
    _marker: PhantomData<T>, // 用于存储对象类型的占位符
}


impl<T: Send+Sync> ObjectPersistOnDiskService<T>
where
    // T: Serialize + for<'de> Deserialize<'de> + Default,
    T: Serialize + for<'de> Deserialize<'de> + Default + 'static,
{
    pub fn new(structureFilePath: String, stringFilePath: String, initialSizeIfNotExists: u64) -> io::Result<Self> {
        let structure_file_access = CachedFileAccessService::new(structureFilePath, initialSizeIfNotExists, 1024, 512);
        let string_repository = StringRepository::new(stringFilePath, initialSizeIfNotExists);
        // let length = get_length();
        let length = {
            let buffer = structure_file_access.read_in_file(0, LENGTH_MARKER_SIZE);
            // 确保 buffer 的长度至少为 4
            assert!(buffer.len() >= 4, "Buffer length must be at least 4 bytes.");

    // 将前       4 个字节转换为 i32，假设使用小端字节序
            let length = i32::from_le_bytes(buffer[0..4].try_into().unwrap());

            Arc::new(Mutex::new(length))
        };
        Ok(Self {
            length,
            structure_file: Mutex::new(structure_file_access),
            string_repository: string_repository,
            _marker: PhantomData,
        })
    }
    
    /// 获取当前对象数量
    pub fn get_length(&self) -> i32 {
        let structure_file_guard =self.structure_file.lock().unwrap(); 
        let buffer = structure_file_guard.read_in_file(0, LENGTH_MARKER_SIZE);
        // 确保 buffer 的长度至少为 4
        assert!(buffer.len() >= 4, "Buffer length must be at least 4 bytes.");

    // 将前 4 个字节转换为 i32，假设使用小端字节序
    let length = i32::from_le_bytes(buffer[0..4].try_into().unwrap());

        // *self.length.lock().unwrap()
        length
    }

    /// 保存当前对象数量
    fn save_length(&self, length: i32) {
        let buffer: [u8; std::mem::size_of::<i32>()] = length.to_ne_bytes();
        let file_guard = self.structure_file.lock().unwrap();
        file_guard.write_in_file(0, &buffer);
    }

    // fn serialize_byte(obj: &T) -> Vec<u8> {
    // }

    /// 序列化对象到字节数组
    fn serialize_object(obj: &T) -> Vec<u8> {
        bincode::serialize(obj).expect("Serialization failed")
    }

    /// 从字节数组反序列化为对象
    fn deserialize_object(data: &[u8]) -> T{
        // println!("{:?}", data);
        bincode::deserialize(data).expect("Deserialization failed")
    }

    /// 将对象写入指定索引位置
    fn write_index(&self, index: i64, obj: T) {
        let size_of_object = get_item_size::<T>();
        let data = Self::serialize_object(&obj);
        println!("bytes length of data to write once: {} ", &data.len());
        // let offset = (4 + (index as usize * data.len())) as u64;
        let offset   = (size_of_object * index as usize + LENGTH_MARKER_SIZE) as u64;

        let file = self.structure_file.lock().unwrap();
        // file.seek(SeekFrom::Start(offset)).expect("Seek failed.");
        // file.write_all(&data).expect("Write failed.");
        file.write_in_file(offset, &data); 
    }
    
    fn bulk_write_index(&self, index: i64, objs: Vec<T>) {
        let size_of_object = get_item_size::<T>();
        let count  = objs.len();
        let mut buffer: Vec<u8> = vec![0; size_of_object * count];
        println!("length of buffer: {}", buffer.len());
        
        // let data: Vec<u8> = objs.par_iter()
        // .map(|obj| Self::serialize_object(obj))
        // .flatten()
        // .collect::<Vec<u8>>();

let serialized_objs: Vec<Vec<u8>> = objs.par_iter()
        .map(|obj| Self::serialize_object(obj))
        .collect::<Vec<Vec<u8>>>();

    let mut current_position = 0;
    for serialized_obj in &serialized_objs {
    // for obj in &objs {
        // let serialized_obj = Self::serialize_object(&obj);
        let serialized_size = serialized_obj.len();

        // 将序列化对象写入缓冲区
        buffer[current_position..current_position + serialized_size].copy_from_slice(&serialized_obj);
        current_position += serialized_size;
        let white_space = size_of_object -  serialized_size;
        println!("white_space: {}", white_space);

        current_position += white_space;
    }
    
        let vec_data: Vec<Vec<u8>> = objs.par_iter()
        .map(|obj| Self::serialize_object(obj))
        .collect::<Vec<Vec<u8>>>();
        println!("bulk write size_of_object:{}", size_of_object);
        println!("length of  vec_data:{}", vec_data.len());
        println!("length of  item of vec_data:{}", vec_data[0].len());
        println!("bytes of vec_data:{}", vec_data[0].len() * vec_data.len()); 
        
        // .flat_map(|vec| vec)
        // let offset = (4 + (index as usize * data.len())) as u64;
        let offset   = (size_of_object * index as usize + LENGTH_MARKER_SIZE) as u64;
        println!("bulk write offset: {}",  offset);
        // println!("bulk write data length:{}", data.len());

        let file_guard = self.structure_file.lock().unwrap();
        // file.seek(SeekFrom::Start(offset)).expect("Seek failed.");
        // file.write_all(&data).expect("Write failed.");
        // file_guard.write_in_file(offset, &data); 
        file_guard.write_in_file(offset, &buffer); 
    }


    /// 添加单个对象
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
            *length += count as i32;
            // *length += 1;
            self.save_length(*length);
            index
        };
        println!("add bulk index_to_write:{}", index_to_write);
        self.bulk_write_index(index_to_write.into(), objs);
    }
    
    pub fn read(&self, index: usize, count: usize) -> T{
        let size_of_object = get_item_size::<T>();
        // let offset = (4 + (index as usize * data.len())) as u64;
        let offset   = (size_of_object * index as usize + LENGTH_MARKER_SIZE) as u64;
        println!("read offset:{}", offset);

        let file_guard = self.structure_file.lock().unwrap();
        let length = count*size_of_object;
        println!("read length:{}", length);
        let  data: Vec<u8> = file_guard.read_in_file(offset, length);
        dbg!(&data.len());
        let obj = Self::deserialize_object(&data);


        
        // let buffer: [u8; std::mem::size_of::<i32>()] = length.to_ne_bytes();
        // let file_guard = self.structure_file.lock().unwrap();
        // file_guard.read_in_file(0, &buffer);
obj
    }
    
    pub fn read_bulk(&self, index: usize, count: usize) -> Vec<T>{
        let size_of_object = get_item_size::<T>();
        let offset   = (size_of_object * index as usize + LENGTH_MARKER_SIZE) as u64;
        println!("read offset:{}", offset);
        let length = count*size_of_object;
        println!("read length:{}", length);

        let file_guard = self.structure_file.lock().unwrap();
        let  data: Vec<u8> = file_guard.read_in_file(offset, length);
        // let objs: Vec<T> = data.chunks(size_of_object)
        // .map(|data| Self::deserialize_object(data)) // one thread  deserialize 
        let objs: Vec<T> = data.par_chunks(size_of_object)
        .map(|data| Self::deserialize_object(data)) 
        .collect(); 
        
        objs
    }



}


fn get_item_size1<T: Any>() -> usize {
    let mut size = 0;

    // 获取类型的字段信息
    let type_name = std::any::type_name::<T>();

    // 使用简单的模式匹配来判断类型
    if type_name == "i32" || type_name == "bool" {
        size += size_of::<i32>();
    } else if type_name == "String" {
        size += size_of::<u64>() * 2; // Offset 和 Length（两个 long 值）
    } else if type_name == "std::time::SystemTime" { // Rust 中的 DateTime
        size += size_of::<u64>(); // DateTime（以 Ticks 存储，作为 u64）
    } else {
        panic!("Unsupported property type: {:?}", type_name);
    }

    size
}

fn get_item_size<T>() -> usize {
    // 计算类型 T 的大小
    size_of::<T>()
}

 # [cfg(test)]
mod test {
    use super:: * ;
    const  count: usize = 999999;
    
    #[derive(Serialize, Deserialize, Default, Debug, Clone)]
        struct ExampleStruct {
            my_number: usize,
            my_string: String,
            // my_boolean: bool,
        }
        
    
    # [test]
    fn test_write_one() {
        // 创建服务实例        
        let my_service = ObjectPersistOnDiskService:: < ExampleStruct > ::new("data.bin".to_string(), "StringData.bin".to_string(), 1024).unwrap();
        // 示例添加对象
        let i = 1;
        let my_obj = ExampleStruct {
            my_number: i,
            my_string: format!("hello, world!{i}").to_string(),
            // my_boolean:  i %4 ==0,
        };
        my_service.add(my_obj);
    }
    
    
                #[test]
    fn test_read_one() {
        let read_service =ObjectPersistOnDiskService:: <ExampleStruct> ::new("data.bin".to_string(), "StringData.bin".to_string(), 1024).unwrap();
        read_service.read(0, 10);
    }


    # [test]
    fn test_write_bulk() {
        // 创建服务实例        
        let my_service = ObjectPersistOnDiskService:: <ExampleStruct> ::new("data.bin".to_string(), "StringData.bin".to_string(), 1024).unwrap();
        let mut objs_list = std::vec::Vec::new();
        for i in 0..count {
        // 示例添加对象
        let my_obj = ExampleStruct {
            my_number: i,
            my_string: format!("hello, world!{i}").to_string(),
            // my_boolean:  i %4 ==0,
        };
        objs_list.push(my_obj);
        }
        my_service.add_bulk(objs_list);
    }

    # [test]
    fn test_size_of_struct() {
        let my_read_service = ObjectPersistOnDiskService:: <ExampleStruct> ::new("data.bin".to_string(), "StringData.bin".to_string(), 1024).unwrap();
        let mut objs_list = std::vec::Vec::new();
        for i in 1..count {
        // 示例添加对象
        let my_obj = ExampleStruct {
            my_number: i,
            my_string: format!("hello, world!{i}").to_string(),
        };
        objs_list.push(my_obj.clone());
        // my_read_service.add(my_obj);
        }
        // my_service.add(objs_list);
        // let vec_objs = my_service.read(0, 10);
        // println!("{:?}", vec_objs);  
        let my_structure_size = size_of::<ExampleStruct>();
        let my_vec_structure_size = size_of::<Vec<ExampleStruct>>();
        let my_string_size = size_of::<String>();
        let my_str_size = size_of::<&str>();
        let my_char_size = size_of::<char>();
        let my_bool_size = size_of::<bool>();
        let objs = my_read_service.read(1, 2);
        println!("{:?}", objs);
        dbg!(my_structure_size, my_vec_structure_size, my_str_size, my_string_size, my_char_size, my_bool_size, objs);
        // assert!(false);
    }
    
    #[test]
    fn test_add_bulk() {
        // 创建服务实例        
        let write_service = ObjectPersistOnDiskService:: <ExampleStruct> ::new("data.bin".to_string(), "StringData.bin".to_string(), 1024).unwrap();
        let mut objs_list = std::vec::Vec::new();
        for i in 0..count {
            // let i = 1;
        let my_obj = ExampleStruct {
            my_number: i,
            // my_string: format!("hello, {i} world!").to_string(),
            my_string: "hello, world!".to_string(),
        };
        objs_list.push(my_obj.clone());
        println!("size of ExampleStruct:{}", size_of::<ExampleStruct>());
        }
        write_service.add_bulk(objs_list);
            }
            
            #[test]
    fn test_read_bulk() {
        let read_service =ObjectPersistOnDiskService:: <ExampleStruct> ::new("data.bin".to_string(), "StringData.bin".to_string(), 1024).unwrap();
        let objs = read_service.read_bulk(0, count);
        println!("last read obj: {:?}", objs[objs.len() - 1]);
    }
    
    #[test]
    fn test_io_bulk() {
        // 创建服务实例        
        let io_service = ObjectPersistOnDiskService:: <ExampleStruct> ::new("data.bin".to_string(), "StringData.bin".to_string(), 1024).unwrap();

        let mut objs_list = std::vec::Vec::new();
        for i in 0..count {
        // 示例添加对象
        let i =1;
        let my_obj = ExampleStruct {
            my_number: i,
            my_string: format!("hello, {i} world!").to_string(),
        };
        objs_list.push(my_obj.clone());
        // println!("size of ExampleStruct:{}", size_of::<ExampleStruct>());
        }
        io_service.add_bulk(objs_list);
        let objs = io_service.read_bulk(0, count );
        assert_eq!(objs.len(), count);
assert_eq!(objs[count - 1].my_number as usize, count - 1);
    }
    
    #[test]
    fn test_get_length() {
        let read_service =ObjectPersistOnDiskService:: <ExampleStruct> ::new("data.bin".to_string(), "StringData.bin".to_string(), 1024).unwrap();
        let length = read_service.get_length();
        println!("length: {}", length);
    }
}

