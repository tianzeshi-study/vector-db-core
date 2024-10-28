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
pub struct ObjectPersistOnDiskService<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Default,
{
    length: Arc<Mutex<i32>>, // 线程安全的对象数量
    structure_file: Mutex<CachedFileAccessService>, // 结构文件的文件句柄
    string_repository: StringRepository,
    _marker: PhantomData<T>, // 用于存储对象类型的占位符
}


impl<T: Send> ObjectPersistOnDiskService<T>
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
    fn get_length(&self) -> i32 {
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
        bincode::deserialize(data).expect("Deserialization failed")
    }

    /// 将对象写入指定索引位置
    fn write_index(&self, index: i64, obj: T) {
        let size_of_object = get_item_size::<T>();
        let data = Self::serialize_object(&obj);
        // let offset = (4 + (index as usize * data.len())) as u64;
        let offset   = (size_of_object * index as usize + LENGTH_MARKER_SIZE) as u64;

        let file = self.structure_file.lock().unwrap();
        // file.seek(SeekFrom::Start(offset)).expect("Seek failed.");
        // file.write_all(&data).expect("Write failed.");
        file.write_in_file(offset, &data); 
    }

// fn write_bulk(&self, index: i64, objs: Vec<T>) {
        // let size_of_object = get_item_size::<Vec<T>>();
        // let data = Self::serialize_object(&obj);

        // objs.par_iter().enumerate().for_each(|(i, obj)| {
            // serialize_bytes(obj, buffer, size_of_object * i);
        // });
        // let offset   = (size_of_object * index as usize + LENGTH_MARKER_SIZE) as u64;

        // let file = self.structure_file.lock().unwrap();
        // file.write_in_file(offset, &data); 
    // }

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
    
    pub fn read(&self, index: usize, count: usize) -> T{
        let size_of_object = get_item_size::<T>();
        // let offset = (4 + (index as usize * data.len())) as u64;
        let offset   = (size_of_object * index as usize + LENGTH_MARKER_SIZE) as u64;
        println!("read offset:{}", offset);

        let file_guard = self.structure_file.lock().unwrap();
        let length = count*size_of_object;
        println!("read length:{}", length);
        let  data: Vec<u8> = file_guard.read_in_file(offset, length); 
        let objs = Self::deserialize_object(&data);

        
        // let buffer: [u8; std::mem::size_of::<i32>()] = length.to_ne_bytes();
        // let file_guard = self.structure_file.lock().unwrap();
        // file_guard.read_in_file(0, &buffer);
objs
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
     # [test]
    fn test_object_persist_on_disk_service() {

// 示例对象结构，实现 `Serialize` 和 `Deserialize` 特征
#[derive(Serialize, Deserialize, Default)]
struct MyStruct {
    id: i32,
    name: String,
}

#[derive(Serialize, Deserialize, Default)]
struct IntStruct {id: i32}

        // 创建服务实例
        let my_service = ObjectPersistOnDiskService:: < MyStruct > ::new("data.bin".to_string(), "StringData.bin".to_string(), 1024).unwrap();
        let service = ObjectPersistOnDiskService:: < IntStruct > ::new("data.bin".to_string(), "StringData.bin".to_string(), 1024).unwrap();
        // 示例添加对象
        let obj = IntStruct {id:0};
        let my_obj = MyStruct {
            id: 0,
            name: String::from("hello")
        };
        let my_obj2 = MyStruct {
            id: 1,
            name: String::from("hello")
        };
        service.add(obj);
        my_service.add(my_obj);
        my_service.add(my_obj2);

        // 示例读取对象
        // let read_obj = service.read_index(0);
        // let read_obj1 = service.read_index(1);
        // assert_eq!(read_obj.id, 0);
        // assert_eq!(read_obj.name, "hello");
        // println!("读取的对象: id={}, name={}", read_obj.id, read_obj.name);
        // dbg!(read_obj.id);
        // dbg!(read_obj.name);
    }
    
    # [test]
    fn test_write_performance() {
        // 示例对象结构，实现 `Serialize` 和 `Deserialize` 特征
        #[derive(Serialize, Deserialize, Default)]
        struct MyStruct {
            my_number: i32,
            my_string: String,
            my_boolean: bool,
        }

        // 创建服务实例        
        let my_service = ObjectPersistOnDiskService:: < MyStruct > ::new("data.bin".to_string(), "StringData.bin".to_string(), 1024).unwrap();
        for i in 0..1000 {
        // 示例添加对象
        let my_obj = MyStruct {
            my_number: i,
            my_string: format!("hello, world!{i}").to_string(),
            my_boolean:  i %4 ==0,
        };
        my_service.add(my_obj);
        }

    }
    
    
# [test]
    fn test_bulk_write() {
        // 示例对象结构，实现 `Serialize` 和 `Deserialize` 特征
        #[derive(Serialize, Deserialize, Default)]
        struct MyStruct {
            my_number: i32,
            my_string: String,
            my_boolean: bool,
        }

        // 创建服务实例        
        let my_service = ObjectPersistOnDiskService:: <Vec<MyStruct>> ::new("data.bin".to_string(), "StringData.bin".to_string(), 1024).unwrap();
        let mut objs_list = std::vec::Vec::new();
        for i in 0..1000000 {
        // 示例添加对象
        let my_obj = MyStruct {
            my_number: i,
            my_string: format!("hello, world!{i}").to_string(),
            my_boolean:  i %4 ==0,
        };
        objs_list.push(my_obj);
        }
        my_service.add(objs_list);
    }

    # [test]
    fn test_read() {
        // 示例对象结构，实现 `Serialize` 和 `Deserialize` 特征
        #[derive(Serialize, Deserialize, Default, Debug, Clone)]
        struct MyStruct {
            my_number: i32,
            my_string: String,
            // my_str: &'1 str,
            // my_boolean: bool,
        }

        // 创建服务实例        
        let my_service = ObjectPersistOnDiskService:: <Vec<MyStruct>> ::new("data.bin".to_string(), "StringData.bin".to_string(), 1024).unwrap();
        let my_read_service = ObjectPersistOnDiskService:: <MyStruct> ::new("data.bin".to_string(), "StringData.bin".to_string(), 1024).unwrap();
        let mut objs_list = std::vec::Vec::new();
        for i in 0..10 {
        // 示例添加对象
        let test_string = "hello, world!".to_string();
        let my_obj = MyStruct {
            my_number: i,
            // my_string: format!("hello, world!{i}").to_string(),
            // my_string: "hello, world!".to_string(),
            my_string: test_string,
            // my_str: "hello str",
            // my_boolean: true,
        };
        objs_list.push(my_obj.clone());
        // my_read_service.add(my_obj);
        }
        // my_service.add(objs_list);
        // let vec_objs = my_service.read(0, 10);
        // println!("{:?}", vec_objs);  
        let my_structure_size = size_of::<MyStruct>();
        let my_vec_structure_size = size_of::<Vec<MyStruct>>();
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
    fn test_read_bulk() {
        // 示例对象结构，实现 `Serialize` 和 `Deserialize` 特征
        #[derive(Serialize, Deserialize, Default, Debug, Clone)]
        struct MyStruct {
            my_number: i32,
            my_string: String,
            // my_str: &'1 str,
            // my_boolean: bool,
        }

        // 创建服务实例        
        let my_service = ObjectPersistOnDiskService:: <Vec<MyStruct>> ::new("data.bin".to_string(), "StringData.bin".to_string(), 1024).unwrap();
        let my_read_service = ObjectPersistOnDiskService:: <MyStruct> ::new("data.bin".to_string(), "StringData.bin".to_string(), 1024).unwrap();
        let mut objs_list = std::vec::Vec::new();
        for i in 0..10 {
        // 示例添加对象
        let test_string = "hello, world!".to_string();
        let my_obj = MyStruct {
            my_number: i,
            // my_string: format!("hello, world!{i}").to_string(),
            // my_string: "hello, world!".to_string(),
            my_string: test_string,
            // my_str: "hello str",
            // my_boolean: true,
        };
        objs_list.push(my_obj.clone());
        // my_read_service.add(my_obj);
        }
        let objs = my_read_service.read_bulk(3, 40);
        println!("{:?}", objs);


    }
    
}

