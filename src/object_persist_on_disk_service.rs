use rayon::prelude::*;
use serde::{Serialize, Deserialize};
use serde_json::Value;
use std::io::{self};
use std::sync::{Arc, Mutex};
use std::mem::size_of;
// use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::marker::PhantomData;

// use dynamic_utils::CheckDynamicSize;
use dynamic_vector::DynamicVector;
use dynamic_vector::CheckDynamicSize;


use crate::cached_file_access_service::CachedFileAccessService;
use crate::string_repository::StringRepository; 

// const LENGTH_MARKER_SIZE: usize = size_of::<i32>(); // We reserve the first 4 bytes for Length
const LENGTH_MARKER_SIZE: usize = size_of::<u64>(); // We reserve the first 8 bytes for Length

/// 提供了将对象序列化和反序列化到磁盘的服务
/// 
/// - `T`: 要序列化的对象类型，必须实现 `Serialize` 和 `DeserializeOwned` 特征
pub struct ObjectPersistOnDiskService<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Default+Send,
{
    length: Arc<Mutex<i32>>, 
    structure_file: Mutex<CachedFileAccessService>, // 结构文件的文件句柄
    // string_repository: StringRepository,
    dynamic_repository_dir:  String,
    initial_size_if_not_exists: u64,
    _marker: PhantomData<T>, // 用于存储对象类型的占位符
}


impl<T: Send+Sync> ObjectPersistOnDiskService<T>
where
    // T: Serialize + for<'de> Deserialize<'de> + Default,
    T: Serialize + for<'de> Deserialize<'de> + Default + 'static+ DynamicVector,
{
    pub fn new(structure_file_path: String, string_file_path: String, initial_size_if_not_exists: u64) -> io::Result<Self> {
        let structure_file_access = CachedFileAccessService::new(structure_file_path, initial_size_if_not_exists);
        std::fs::create_dir_all(&string_file_path);
        // let string_repository = StringRepository::new(string_file_path.clone(), initial_size_if_not_exists);
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
            // string_repository: string_repository,
            dynamic_repository_dir: string_file_path,
            initial_size_if_not_exists: initial_size_if_not_exists,
            _marker: PhantomData,
        })
    }
    
    /// 获取当前对象数量
    pub fn get_length(&self) -> u64 {
        let structure_file_guard =self.structure_file.lock().unwrap(); 
        let buffer = structure_file_guard.read_in_file(0, LENGTH_MARKER_SIZE);
        // 确保 buffer 的长度至少为 4
        assert!(buffer.len() >= 4, "Buffer length must be at least 4 bytes.");

    // 将前 4 个字节转换为 i32，假设使用小端字节序
    // let length = i32::from_le_bytes(buffer[0..4].try_into().unwrap());
    let length = u64::from_le_bytes(buffer[0..8].try_into().unwrap());

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
        // println!("length of data  to deserialize {:?}", data.len());
        // bincode::deserialize(data).expect("Deserialization failed")
        // bincode::deserialize(data).unwrap_or_else(|e| {
            // eprintln!("Deserialization failed: {}", e);
            // panic!("Deserialization error")
        // })
        bincode::deserialize(data).expect("Deserialization failed")
    }

    /// 将对象写入指定索引位置
    fn write_index(&self, index: i64, obj: T) {
        if !obj.is_dynamic_structure() {
        let size_of_object = get_item_size::<T>();
        let data = Self::serialize_object(&obj);
        println!("bytes length of data to write once: {} ", &data.len());
        // let offset = (4 + (index as usize * data.len())) as u64;
        let offset   = (size_of_object * index as usize + LENGTH_MARKER_SIZE as usize) as u64;

        let file = self.structure_file.lock().unwrap();
        // file.seek(SeekFrom::Start(offset)).expect("Seek failed.");
        // file.write_all(&data).expect("Write failed.");
        file.write_in_file(offset, &data);
        } else {
            let size_of_object = get_item_size::<T>();
            let file = self.structure_file.lock().unwrap();
            self.save_object_dynamic(vec![obj]);
        }
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
        let white_space = size_of_object -  serialized_size;
        println!("white_space: {}", white_space);
        // 将序列化对象写入缓冲区
        buffer[current_position..current_position + serialized_size].copy_from_slice(&serialized_obj);
        current_position += white_space;
        
        current_position += serialized_size;


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
        let offset   = (size_of_object * index as usize + LENGTH_MARKER_SIZE as usize) as u64;
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
        // if !obj.is_dynamic_structure() {
        let index_to_write = {
            let mut length = self.length.lock().unwrap();
            let index = *length;
            *length += 1;
            self.save_length(*length);
            index
        };
        self.write_index(index_to_write.into(), obj);
        // } else {
            // let  dynamic_fields: Vec<String> = obj.get_dynamic_fields();
            // println!("dynamic fields: {:?}", dynamic_fields);
            // self.save_object_dynamic(vec![obj]);
            // panic! ("dynamic  type");
        // }
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
    
    // save_object_dynamic(&self, objs: Vec<T>) -> Vec<string_repository::ObjectWithPersistedDynamic<T>> {
    fn save_object_dynamic(&self, objs: Vec<T>)  {
        let  dynamic_fields: Vec<String> =  objs[0].get_dynamic_fields();
        let  dynamic_fields_count = dynamic_fields.len();
        println!("dynamic_fields: {:?}, dynamic_fields_count: {}", dynamic_fields, dynamic_fields_count);

        let total_dynamic_fields_count = dynamic_fields_count * objs.len();
        let mut dynamic_bytes:Vec<u8> = vec![0;total_dynamic_fields_count];
let mut json_objs: Vec<_> = objs.par_iter()
     .map( |obj| serde_json::to_value(obj).unwrap())
     .collect();
     println!("json_objs, {:?}", json_objs);
     // let objs_value: Vec<_> =Vec::new();
     let objs_value: Vec<_> =Vec::new();
     
     let arc_objs = Arc::new(Mutex::new(json_objs));
     let arc_objs_clone = Arc::clone(&arc_objs);
     
     let arc_objs_value = Arc::new(Mutex::new(objs_value));
     let arc_objs_value_clone = Arc::clone(&arc_objs_value);
     
        dynamic_fields.par_iter()
        // .map( |field| {
        .for_each(move  |field| {
        // let mut field_obj_len_list: Vec<usize> = vec![0; arc_objs_clone.len()];
        let mut field_obj_len_list: Vec<usize> = vec![0; arc_objs_clone.lock().unwrap().len()];
        // let field_string_list: Vec<String> =  objs.par_iter()
    // let  mut json_objs: Vec<Value> = objs.par_iter()
    // json_objs = objs.par_iter()
     // .map( |obj| serde_json::to_value(obj).unwrap())
     // .map(|obj| obj.get(field.clone()))
     // .filter_map( |x| x)
     // .cloned()
     
    // *arc_objs_value_clone.lock().unwrap() = arc_objs_clone.lock().unwrap()
    let  field_string_list = arc_objs_clone.lock().unwrap()
     .par_iter()
     .filter_map(|obj| obj.get(field.clone()).cloned())
     .collect::<Vec<_>>();
     // dbg!(arc_objs_value_clone.lock().unwrap());
     
     
     // let field_string_list: Vec<String> = json_objs.par_iter()
     // let field_string_list1: Vec<String> = arc_objs_clone.lock().unwrap().par_iter()
     *arc_objs_value_clone.lock().unwrap() = arc_objs_clone.lock().unwrap()
     .par_iter()
     // .map(|obj| obj.to_string())
     // .collect::<Vec<String>>();
     .cloned()
     .collect::<Vec<Value>>();
     dbg!(&field_string_list);
     
     field_string_list.iter()
     .map(|obj| obj.to_string())
     .for_each(|obj| {
         field_obj_len_list.push(obj.len());
     });
     let field_string = field_string_list.iter()
     .cloned()
     .map(|obj| obj.to_string())
     .collect::<String>();
     
let file_path = std::path::Path::new(&self.dynamic_repository_dir).join(field.clone());
// std::fs::File::create(file_path.clone());
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .open(file_path.clone());

let file_path_str = file_path.to_string_lossy().into_owned();
let mut  string_repository = StringRepository::new(file_path_str, self.initial_size_if_not_exists.clone());
let byte_vector: Vec<u8> = field_string.as_bytes().to_vec();
let (offset, total_length ) = string_repository.write_string_content_and_get_offset(byte_vector);
// let obj_with_persisted_dynamic = objs.iter() 
// field_obj_len_list.iter().map(|length|{  field
// .for_each(|i| {
    let mut current_offset = offset;
for i in 0..objs.len() {
    // if let Some(dynamic_obj_value) = json_objs[i].get_mut(field.clone()) {
    // if let Some(dynamic_obj_value) = arc_objs_clone.lock().unwrap()[i].get_mut(field.clone()) {

    if let Some(dynamic_obj_value) = arc_objs_value_clone.lock().unwrap()[i].get_mut(field.clone()) {
        let offset_and_total_length = vec![current_offset, current_offset + field_obj_len_list[i] as u64];
        dbg!(&field_obj_len_list[i]);
        println!("offset_and_total_length: {:?}", &offset_and_total_length);
        *dynamic_obj_value = offset_and_total_length.into();
        current_offset +=  field_obj_len_list[i] as u64;
    } else {
        println!("no value!");
    }
}
// json_objs
// arc_objs_clone
});
// .flatten()
// .collect::<Vec<_>>();
    }
    
}


fn get_item_size<T>() -> usize {
    // 计算类型 T 的大小
    size_of::<T>()
}

 
# [cfg(test)]
mod test {
    use super:: * ;
    const  COUNT: usize = 10;
    
    #[derive(Serialize, Deserialize, Default, Debug, Clone, CheckDynamicSize)]
        pub struct ExampleStruct {
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
    fn test_string_write_bulk() {
        // 创建服务实例        
        let my_service = ObjectPersistOnDiskService:: <ExampleStruct> ::new("data.bin".to_string(), "StringData.bin".to_string(), 1024).unwrap();
        let mut objs_list = std::vec::Vec::new();
        for i in 0..COUNT {
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
        for i in 1..COUNT {
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
        for i in 0..COUNT {
            // let i = 1;
        let my_obj = ExampleStruct {
            my_number: i,
            my_string: format!("hello, {i} world!").to_string(),
            // my_string: "hello, world!".to_string(),
            // my_string: "请问如果把该String写入文件,是将指针和长度写入文件吗 ChatGPT 说： ChatGPT 当你将 String 写入文件时，实际上写入的是字符串的内容，而不是指针和长度。Rust 中的 String 是一个动态大小的 UTF-8 编码字符串，它的内部结构包含指针、长度和容量，但在文件操作时只关注字符串的字节内容。 文件写入过程 当你使用 Rust 的标准库中的文件写入方法（例如 std::fs::File 和 std::io::Write trait）时，具体过程如下： 1. 获取内容：通过 String 的引用或方法获取其内容（即 UTF-8 编码的字节）。 2. 写入内容：将这些字节写入到文件中。 示例代码 下面是一个将 String 写入文件的示例： rust 复制代码 use std::fs::File; use std::io::{self, Write};  fn main() -> io::Result<()> {     // 创建一个 String     let my_string = String::from(Hello, Rust!);      // 创建或打开文件     let mut file = File::create(output.txt)?;      // 将字符串写入文件     file.write_all(my_string.as_bytes())?;      Ok(()) } 解释 1. 创建 String：在示例中，我们创建了一个包含字符串内容的 String 变量 my_string。 2. 打开文件：使用 File::create 创建一个新文件 output.txt，如果文件已经存在，将会被覆盖。 3. 写入内容： • 使用 my_string.as_bytes() 方法获取字符串内容的字节数组。 • write_all 方法将这些字节写入文件。 总结 • 写入内容：在将 String 写入文件时，仅写入字符串的字节内容，而不是指针和长度。 • 文件内容：最终生成的文件只包含字符串的实际文本，而不包含任何关于 String 结构的内部信息。".to_string(),
        };
        objs_list.push(my_obj.clone());
        println!("size of ExampleStruct:{}", size_of::<ExampleStruct>());
        }
        write_service.add_bulk(objs_list);
            }
            
            #[test]
    fn test_read_bulk() {
        let read_service =ObjectPersistOnDiskService:: <ExampleStruct> ::new("data.bin".to_string(), "StringData.bin".to_string(), 1024).unwrap();
        let objs = read_service.read_bulk(0, COUNT);
        println!("last read obj: {:?}", objs[objs.len() - 1]);
    }
    
    #[test]
    fn test_io_bulk() {
        // 创建服务实例        
        let io_service = ObjectPersistOnDiskService:: <ExampleStruct> ::new("data.bin".to_string(), "StringData.bin".to_string(), 1024).unwrap();

        let mut objs_list = std::vec::Vec::new();
        for i in 0..COUNT {
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
        let objs = io_service.read_bulk(0, COUNT );
        assert_eq!(objs.len(), COUNT);
// assert_eq!(objs[COUNT - 1].my_number as usize, COUNT - 1);
    }
    
    #[test]
    fn test_get_length() {
        let read_service =ObjectPersistOnDiskService:: <ExampleStruct> ::new("data.bin".to_string(), "StringData.bin".to_string(), 1024).unwrap();
        let length = read_service.get_length();
        println!("length: {}", length);
    }
    
    #[test]
fn test_macro() {
    let i =10;
    let example = ExampleStruct {
        my_number: i,
        my_string: format!("hello, {i} world!").to_string(),
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

