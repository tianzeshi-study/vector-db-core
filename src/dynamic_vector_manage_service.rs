use rayon::prelude::*;
pub use serde::{Serialize, Deserialize};
use serde_json::Value;
use std::io::{self};
use std::sync::{Arc, Mutex, RwLock};
use std::cell::RefCell;
use std::time::Instant;
use std::mem::{size_of, size_of_val};
use std::marker::PhantomData;
use std::future::Future;


pub use dynamic_vector::DynamicVector;
pub use dynamic_vector::VectorCandidate;
pub use dynamic_vector::CheckDynamicSize;



use crate::file_access_service::FileAccessService;
use crate::string_repository::StringRepository; 


const LENGTH_MARKER_SIZE: usize = size_of::<u64>(); // We reserve the first 8 bytes for Length


pub struct DynamicVectorManageService<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Default+Send,
{
    length: Arc<Mutex<i32>>, 
    // structure_file: Mutex<CachedFileAccessService>, // 结构文件的文件句柄
    structure_file: Mutex<FileAccessService>, 
    string_repository: StringRepository,
    dynamic_repository_dir:  String,
    initial_size_if_not_exists: u64,
    _marker: PhantomData<T>, // 用于存储对象类型的占位符
}


impl<T: Send+Sync> DynamicVectorManageService<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Default + 'static+ DynamicVector +std::fmt::Debug + Clone,
{
    pub fn new(structure_file_path: String, string_file_path: String, initial_size_if_not_exists: u64) -> io::Result<Self> {
        // let structure_file_access = CachedFileAccessService::new(structure_file_path, initial_size_if_not_exists);
        let structure_file_access = FileAccessService::new(structure_file_path, initial_size_if_not_exists);
        let string_repository = StringRepository::new(string_file_path.clone(), initial_size_if_not_exists);
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
            dynamic_repository_dir: string_file_path,
            initial_size_if_not_exists: initial_size_if_not_exists,
            _marker: PhantomData,
        })
    }
    

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


    fn save_length(&self, length: i32) {
        let buffer: [u8; std::mem::size_of::<i32>()] = length.to_ne_bytes();
        let file_guard = self.structure_file.lock().unwrap();
        file_guard.write_in_file(0, &buffer);
    }



    fn load_object_dynamic(&self, objs: Vec<T>) -> Vec<Value>{

        let  dynamic_fields: Vec<String> =  objs[0].get_dynamic_fields();
        let  dynamic_fields_count = dynamic_fields.len();
        println!("dynamic_fields: {:?}, dynamic_fields_count: {}", dynamic_fields, dynamic_fields_count);

        let total_dynamic_fields_count = dynamic_fields_count * objs.len();
        let objs_len = objs.len();
        let mut dynamic_bytes:Vec<u8> = vec![0;total_dynamic_fields_count];
        println!("origin objs, {:?}", &objs);
             
let mut json_objs: Vec<Value> = objs.par_iter()
     .map( |obj| serde_json::to_value(obj).unwrap())
     .collect();

     // println!("json_objs, {:?}", json_objs);

     let objs_value: Vec<_> =Vec::new();
     
     let arc_objs = Arc::new(Mutex::new(json_objs));
     let arc_objs_clone = Arc::clone(&arc_objs);
     
     let arc_objs_value = Arc::new(Mutex::new(objs_value));
     let arc_objs_value_clone = Arc::clone(&arc_objs_value);
     
        dynamic_fields.par_iter()
        .for_each(move  |field| {

        let mut field_obj_len_list: Vec<Vec<usize>> = Vec::new();
    let  field_arguments_list: Vec<Vec<usize>> = arc_objs_clone.lock().unwrap()
     .par_iter()
     .map(|obj| obj.get(field.clone()).unwrap())
     .map( |obj| {
         dbg!(obj);
         serde_json::from_value(obj.clone()).unwrap()
     })
     .collect::<Vec<Vec<usize>>>();

     *arc_objs_value_clone.lock().unwrap() = arc_objs_clone.lock().unwrap()
     .par_iter()
     .cloned()
     .collect::<Vec<Value>>();

     
     field_arguments_list.iter()
     .for_each(|obj| {
         field_obj_len_list.push(obj.to_vec());
     });
     dbg!(&field_arguments_list);
     
let file_path = std::path::Path::new(&self.dynamic_repository_dir).join(field.clone());

let file_path_str = file_path.to_string_lossy().into_owned();
let mut  string_repository = StringRepository::new(file_path_str, self.initial_size_if_not_exists.clone());

let start_offset: usize = field_obj_len_list[0][0];
let total_length:usize = field_obj_len_list[field_arguments_list.len() - 1 ][1];
dbg!(&start_offset, &total_length);
let string_bytes:Vec<u8> =  string_repository.load_string_content(start_offset as u64, total_length as u64);
// let string_objs: Vec<u8> = bincode::deserialize(&string_bytes).expect("Deserialization failed");
let string_objs: Vec<u8> = string_bytes.to_vec();
dbg!(&string_objs);
   
    let mut current_offset = 0;
    for i in 0..objs_len {
    if let Some(dynamic_obj_value) = arc_objs_value_clone.lock().unwrap()[i].get_mut(field.clone()) {
        let offset_and_total_length = &field_obj_len_list[i];
        // let total_length =  offset_and_total_length[1];
        let offset =  offset_and_total_length[0];
        let end_offset =  offset_and_total_length[1];
        let string_bytes_len = end_offset - offset;
        let current_field_string_bytes  =   &string_bytes[current_offset..current_offset+string_bytes_len];
        current_offset += string_bytes_len;
        // let current_field_string  = String::from_utf8(current_field_string_bytes.to_vec()).expect("Invalid UTF-8 sequence");
        let current_field_string  = current_field_string_bytes.to_vec();
        dbg!(&current_field_string_bytes, &current_field_string);

        *dynamic_obj_value = serde_json::to_value(current_field_string).unwrap();
        dbg!(&dynamic_obj_value);
    } else {
        println!("no value!");
    }
}
});

let obj_with_persisted_dynamic = arc_objs_value.lock().unwrap().clone();
dbg!(&obj_with_persisted_dynamic);
obj_with_persisted_dynamic
    }
    
    fn save_dynamic(&mut self, objs: Vec<T>) -> (u64, u64) {
        let bytes = bincode::serialize(&objs).expect("Serialization failed");
        let (start_offset, end_offset) = self.string_repository.write_string_content_and_get_offset(bytes);
        
        (start_offset, end_offset)
    }
    
    fn save_dynamic_bulk1(&mut self, objs: Vec<T>) -> Vec<(u64, u64)> {
        
        let bytes: Vec<u8> = objs.par_iter()
        .map(|obj| bincode::serialize(&obj).expect("Serialization failed") )
        .flatten()
        // .cloned()
        .collect();
        let length_list: Vec<u64> = objs.par_iter()
        .map(|obj| bincode::serialize(&obj).expect("Serialization failed") )
        .map(|obj| obj.len() as u64)
        .collect();
        
        let (start_offset, _) = self.string_repository.write_string_content_and_get_offset(bytes);
        
        length_list
        .into_iter()
        .scan(start_offset, |current_offset, length| {
            let start = *current_offset;        // 当前对象的起点
            let end = start + length;          // 当前对象的终点
            *current_offset = end;             // 更新累加器为下一个对象的起点
            Some((start, end))                 // 返回当前区间
        })
        .collect::<Vec<(u64, u64)>>()
            }
            
            fn save_dynamic_bulk(&mut self, objs: Vec<T>) -> Vec<(u64, u64)> {
        
        let bytes_list: Vec<Vec<u8>>  = objs.par_iter()
        .map(|obj| bincode::serialize(&obj).expect("Serialization failed") )
        .collect::<Vec<Vec<u8>>>();
        
        let bytes: Vec<u8> = bytes_list.par_iter().flatten().cloned().collect();
        let length_list: Vec<u64> = bytes_list.par_iter()
        .map(|obj| obj.len() as u64)
        .collect();
        let (start_offset, _) = self.string_repository.write_string_content_and_get_offset(bytes);
        
        length_list
        .into_iter()
        .scan(start_offset, |current_offset, length| {
            let start = *current_offset;        // 当前对象的起点
            let end = start + length;          // 当前对象的终点
            *current_offset = end;             // 更新累加器为下一个对象的起点
            Some((start, end))                 // 返回当前区间
        })
        .collect::<Vec<(u64, u64)>>()
            }
    
    fn load_dynamic(&self, start_offset: u64, end_offset: u64) -> Vec<T> {
        let bytes: Vec<u8> = self.string_repository.load_string_content(start_offset, end_offset);
        let objs: Vec<T> =  bincode::deserialize(&bytes).expect("Serialization failed");
        objs
    }
    
    fn load_dynamic_bulk(&self, start_offset_and_end_offset_list: Vec<(u64, u64)>) -> Vec<T> {
        let start_offset = start_offset_and_end_offset_list[0].0;
        let end_offset = start_offset_and_end_offset_list[start_offset_and_end_offset_list.len() - 1].1;
        let bytes: Vec<u8> = self.string_repository.load_string_content(start_offset, end_offset);
        let length_list: Vec<u64> =start_offset_and_end_offset_list
        .par_iter()
.map(|obj| obj.1 - obj.0)
.collect();


let mut start = 0;
    let byte_vectors: Vec<Vec<u8>> = length_list
        .into_iter() 
        .map(|length| {
            let length = length as usize;

            assert!(start + length <= bytes.len(), "Invalid length_list or bytes!");

            // 提取当前范围的切片并转换为 Vec<u8>
            let segment = bytes[start..start + length].to_vec();

            // 更新起始位置
            start += length;

            segment
        })
        .collect(); // 收集为 Vec<Vec<u8>>

        let objs: Vec<T> =  byte_vectors
        .par_iter()
        .map(|obj| bincode::deserialize(&obj).expect("Serialization failed"))
        .collect();
        
        objs
    }
    
    pub fn  save(&mut self, obj: T) {
        let index_to_write = {
            let mut length = self.length.lock().unwrap();
            let index = *length;
            *length += 1;
            self.save_length(*length);
            index
        };
        let file_offset = index_to_write as u64* LENGTH_MARKER_SIZE as u64 * 2 + LENGTH_MARKER_SIZE as u64;
        let (start_offset, end_offset) = self.save_dynamic(vec![obj]);
        let mut bytes_offset: Vec<u8> =  start_offset.to_le_bytes().to_vec();
        let bytes_total_length: Vec<u8> = end_offset.to_le_bytes().to_vec();
        bytes_offset.extend(bytes_total_length.iter());
        let file_guard = self.structure_file.lock().unwrap();
        file_guard.write_in_file(file_offset, &bytes_offset);

    }
    
    pub fn load(&self, index: u64) -> T{
    let file_offset   = 2 * LENGTH_MARKER_SIZE as u64* index as u64 + LENGTH_MARKER_SIZE as u64;
    dbg!(&file_offset);
    let file_guard = self.structure_file.lock().unwrap();
    let  marker_data: Vec<u8> = file_guard.read_in_file(file_offset, 2 * LENGTH_MARKER_SIZE);
    assert_eq!(marker_data.len(), 16);
    let start_offset_bytes = &marker_data[0..8];
    let end_offset_bytes  = &marker_data[8..16];
    let start_offset = u64::from_le_bytes(start_offset_bytes.try_into().unwrap()); 
    let end_offset = u64::from_le_bytes(end_offset_bytes.try_into().unwrap());
    dbg!(&start_offset, &end_offset);
    let objs: Vec<T> = self.load_dynamic(start_offset, end_offset);
    
    let obj = objs[0].clone();
    
    obj
    }
    
    pub fn  save_bulk(&mut self, objs: Vec<T>) {
        let index_to_write = {
            let count = objs.len();
            let mut length = self.length.lock().unwrap();
            let index = *length;
            *length += count as i32;
            self.save_length(*length);
            index
        };
        let file_offset = index_to_write as u64* LENGTH_MARKER_SIZE as u64 * 2 + LENGTH_MARKER_SIZE as u64;
let start = Instant::now();
        let start_offset_and_end_offset: Vec<(u64, u64)> = self.save_dynamic_bulk(objs);
        let save_dynamic_duration = start.elapsed();
        println!("save dynamic content  took: {:?}", save_dynamic_duration); 
        let offset_buffer: Vec<u8> = start_offset_and_end_offset.par_iter()
        .map(|obj| {
            let start_offset =obj.0;
            let end_offset = obj.1;            
            let mut bytes_offset: Vec<u8> =  start_offset.to_le_bytes().to_vec();
            let bytes_total_length: Vec<u8> = end_offset.to_le_bytes().to_vec();
            bytes_offset.extend(bytes_total_length.iter());
            bytes_offset
        })
        .flatten()
        .collect::<Vec<u8>>();
        let collect_offsets_duration = start.elapsed();
        println!("collect offsets took: {:?}", collect_offsets_duration - save_dynamic_duration);
        let file_guard = self.structure_file.lock().unwrap();
        file_guard.write_in_file(file_offset, &offset_buffer);
        let write_offsets_duration = start.elapsed();
        println!("write offsets took: {:?}", write_offsets_duration - collect_offsets_duration);

    }
    
    pub fn load_bulk1(&self, index: u64, count: u64) -> Vec<T> {
    let file_offset   = 2 * LENGTH_MARKER_SIZE as u64* index as u64 + LENGTH_MARKER_SIZE as u64;
    let file_guard = self.structure_file.lock().unwrap();
    let  marker_data: Vec<u8> = file_guard.read_in_file(file_offset, 2 * LENGTH_MARKER_SIZE* count as usize);
    dbg!(&marker_data, &file_offset);
    let total_marker_length  = 16 * count;
    dbg!(&total_marker_length);
    assert_eq!(marker_data.len() as u64,total_marker_length );
    let start_offset_bytes = &marker_data[0..8];
    let end_offset_bytes  = &marker_data[(total_marker_length as usize - 8)..(total_marker_length as usize)];
    let start_offset = u64::from_le_bytes(start_offset_bytes.try_into().unwrap()); 
    let end_offset = u64::from_le_bytes(end_offset_bytes.try_into().unwrap());
    dbg!(&start_offset, &end_offset);
    let objs: Vec<T> = self.load_dynamic(start_offset, end_offset);
    
    objs
    }
    
    pub fn load_bulk(&self, index: u64, count: u64) -> Vec<T> {
    let file_offset   = 2 * LENGTH_MARKER_SIZE as u64* index as u64 + LENGTH_MARKER_SIZE as u64;
    let file_guard = self.structure_file.lock().unwrap();
    let  marker_data: Vec<u8> = file_guard.read_in_file(file_offset, 2 * LENGTH_MARKER_SIZE* count as usize);
    dbg!(&marker_data, &file_offset);
    let total_marker_length  = 16 * count;
    dbg!(&total_marker_length);
    assert_eq!(marker_data.len() as u64,total_marker_length );
    let start_offset_bytes = &marker_data[0..8];
    let end_offset_bytes  = &marker_data[(total_marker_length as usize - 8)..(total_marker_length as usize)];
    let start_offset = u64::from_le_bytes(start_offset_bytes.try_into().unwrap()); 
    let end_offset = u64::from_le_bytes(end_offset_bytes.try_into().unwrap());
    dbg!(&start_offset, &end_offset);
    // let objs: Vec<T> = self.load_dynamic(start_offset, end_offset);
    
    let start_offset_and_end_offset_list: Vec<(u64, u64)> =  marker_data
        .chunks_exact(16) // 每次取16字节的切片
        .map(|chunk| {
            // 将前8字节解析为第一个u64
            let part1 = u64::from_le_bytes(chunk[0..8].try_into().expect("Failed to parse u64!"));
            // 将后8字节解析为第二个u64
            let part2 = u64::from_le_bytes(chunk[8..16].try_into().expect("Failed to parse u64!"));
            (part1, part2)
        })
        .collect();
        let objs: Vec<T> = self.load_dynamic_bulk(start_offset_and_end_offset_list);
        
    objs
    }
    

}



# [cfg(test)]
mod test {
    use super:: * ;
    const  COUNT: usize = 1000000;
    
    #[derive(Serialize, Deserialize, Default, Debug, Clone, CheckDynamicSize)]
        pub struct ExampleStruct {
            my_vec: Vec<usize>,
            my_vec1: Vec<usize>,
            my_vec2: Vec<usize>,
        }
        
        

    
    # [test]
    fn test_save_one() {

        let mut my_service = DynamicVectorManageService:: < ExampleStruct > ::new("Dynamic.bin".to_string(), "StringDynamic.bin".to_string(), 1024).unwrap();

        let i = 2;
        let vec_test = vec![i];
        let my_obj = ExampleStruct {
            my_vec: vec_test.clone(),
            my_vec1: vec_test.clone(),
            my_vec2: vec_test.clone(),
        };
        
        println!("size of my obj: {}", size_of_val(&my_obj));  
        // my_service.add(my_obj);
        my_service.save(my_obj);
        
    }
    

                #[test]
    fn test_load_one() {
        let read_service =DynamicVectorManageService:: <ExampleStruct> ::new("Dynamic.bin".to_string(), "StringDynamic.bin".to_string(), 1024).unwrap();
        let result = read_service.load(1);
        println!("read one result:\n {:?}", result);
    }


    
    #[test]
    fn test_save_bulk() {
        // 创建服务实例        
        let mut write_service = DynamicVectorManageService:: <ExampleStruct> ::new("Dynamic.bin".to_string(), "StringDynamic.bin".to_string(), 1024).unwrap();
        let mut objs_list = std::vec::Vec::new();
        for i in 0..COUNT {
            // let i = 1;
let vec_test = vec![i];
        let my_obj = ExampleStruct {
            my_vec: vec_test.clone(),
            my_vec1: vec_test.clone(),
            my_vec2: vec_test.clone(),
        };
        objs_list.push(my_obj.clone());
        // println!("size of ExampleStruct:{}", size_of::<ExampleStruct>());
        }
        write_service.save_bulk(objs_list);
            }
            
            #[test]
    fn test_load_bulk() {
        let mut read_service =DynamicVectorManageService:: <ExampleStruct> ::new("Dynamic.bin".to_string(), "StringDynamic.bin".to_string(), 1024).unwrap();
        let start = Instant::now();
        let objs = read_service.load_bulk(0, COUNT as u64);
        let load_bulk_duration = start.elapsed(); 
        println!("load bulk    took: {:?}", load_bulk_duration);
        println!("last read obj: {:?}\n total count {}", objs[objs.len() -1 ], objs.len());
    }
    
    #[test]
    fn test_io_dynamic_bulk() {
        // 创建服务实例        
        let mut io_service = DynamicVectorManageService:: <ExampleStruct> ::new("Dynamic.bin".to_string(), "StringDynamic.bin".to_string(), 1024).unwrap();

        let mut objs_list = std::vec::Vec::new();
        for i in 0..COUNT {
        // 示例添加对象
        let i =1;
        let vec_test = vec![i];
        let my_obj = ExampleStruct {
            my_vec: vec_test.clone(),
            my_vec1: vec_test.clone(),
            my_vec2: vec_test.clone(),
        };
        
        objs_list.push(my_obj.clone());
        // println!("size of ExampleStruct:{}", size_of::<ExampleStruct>());
        }
        io_service.save_bulk(objs_list);
        let objs = io_service.load_bulk(0, COUNT  as u64);
        assert_eq!(objs.len(), COUNT);
// assert_eq!(objs[COUNT - 1].my_number as usize, COUNT - 1);
    }
    
    #[test]
    fn test_get_dynamic_length() {
        let read_service =DynamicVectorManageService:: <ExampleStruct> ::new("Dynamic.bin".to_string(), "StringDynamic.bin".to_string(), 1024).unwrap();
        let length = read_service.get_length();
        println!("length: {}", length);
    }
    
    }

