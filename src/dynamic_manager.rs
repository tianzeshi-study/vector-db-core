use tokio::task;
use rayon::prelude::*;
pub use serde::{Serialize, Deserialize};
use serde_json::Value;
use std::future;
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


use crate::cached_file_access_service::CachedFileAccessService;
use crate::string_repository::StringRepository; 


const LENGTH_MARKER_SIZE: usize = size_of::<u64>(); // We reserve the first 8 bytes for Length


pub struct ObjectPersistOnDiskService<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Default+Send,
{
    length: Arc<Mutex<i32>>, 
    structure_file: Mutex<CachedFileAccessService>, // 结构文件的文件句柄
    string_repository: StringRepository,
    dynamic_repository_dir:  String,
    initial_size_if_not_exists: u64,
    _marker: PhantomData<T>, // 用于存储对象类型的占位符
}


impl<T: Send+Sync> ObjectPersistOnDiskService<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Default + 'static+ DynamicVector +std::fmt::Debug + Clone,
{
    pub fn new(structure_file_path: String, string_file_path: String, initial_size_if_not_exists: u64) -> io::Result<Self> {
        let structure_file_access = CachedFileAccessService::new(structure_file_path, initial_size_if_not_exists);
        // std::fs::create_dir_all(&string_file_path);
        let string_repository = StringRepository::new(string_file_path.clone(), initial_size_if_not_exists);
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

    fn serialize_object(obj: &T) -> Vec<u8> {
        bincode::serialize(obj).expect("Serialization failed")
    }


    fn deserialize_object(data: &[u8]) -> T{
        // println!("length of data  to deserialize {:?}", data.len());
        // bincode::deserialize(data).expect("Deserialization failed")
        // bincode::deserialize(data).unwrap_or_else(|e| {
            // eprintln!("Deserialization failed: {}", e);
            // panic!("Deserialization error")
        // })
        bincode::deserialize(data).expect("Deserialization failed")
    }


    fn write_index(&self, index: i64, obj: T) {

        if !obj.is_dynamic_structure() {
        let size_of_object = get_item_size::<T>();
        let data = Self::serialize_object(&obj);

        println!("bytes length of data to write once: {} ", &data.len());


        let offset   = (size_of_object * index as usize + LENGTH_MARKER_SIZE as usize) as u64;

        // file.seek(SeekFrom::Start(offset)).expect("Seek failed.");
        // file.write_all(&data).expect("Write failed.");
        let file = self.structure_file.lock().unwrap();
        file.write_in_file(offset, &data);
        } else {
            let size_of_object = get_item_size::<T>();

            let objs_with_persisted_dynamic  = self.save_object_dynamic(vec![obj]);
            // dbg!(&objs_with_persisted_dynamic);
            
            let size_of_value_object = get_item_size::<Value>();
            // dbg!(size_of_value_object);
            let dyna_obj:T =serde_json::from_value(objs_with_persisted_dynamic[0].clone()).unwrap();
            let data = bincode::serialize(&dyna_obj).expect("Serialization failed");
            // let data = Self::serialize_object(&obj_with_persisted_dynamic[0]);
            // let data = bincode::serialize(&obj_with_persisted_dynamic).expect("Serialization failed");
            // let data: Vec<u8> = serde_json::to_vec(&obj_with_persisted_dynamic).unwrap();
            println!("bytes length of dynamic data to write once: {} ", &data.len());

            let offset   = (size_of_object * index as usize + LENGTH_MARKER_SIZE as usize) as u64;

            let file_guard = self.structure_file.lock().unwrap();
                        file_guard.write_in_file(offset, &data);
        }
    }
    
    
    fn bulk_write_index(&self, index: i64, objs: Vec<T>) {
        if !objs[0].is_dynamic_structure() {
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


        } else {
            let size_of_object = get_item_size::<T>();
            let offset   = (size_of_object * index as usize + LENGTH_MARKER_SIZE as usize) as u64;
            let count  = objs.len();

let start = Instant::now(); // 记录开始时间
            // let objs_with_persisted_dynamic: Vec<Value>  = self.save_object_dynamic(objs);
            let objs_with_persisted_dynamic: Vec<Value>  = self.run_async_save_object_dynamic(objs);
            
    let save_string_duration = start.elapsed(); 
    println!("save object dynamic  took: {:?}", save_string_duration);
    
    

let dyna_objs:Vec<T> =objs_with_persisted_dynamic.par_iter()
.map(|obj| serde_json::from_value(obj.clone()).unwrap())
.collect();

let duration_deserialize_value   = start.elapsed();
println!("value deserialize  duration  dynamic  took: {:?}", duration_deserialize_value -save_string_duration);




            let mut buffer: Vec<u8> = vec![0; size_of_object * count];
            println!("length of buffer: {}", buffer.len());
        

let serialized_objs: Vec<Vec<u8>> = dyna_objs.par_iter()
        // .map(|obj| Self::serialize_object(obj))
        .map(|dyna_obj| bincode::serialize(&dyna_obj).expect("Serialization failed")) 
        .collect::<Vec<Vec<u8>>>();
let duration_deserialize_bin   = start.elapsed();
println!("bin deserialize  duration  dynamic  took: {:?}", duration_deserialize_bin -duration_deserialize_value);



    let mut current_position = 0;
    for serialized_obj in &serialized_objs {

        let serialized_size = serialized_obj.len();
        let white_space = size_of_object -  serialized_size;
        // println!("white_space: {}", white_space);
        // 将序列化对象写入缓冲区
        buffer[current_position..current_position + serialized_size].copy_from_slice(&serialized_obj);
        current_position += white_space;
        
        current_position += serialized_size;
    }
    
        // let offset   = (size_of_object * index as usize + LENGTH_MARKER_SIZE as usize) as u64;
        println!("bulk write offset: {}",  offset);

            let file_guard = self.structure_file.lock().unwrap();
                        file_guard.write_in_file(offset, &buffer);
                        let duration_write   = start.elapsed();
println!("write   duration  dynamic  took: {:?}", duration_write - duration_deserialize_bin );


        }

    }



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
    
    pub fn read(&self, index: usize) -> T{
        let size_of_object = get_item_size::<T>();
        let offset   = (size_of_object * index as usize + LENGTH_MARKER_SIZE) as u64;
        // let offset   = 0;
        println!("read offset:{}", offset);

        let length = size_of_object;
        // let length = 36;
        println!("read length:{}", length);
        let file_guard = self.structure_file.lock().unwrap();
        let  data: Vec<u8> = file_guard.read_in_file(offset, length);
        dbg!(&data.len());
        // let obj = Self::deserialize_object(&data);
        let obj: T = bincode::deserialize(&data).expect("Deserialization failed");
        println!("{:?}", obj);
        if !obj.is_dynamic_structure() {
            return obj;
        } else {

            // let obj: Vec<Value> = serde_json::from_slice(&data);
            // let value_obj = serde_json::from_slice(&data).unwrap();
            // let decoded_objs: Vec<Value> = self.load_object_dynamic(value_obj);
            let decoded_objs: Vec<Value> = self.load_object_dynamic(vec![obj]);
            let decoded_obj = decoded_objs[0].clone();
            let result_obj = serde_json::from_value(decoded_obj).unwrap();

            return result_obj;
        }

// obj
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
        
        if !objs[0].is_dynamic_structure() {
        return objs;
        } else {
            let decoded_objs: Vec<Value> = self.load_object_dynamic(objs);

            let result_objs: Vec<T> = decoded_objs.par_iter()
            .map(|obj| serde_json::from_value(obj.clone()).unwrap())
            .collect();

            return result_objs;
        }
    }
    

   
    fn save_object_dynamic(&self, objs: Vec<T>) -> Vec<Value>{
        let  dynamic_fields: Vec<String> =  objs[0].get_dynamic_fields();
        let objs_len = objs.len();
        let  dynamic_fields_count = dynamic_fields.len();
        // println!("dynamic_fields: {:?}, dynamic_fields_count: {}", dynamic_fields, dynamic_fields_count);

        let total_dynamic_fields_count = dynamic_fields_count * objs.len();
        let mut dynamic_bytes:Vec<u8> = vec![0;total_dynamic_fields_count];
        
        let start = Instant::now(); // 记录开始时间
        // let  mut json_objs = &mut objs;
let mut json_objs: Vec<_> = objs.par_iter()
     .map( |obj| serde_json::to_value(obj).unwrap())
     .collect();
     // println!("json_objs, {:?}", json_objs);
    let to_json_value_duration = start.elapsed(); 
    println!("to_json_value_duration duration   took: {:?}", to_json_value_duration);
    let json_objs_to_read = json_objs.clone();

     let mut arc_objs = Arc::new(Mutex::new(json_objs));
     let mut arc_objs_clone = Arc::clone(&arc_objs);
     let arc_objs_value  = Arc::clone(&arc_objs); 
    
        dynamic_fields.par_iter()
        .for_each(|field| {
            // let arc_objs_clone = Arc::clone(&arc_objs);
let json_objs_to_read = json_objs_to_read.clone();

        let field_obj_len_list = Arc::new(RwLock::new(vec![0; objs_len]));
        let field_obj_len_list_clone = Arc::clone(&field_obj_len_list);
       // let arc_objs_value  = Arc::clone(&arc_objs); 
    // let byte_vector: Vec<u8> = arc_objs_value.lock().unwrap()
    let byte_vector: Vec<u8> = json_objs_to_read
     .par_iter()
     // .iter()
     .map(|obj| {
         // dbg!(obj);
     obj.get(field.clone()).unwrap()
     })
     // .map(move |obj| obj.to_string().as_bytes().to_vec())
     // .map( |obj| serde_json::from_value::<Vec<u8>>(obj.clone()).unwrap() )
     .map( |obj| serde_json::from_value::<Vec<usize>>(obj.clone()).unwrap())
     .map(|obj| {
     obj.par_iter().map(|o| *o as u8).collect::<Vec<u8>>()
     }) 
     .enumerate()
     .map(|(i, obj)|  {
                 // dbg!(&field, &obj.len());
                 let field_obj_len_list_write_clone = Arc::clone(&field_obj_len_list);
                 let mut list = field_obj_len_list_write_clone.write().unwrap(); 
                 list[i] = obj.len(); 
         obj
     })
     .flatten()
     .collect::<Vec<u8>>();
     // dbg!(&byte_vector);
     let to_string_duration = start.elapsed(); 
    println!("to string   took: {:?}", to_string_duration -to_json_value_duration);
     

    let file_path = std::path::Path::new(&self.dynamic_repository_dir).join(field.clone());
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .open(file_path.clone());

let file_path_str = file_path.to_string_lossy().into_owned();
let mut  string_repository = StringRepository::new(file_path_str, self.initial_size_if_not_exists.clone());

let (offset, end_offset ) = string_repository.write_string_content_and_get_offset(byte_vector);
 let mut total_length_list: Vec<u64> =  Vec::new();
    let mut current_offset = offset;
let write_string_duration = start.elapsed(); 
    println!("write_string_duration    took: {:?}", write_string_duration - to_string_duration);
    
// objs_iter.par_iter().for_each(|i| {
for i in 0..objs_len {
    if let Some(dynamic_obj_value) = arc_objs_clone.lock().unwrap()[i].get_mut(field.clone()) {
        let field_obj_len_list_read_clone = Arc::clone(&field_obj_len_list);
        let list_to_read = field_obj_len_list_read_clone.read().unwrap();
        // let offset_and_total_length = vec![current_offset, current_offset + field_obj_len_list[i] as u64];
        let offset_and_total_length = vec![current_offset, current_offset + list_to_read[i] as u64];
        // dbg!(&field_obj_len_list, &field_obj_len_list[i]);
        let mut bytes_offset: Vec<u8> =  current_offset.to_le_bytes().to_vec();
        // let bytes_total_length: Vec<u8> = (current_offset + field_obj_len_list[i] as u64).to_le_bytes().to_vec();
        let bytes_total_length: Vec<u8> = (current_offset + list_to_read[i] as u64).to_le_bytes().to_vec();
         bytes_offset.extend(bytes_total_length.iter());
        // println!("offset_and_total_length: {:?}", &offset_and_total_length);
        *dynamic_obj_value = offset_and_total_length.into();
        // dbg!(&dynamic_obj_value);
        // current_offset +=  field_obj_len_list[i] as u64;
        current_offset +=  list_to_read[i] as u64;
    } else {
        println!("no value!");
    }

}

let pack_offset_duration = start.elapsed(); 
    println!("pack offset     took: {:?}", pack_offset_duration - write_string_duration );
    println!("this thread      took: {:?}", pack_offset_duration);


});

// let arc_objs_final_clone = Arc::clone(&arc_objs);
// let obj_with_persisted_dynamic = arc_objs_final_clone.lock().unwrap().to_vec();
let obj_with_persisted_dynamic = arc_objs_clone.lock().unwrap().to_vec();
// dbg!(&obj_with_persisted_dynamic);
obj_with_persisted_dynamic
    }
    



async fn async_save_object_dynamic(&self, objs: Vec<T>) -> Vec<Value> {
    let dynamic_fields: Vec<String> = objs[0].get_dynamic_fields();
    let objs_len = objs.len();
    let dynamic_fields_count = dynamic_fields.len();
    let total_dynamic_fields_count = dynamic_fields_count * objs.len();
    let mut dynamic_bytes: Vec<u8> = vec![0; total_dynamic_fields_count];
    let start = Instant::now(); // 记录开始时间

    let mut json_objs: Vec<_> = objs
        .par_iter()
        .map(|obj| serde_json::to_value(obj).unwrap())
        .collect();

    let to_json_value_duration = start.elapsed();
    println!("to_json_value_duration duration took: {:?}", to_json_value_duration);

    let json_objs_to_read = json_objs.clone();
    let arc_objs = Arc::new(tokio::sync::Mutex::new(json_objs));

    let dynamic_fields_futures = dynamic_fields.par_iter().map(|field| {
    // let dynamic_fields_futures = dynamic_fields.into_iter().map(|field| {
        let arc_objs_clone = Arc::clone(&arc_objs);
        let json_objs_to_read = json_objs_to_read.clone();
        let dynamic_repository_dir = self.dynamic_repository_dir.clone();
let initial_size_if_not_exists         = self.initial_size_if_not_exists.clone();

        async move {

            let field_obj_len_list = Arc::new(tokio::sync::RwLock::new(vec![0; objs_len]));
            
            // let futures: Vec<u8> = json_objs_to_read
            // let byte_vector: Vec<u8> = task::spawn_blocking(move || { json_objs_to_read
            let byte_vector: Vec<u8> = json_objs_to_read
                .par_iter()
                // .iter()
                .map(|obj| serde_json::from_value::<Vec<usize>>(obj.get(&field).unwrap().clone()).unwrap())
                .map(|obj| obj.iter().map(|o| *o as u8).collect::<Vec<u8>>())
                .enumerate()
                .map(|(i, obj)| {
                    
                    let field_obj_len_list_write = Arc::clone(&field_obj_len_list);
                    async { 
                    let mut list = field_obj_len_list_write.write().await;

                    list[i] = obj.len();
                    };
                    obj
                })
                .flatten()
                .collect();
                
     let to_string_duration = start.elapsed(); 
    println!("to string   took: {:?}", to_string_duration -to_json_value_duration);
    
// let byte_vector = futures::future::join_all(futures).await;



            let file_path = std::path::Path::new(&dynamic_repository_dir).join(&field);

                let mut string_repository = StringRepository::new(file_path.to_string_lossy().into_owned(), initial_size_if_not_exists.clone());


                let start_writeing = Instant::now(); // 记录开始时间
                let (offset, end_offset) = string_repository.write_string_content_and_get_offset(byte_vector.clone());
let direct_write_string_duration = start_writeing.elapsed();
    println!("direct_write_string_duration  took: {:?}", direct_write_string_duration);



let write_string_duration = start.elapsed(); 
    println!("write_string_duration    took: {:?}", write_string_duration - to_string_duration);
    

            let mut current_offset = offset;
            for i in 0..objs_len {
                if let Some(dynamic_obj_value) = arc_objs_clone.lock().await[i].get_mut(&field) {
                // if let Some(dynamic_obj_value) = arc_objs_clone.lock().unwrap()[i].get_mut(&field) {
                    let list_to_read = field_obj_len_list.read().await;
                    let offset_and_total_length = vec![current_offset, current_offset + list_to_read[i] as u64];
                    *dynamic_obj_value = offset_and_total_length.into();
                    current_offset += list_to_read[i] as u64;
                }
            }
            
            let pack_offset_duration = start.elapsed(); 
    println!("pack offset     took: {:?}", pack_offset_duration - write_string_duration );
    println!("this thread      took: {:?}", pack_offset_duration);
    
        }
    })
    .collect::<Vec<_>>();

    // 运行所有字段的处理任务
    let _ = futures::future::join_all(dynamic_fields_futures).await;

    let obj_with_persisted_dynamic = arc_objs.lock().await.clone();
    obj_with_persisted_dynamic
}





fn run_async_save_object_dynamic(&self, objs: Vec<T>)-> Vec<Value> {
    // let runtime = tokio::runtime::Runtime::new().unwrap();
    
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()  // 启用所有功能
        .build()
        .unwrap();

    runtime.block_on(async {
        // let objs = vec![/*... 初始化对象数组 ...*/];
        let result = self.async_save_object_dynamic(objs).await;
        // println!("Result: {:?}", result);
        result
    })
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
        // let (start_offset, end_offset) = self.save_dynamic(objs);
        let start_offset_and_end_offset: Vec<(u64, u64)> = self.save_dynamic_bulk(objs);
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
        let file_guard = self.structure_file.lock().unwrap();
        file_guard.write_in_file(file_offset, &offset_buffer);

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


fn get_item_size<T>() -> usize {
    size_of::<T>()
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
    fn test_write_one() {
        // 创建服务实例        
        let my_service = ObjectPersistOnDiskService:: < ExampleStruct > ::new("Dynamic.bin".to_string(), "StringDynamic.bin".to_string(), 1024).unwrap();
        // 示例添加对象
        let i = 1;
        let vec_test = vec![i];
        let my_obj = ExampleStruct {
            my_vec: vec_test.clone(),
            my_vec1: vec_test.clone(),
            my_vec2: vec_test.clone(),
        };
        my_service.add(my_obj);
    }
    
    # [test]
    fn test_save_one() {

        let mut my_service = ObjectPersistOnDiskService:: < ExampleStruct > ::new("Dynamic.bin".to_string(), "StringDynamic.bin".to_string(), 1024).unwrap();

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
        let read_service =ObjectPersistOnDiskService:: <ExampleStruct> ::new("Dynamic.bin".to_string(), "StringDynamic.bin".to_string(), 1024).unwrap();
        let result = read_service.load(1);
        println!("read one result:\n {:?}", result);
    }


    # [test]
    fn test_string_write_bulk() {
        // 创建服务实例        
        let my_service = ObjectPersistOnDiskService:: <ExampleStruct> ::new("Dynamic.bin".to_string(), "StringDynamic.bin".to_string(), 1024).unwrap();
        let mut objs_list = std::vec::Vec::new();
        for i in 0..COUNT {
        // 示例添加对象
        let vec_test = vec![i];
        let my_obj = ExampleStruct {
            my_vec: vec_test.clone(),
            my_vec1: vec_test.clone(),
            my_vec2: vec_test.clone(),
        };
        objs_list.push(my_obj);
        }
        my_service.add_bulk(objs_list);
    }

    # [test]
    fn test_size_of_struct() {
        let my_read_service = ObjectPersistOnDiskService:: <ExampleStruct> ::new("Dynamic.bin".to_string(), "StringDynamic.bin".to_string(), 1024).unwrap();
        let mut objs_list = std::vec::Vec::new();
        for i in 1..COUNT {
        // 示例添加对象
        let vec_test = vec![i];
        let my_obj = ExampleStruct {
            my_vec: vec_test.clone(),
            my_vec1: vec_test.clone(),
            my_vec2: vec_test.clone(),
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
        let objs = my_read_service.read(1);
        println!("{:?}", objs);
        dbg!(my_structure_size, my_vec_structure_size, my_str_size, my_string_size, my_char_size, my_bool_size, objs);
        // assert!(false);
    }
    
    #[test]
    fn test_save_bulk() {
        // 创建服务实例        
        let mut write_service = ObjectPersistOnDiskService:: <ExampleStruct> ::new("Dynamic.bin".to_string(), "StringDynamic.bin".to_string(), 1024).unwrap();
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
        let mut read_service =ObjectPersistOnDiskService:: <ExampleStruct> ::new("Dynamic.bin".to_string(), "StringDynamic.bin".to_string(), 1024).unwrap();
        let objs = read_service.load_bulk(0, COUNT as u64);
        println!("last read obj: {:?}\n total count {}", objs[objs.len() -1 ], objs.len());
    }
    
    #[test]
    fn test_io_dynamic_bulk() {
        // 创建服务实例        
        let io_service = ObjectPersistOnDiskService:: <ExampleStruct> ::new("Dynamic.bin".to_string(), "StringDynamic.bin".to_string(), 1024).unwrap();

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
        io_service.add_bulk(objs_list);
        let objs = io_service.read_bulk(0, COUNT );
        assert_eq!(objs.len(), COUNT);
// assert_eq!(objs[COUNT - 1].my_number as usize, COUNT - 1);
    }
    
    #[test]
    fn test_get_dynamic_length() {
        let read_service =ObjectPersistOnDiskService:: <ExampleStruct> ::new("Dynamic.bin".to_string(), "StringDynamic.bin".to_string(), 1024).unwrap();
        let length = read_service.get_length();
        println!("length: {}", length);
    }
    
    }

