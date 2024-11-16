use rayon::prelude::*;
pub use serde::{Serialize, Deserialize};
use std::io::{self};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::mem::size_of;
use std::marker::PhantomData;






use crate::file_access_service::FileAccessService;
use crate::string_repository::StringRepository; 


const LENGTH_MARKER_SIZE: usize = size_of::<u64>(); // We reserve the first 8 bytes for Length


pub struct DynamicVectorManageService<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Default+Send,
{
    length: Arc<Mutex<u64>>, 
    // structure_file: Mutex<CachedFileAccessService>, // 结构文件的文件句柄
    structure_file: Mutex<FileAccessService>, 
    string_repository: StringRepository,
    _marker: PhantomData<T>, // 用于存储对象类型的占位符
}


impl<T: Send+Sync> DynamicVectorManageService<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Default + 'static +std::fmt::Debug + Clone,
{
    pub fn new(structure_file_path: String, string_file_path: String, initial_size_if_not_exists: u64) -> io::Result<Self> {
        // let structure_file_access = CachedFileAccessService::new(structure_file_path, initial_size_if_not_exists);
        let structure_file_access = FileAccessService::new(structure_file_path, initial_size_if_not_exists);
        let string_repository = StringRepository::new(string_file_path.clone(), initial_size_if_not_exists);
        let length = {
            let buffer = structure_file_access.read_in_file(0, LENGTH_MARKER_SIZE);
            // 确保 buffer 的长度至少为 8
            assert!(buffer.len() >= 8, "Buffer length must be at least 4 bytes.");

    // 将前       8 个字节转换为 u64，使用小端字节序
            let length = u64::from_le_bytes(buffer[0..8].try_into().unwrap());

            Arc::new(Mutex::new(length))
        };
        Ok(Self {
            length,
            structure_file: Mutex::new(structure_file_access),
            string_repository: string_repository,
            _marker: PhantomData,
        })
    }
    

    pub fn get_length(&self) -> u64 {
        let structure_file_guard =self.structure_file.lock().unwrap(); 
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

   
    fn save_dynamic(&mut self, obj: T) -> (u64, u64) {
        let bytes = bincode::serialize(&obj).expect("Serialization failed");
        let (start_offset, end_offset) = self.string_repository.write_string_content_and_get_offset(bytes);
        
        (start_offset, end_offset)
    }
    
    fn _save_dynamic_bulk1(&mut self, objs: Vec<T>) -> Vec<(u64, u64)> {
        let start = Instant::now();
        
        let bytes: Vec<u8> = objs.par_iter()
        .map(|obj| bincode::serialize(&obj).expect("Serialization failed " ) )
        .flatten()
        // .cloned()
        .collect();
        
        let serialize_duration = start.elapsed();
        println!("serialization  took: {:?}", serialize_duration);
        
        let length_list: Vec<u64> = objs.par_iter()
        .map( |obj| bincode::serialize(&obj).expect("Serialization failed") )        .map(|obj| obj.len() as u64)
        .collect();
        
        let collect_length_list_duration = start.elapsed();
        println!("collect length list took: {:?}", collect_length_list_duration - serialize_duration);
        
        let (start_offset, _) = self.string_repository.write_string_content_and_get_offset(bytes);
        let write_vector_content_duration  =  start.elapsed();
        println!("save vector content  took: {:?}", write_vector_content_duration - collect_length_list_duration);
        
        let offsets_list =  length_list
        .into_iter()
        .scan(start_offset, |current_offset, length| {
            let start = *current_offset;        // 当前对象的起点
            let end = start + length;          // 当前对象的终点
            *current_offset = end;             // 更新累加器为下一个对象的起点
            Some((start, end))                 // 返回当前区间
        })
        .collect::<Vec<(u64, u64)>>();
        
        let collect_offsets_duration = start.elapsed();
        println!("collect offsets took: {:?}", collect_offsets_duration - write_vector_content_duration); 
        
        offsets_list
            }
            
            pub fn save_dynamic_bulk(&mut self, objs: Vec<T>) -> Vec<(u64, u64)> {
        let start = Instant::now();
        let bytes_list: Vec<Vec<u8>>  = objs.par_iter()
        .map(|obj| bincode::serialize(&obj).expect("Serialization failed") )
        .collect::<Vec<Vec<u8>>>();
        
        let bytes: Vec<u8> = bytes_list.par_iter().flatten().cloned().collect();
        let serialize_duration = start.elapsed();
        println!("serialization  took: {:?}", serialize_duration);

        let length_list: Vec<u64> = bytes_list.par_iter()
        .map(|obj| obj.len() as u64)
        .collect();
        
        let collect_length_list_duration = start.elapsed();
        println!("collect length list took: {:?}", collect_length_list_duration - serialize_duration);
        
        let (start_offset, _) = self.string_repository.write_string_content_and_get_offset(bytes);
        
        let write_vector_content_duration  =  start.elapsed();
        println!("save vector content  took: {:?}", write_vector_content_duration - collect_length_list_duration);
        
        let offsets_list = length_list
        .into_iter()
        .scan(start_offset, |current_offset, length| {
            let start = *current_offset;        // 当前对象的起点
            let end = start + length;          // 当前对象的终点
            *current_offset = end;             // 更新累加器为下一个对象的起点
            Some((start, end)) 
            
        })
        .collect::<Vec<(u64, u64)>>();
        
            let collect_offsets_duration = start.elapsed();
        println!("collect offsets took: {:?}", collect_offsets_duration - write_vector_content_duration); 
        offsets_list
            }
    
    fn load_dynamic(&self, start_offset: u64, end_offset: u64) -> T {
        let bytes: Vec<u8> = self.string_repository.load_string_content(start_offset, end_offset);
        let obj: T =  bincode::deserialize(&bytes).expect("deserialization failed");
        obj
    }
    
    pub fn load_dynamic_bulk(&self, start_offset_and_end_offset_list: Vec<(u64, u64)>) -> Vec<T> {
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
        let (start_offset, end_offset) = self.save_dynamic(obj);
        let mut bytes_offset: Vec<u8> =  start_offset.to_le_bytes().to_vec();
        let bytes_total_length: Vec<u8> = end_offset.to_le_bytes().to_vec();
        bytes_offset.extend(bytes_total_length.iter());
        let file_guard = self.structure_file.lock().unwrap();
        file_guard.write_in_file(file_offset, &bytes_offset);

    }
    
    pub fn load(&self, index: u64) -> T{
    let file_offset   = 2 * LENGTH_MARKER_SIZE as u64* index as u64 + LENGTH_MARKER_SIZE as u64;
    // dbg!(&file_offset);
    let file_guard = self.structure_file.lock().unwrap();
    let  marker_data: Vec<u8> = file_guard.read_in_file(file_offset, 2 * LENGTH_MARKER_SIZE);
    assert_eq!(marker_data.len(), 16);
    let start_offset_bytes = &marker_data[0..8];
    let end_offset_bytes  = &marker_data[8..16];
    let start_offset = u64::from_le_bytes(start_offset_bytes.try_into().unwrap()); 
    let end_offset = u64::from_le_bytes(end_offset_bytes.try_into().unwrap());
    // dbg!(&start_offset, &end_offset);
    let obj: T = self.load_dynamic(start_offset, end_offset);
    
    // let obj = objs[0].clone();
    
    obj
    }
    
    pub fn  save_bulk(&mut self, objs: Vec<T>) {
        let index_to_write = {
            let count = objs.len();
            let mut length = self.length.lock().unwrap();
            let index = *length;
            *length += count as u64;
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
    
    
    pub fn load_bulk(&self, index: u64, count: u64) -> Vec<T> {
    let file_offset   = 2 * LENGTH_MARKER_SIZE as u64* index as u64 + LENGTH_MARKER_SIZE as u64;
    let file_guard = self.structure_file.lock().unwrap();
    let  marker_data: Vec<u8> = file_guard.read_in_file(file_offset, 2 * LENGTH_MARKER_SIZE* count as usize);
    // dbg!(&marker_data, &file_offset);
    let total_marker_length  = 16 * count;
    // dbg!(&total_marker_length);
    assert_eq!(marker_data.len() as u64,total_marker_length );
    // let start_offset_bytes = &marker_data[0..8];
    // let end_offset_bytes  = &marker_data[(total_marker_length as usize - 8)..(total_marker_length as usize)];
    // let start_offset = u64::from_le_bytes(start_offset_bytes.try_into().unwrap()); 
    // let end_offset = u64::from_le_bytes(end_offset_bytes.try_into().unwrap());
    // dbg!(&start_offset, &end_offset);
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
    use dynamic_vector::CheckDynamicSize;
    use dynamic_vector::VectorCandidate;
    
    const  COUNT: usize = 1000000;
    
    #[derive(Serialize, Deserialize, Default, Debug, Clone)]
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
        let read_service =DynamicVectorManageService:: <ExampleStruct> ::new("Dynamic.bin".to_string(), "StringDynamic.bin".to_string(), 1024).unwrap();
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

        let mut vec_test = vec![i];
        let my_obj = ExampleStruct {
            my_vec: vec_test.clone(),
            my_vec1: vec_test.clone(),
            my_vec2: vec_test.clone(),
        };
        
        vec_test.push(i +1);
        objs_list.push(my_obj.clone());
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

