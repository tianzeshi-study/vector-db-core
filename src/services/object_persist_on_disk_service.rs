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

pub use dynamic_vector::{
    CheckDynamicSize,
    DynamicVector,
    VectorCandidate,
};

use crate::services::{
    file_access_service::FileAccessService,
    string_repository::StringRepository,
};


const LENGTH_MARKER_SIZE: usize = size_of::<u64>(); 



pub struct ObjectPersistOnDiskService<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Default + Send,
{
    length: Arc<Mutex<u64>>,
    structure_file: Mutex<FileAccessService>, 
    
    dynamic_repository_dir: String,
    initial_size_if_not_exists: u64,
    _marker: PhantomData<T>, 
}

impl<T: Send + Sync> ObjectPersistOnDiskService<T>
where
    
    T: Serialize
        + for<'de> Deserialize<'de>
        + Default
        + 'static
        + DynamicVector
        + std::fmt::Debug,
{
    pub fn new(
        structure_file_path: String,
        string_file_path: String,
        initial_size_if_not_exists: u64,
    ) -> io::Result<Self> {
        let structure_file_access =
            FileAccessService::new(structure_file_path, initial_size_if_not_exists);
        let _ = std::fs::create_dir_all(&string_file_path);
        
        
        let length = {
            let buffer = structure_file_access.read_in_file(0, LENGTH_MARKER_SIZE);
            
            assert!(buffer.len() >= 4, "Buffer length must be at least 4 bytes.");

            
            let length = u64::from_le_bytes(buffer[0..8].try_into().unwrap());

            Arc::new(Mutex::new(length))
        };
        Ok(Self {
            length,
            structure_file: Mutex::new(structure_file_access),
            
            dynamic_repository_dir: string_file_path,
            initial_size_if_not_exists,
            _marker: PhantomData,
        })
    }

    
    pub fn get_length(&self) -> u64 {
        let structure_file_guard = self.structure_file.lock().unwrap();
        let buffer = structure_file_guard.read_in_file(0, LENGTH_MARKER_SIZE);
        
        assert!(buffer.len() >= 4, "Buffer length must be at least 4 bytes.");

        
        
        let length = u64::from_le_bytes(buffer[0..8].try_into().unwrap());

        
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






        bincode::deserialize(data).expect("Deserialization failed")
    }


    fn write_index(&self, index: u64, obj: T) {
        if !obj.is_dynamic_structure() {
            let size_of_object = get_item_size::<T>();
            let data = Self::serialize_object(&obj);

            println!("bytes length of data to write once: {} ", &data.len());

            let offset =
                (size_of_object * index as usize + LENGTH_MARKER_SIZE as usize) as u64;



            let file = self.structure_file.lock().unwrap();
            file.write_in_file(offset, &data);
        } else {
            let size_of_object = get_item_size::<T>();

            let objs_with_persisted_dynamic = self.save_object_dynamic(vec![obj]);




            let dyna_obj: T =
                serde_json::from_value(objs_with_persisted_dynamic[0].clone()).unwrap();
            let data = bincode::serialize(&dyna_obj).expect("Serialization failed");



            println!(
                "bytes length of dynamic data to write once: {} ",
                &data.len()
            );

            let offset =
                (size_of_object * index as usize + LENGTH_MARKER_SIZE as usize) as u64;

            let file_guard = self.structure_file.lock().unwrap();
            file_guard.write_in_file(offset, &data);
        }
    }

    fn bulk_write_index(&self, index: u64, objs: Vec<T>) {
        if !objs[0].is_dynamic_structure() {
            let size_of_object = get_item_size::<T>();
            let count = objs.len();
            let mut buffer: Vec<u8> = vec![0; size_of_object * count];
            println!("length of buffer: {}", buffer.len());






            let serialized_objs: Vec<Vec<u8>> = objs
                .par_iter()
                .map(|obj| Self::serialize_object(obj))
                .collect::<Vec<Vec<u8>>>();

            let mut current_position = 0;
            for serialized_obj in &serialized_objs {


                let serialized_size = serialized_obj.len();
                let white_space = size_of_object - serialized_size;
                println!("white_space: {}", white_space);

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



            let offset =
                (size_of_object * index as usize + LENGTH_MARKER_SIZE as usize) as u64;
            println!("bulk write offset: {}", offset);


            let file_guard = self.structure_file.lock().unwrap();



            file_guard.write_in_file(offset, &buffer);
        } else {
            let size_of_object = get_item_size::<T>();
            let offset =
                (size_of_object * index as usize + LENGTH_MARKER_SIZE as usize) as u64;
            let count = objs.len();

            let start = Instant::now(); 
            let objs_with_persisted_dynamic: Vec<Value> = self.save_object_dynamic(objs);
            

            let save_string_duration = start.elapsed();
            println!("save object dynamic  took: {:?}", save_string_duration);

            let dyna_objs: Vec<T> = objs_with_persisted_dynamic
                .par_iter()
                .map(|obj| serde_json::from_value(obj.clone()).unwrap())
                .collect();

            let duration_deserialize_value = start.elapsed();
            println!(
                "value deserialize  duration  dynamic  took: {:?}",
                duration_deserialize_value - save_string_duration
            );

            let mut buffer: Vec<u8> = vec![0; size_of_object * count];
            println!("length of buffer: {}", buffer.len());

            let serialized_objs: Vec<Vec<u8>> = dyna_objs
                .par_iter()
                
                .map(|dyna_obj| bincode::serialize(&dyna_obj).expect("Serialization failed"))
                .collect::<Vec<Vec<u8>>>();
            let duration_deserialize_bin = start.elapsed();
            println!(
                "bin deserialize  duration  dynamic  took: {:?}",
                duration_deserialize_bin - duration_deserialize_value
            );

            let mut current_position = 0;
            for serialized_obj in &serialized_objs {
                let serialized_size = serialized_obj.len();
                let white_space = size_of_object - serialized_size;
                
                
                buffer[current_position..current_position + serialized_size]
                    .copy_from_slice(&serialized_obj);
                current_position += white_space;

                current_position += serialized_size;
            }

            
            println!("bulk write offset: {}", offset);

            let file_guard = self.structure_file.lock().unwrap();
            file_guard.write_in_file(offset, &buffer);
            let duration_write = start.elapsed();
            println!(
                "write   duration  dynamic  took: {:?}",
                duration_write - duration_deserialize_bin
            );
        }
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
                println!("read length:{}", length);
        let file_guard = self.structure_file.lock().unwrap();
        let data: Vec<u8> = file_guard.read_in_file(offset, length);


        let obj: T = bincode::deserialize(&data).expect("Deserialization failed");
        println!("{:?}", obj);
        if !obj.is_dynamic_structure() {
            return obj;
        } else {



            let decoded_objs: Vec<Value> = self.load_object_dynamic(vec![obj]);
            let decoded_obj = decoded_objs[0].clone();
            let result_obj = serde_json::from_value(decoded_obj).unwrap();

            return result_obj;
        }


    }

    pub fn read_bulk(&self, index: usize, count: usize) -> Vec<T> {
        let size_of_object = get_item_size::<T>();
        let offset = (size_of_object * index as usize + LENGTH_MARKER_SIZE) as u64;
        println!("read offset:{}", offset);
        let length = count * size_of_object;
        println!("read length:{}", length);

        let file_guard = self.structure_file.lock().unwrap();
        let data: Vec<u8> = file_guard.read_in_file(offset, length);


        let objs: Vec<T> = data
            .par_chunks(size_of_object)
            .map(|data| Self::deserialize_object(data))
            .collect();

        if !objs[0].is_dynamic_structure() {
            return objs;
        } else {
            let decoded_objs: Vec<Value> = self.load_object_dynamic(objs);

            let result_objs: Vec<T> = decoded_objs
                .par_iter()
                .map(|obj| serde_json::from_value(obj.clone()).unwrap())
                .collect();

            return result_objs;
        }
    }

    fn save_object_dynamic(&self, objs: Vec<T>) -> Vec<Value> {
        let dynamic_fields: Vec<String> = objs[0].get_dynamic_fields();
        let objs_len = objs.len();






        let start = Instant::now(); 
                                    
        let json_objs: Vec<_> = objs
            .par_iter()
            .map(|obj| serde_json::to_value(obj).unwrap())
            .collect();
        
        let to_json_value_duration = start.elapsed();
        println!(
            "to_json_value_duration duration   took: {:?}",
            to_json_value_duration
        );
        let json_objs_to_read = json_objs.clone();

        let arc_objs = Arc::new(Mutex::new(json_objs));
        let arc_objs_clone = Arc::clone(&arc_objs);
        

        dynamic_fields.par_iter().for_each(|field| {
            
            let json_objs_to_read = json_objs_to_read.clone();

            let field_obj_len_list = Arc::new(RwLock::new(vec![0; objs_len]));
            
            
            
            let byte_vector: Vec<u8> = json_objs_to_read
                .par_iter()
                
                .map(|obj| {
                    
                    obj.get(field.clone()).unwrap()
                })
                
                
                .map(|obj| serde_json::from_value::<Vec<usize>>(obj.clone()).unwrap())
                .map(|obj| obj.par_iter().map(|o| *o as u8).collect::<Vec<u8>>())
                .enumerate()
                .map(|(i, obj)| {
                    
                    let field_obj_len_list_write_clone = Arc::clone(&field_obj_len_list);
                    let mut list = field_obj_len_list_write_clone.write().unwrap();
                    list[i] = obj.len();
                    obj
                })
                .flatten()
                .collect::<Vec<u8>>();
            
            let to_string_duration = start.elapsed();
            println!(
                "to string   took: {:?}",
                to_string_duration - to_json_value_duration
            );

            let file_path =
                std::path::Path::new(&self.dynamic_repository_dir).join(field.clone());
            let _ = std::fs::OpenOptions::new()
                .create_new(true)
                .open(file_path.clone());

            let file_path_str = file_path.to_string_lossy().into_owned();
            let string_repository = StringRepository::new(
                file_path_str,
                self.initial_size_if_not_exists.clone(),
            );

            let (offset, _end_offset) =
                string_repository.write_string_content_and_get_offset(byte_vector);
            let _total_length_list: Vec<u64> = Vec::new();
            let mut current_offset = offset;
            let write_string_duration = start.elapsed();
            println!(
                "write_string_duration    took: {:?}",
                write_string_duration - to_string_duration
            );

            
            for i in 0..objs_len {
                if let Some(dynamic_obj_value) =
                    arc_objs_clone.lock().unwrap()[i].get_mut(field.clone())
                {
                    let field_obj_len_list_read_clone = Arc::clone(&field_obj_len_list);
                    let list_to_read = field_obj_len_list_read_clone.read().unwrap();
                    
                    let offset_and_total_length =
                        vec![current_offset, current_offset + list_to_read[i] as u64];
                    
                    let mut bytes_offset: Vec<u8> = current_offset.to_le_bytes().to_vec();
                    
                    let bytes_total_length: Vec<u8> = (current_offset
                        + list_to_read[i] as u64)
                        .to_le_bytes()
                        .to_vec();
                    bytes_offset.extend(bytes_total_length.iter());
                    
                    *dynamic_obj_value = offset_and_total_length.into();


                    current_offset += list_to_read[i] as u64;
                } else {
                    println!("no value!");
                }
            }

            let pack_offset_duration = start.elapsed();
            println!(
                "pack offset     took: {:?}",
                pack_offset_duration - write_string_duration
            );
            println!("this thread      took: {:?}", pack_offset_duration);
        });



        let obj_with_persisted_dynamic = arc_objs_clone.lock().unwrap().to_vec();

        obj_with_persisted_dynamic
    }

    fn load_object_dynamic(&self, objs: Vec<T>) -> Vec<Value> {
        let dynamic_fields: Vec<String> = objs[0].get_dynamic_fields();
        let dynamic_fields_count = dynamic_fields.len();
        println!(
            "dynamic_fields: {:?}, dynamic_fields_count: {}",
            dynamic_fields, dynamic_fields_count
        );


        let objs_len = objs.len();

        println!("origin objs, {:?}", &objs);

        let json_objs: Vec<Value> = objs
            .par_iter()
            .map(|obj| serde_json::to_value(obj).unwrap())
            .collect();



        let objs_value: Vec<_> = Vec::new();

        let arc_objs = Arc::new(Mutex::new(json_objs));
        let arc_objs_clone = Arc::clone(&arc_objs);

        let arc_objs_value = Arc::new(Mutex::new(objs_value));
        let arc_objs_value_clone = Arc::clone(&arc_objs_value);

        dynamic_fields.par_iter().for_each(move |field| {
            let mut field_obj_len_list: Vec<Vec<usize>> = Vec::new();
            let field_arguments_list: Vec<Vec<usize>> = arc_objs_clone
                .lock()
                .unwrap()
                .par_iter()
                .map(|obj| obj.get(field.clone()).unwrap())
                .map(|obj| {

                    serde_json::from_value(obj.clone()).unwrap()
                })
                .collect::<Vec<Vec<usize>>>();

            *arc_objs_value_clone.lock().unwrap() = arc_objs_clone
                .lock()
                .unwrap()
                .par_iter()
                .cloned()
                .collect::<Vec<Value>>();

            field_arguments_list.iter().for_each(|obj| {
                field_obj_len_list.push(obj.to_vec());
            });


            let file_path =
                std::path::Path::new(&self.dynamic_repository_dir).join(field.clone());

            let file_path_str = file_path.to_string_lossy().into_owned();
            let string_repository = StringRepository::new(
                file_path_str,
                self.initial_size_if_not_exists.clone(),
            );

            let start_offset: usize = field_obj_len_list[0][0];
            let total_length: usize =
                field_obj_len_list[field_arguments_list.len() - 1][1];

            let string_bytes: Vec<u8> = string_repository
                .load_string_content(start_offset as u64, total_length as u64);




            let mut current_offset = 0;
            for i in 0..objs_len {
                if let Some(dynamic_obj_value) =
                    arc_objs_value_clone.lock().unwrap()[i].get_mut(field.clone())
                {
                    let offset_and_total_length = &field_obj_len_list[i];

                    let offset = offset_and_total_length[0];
                    let end_offset = offset_and_total_length[1];
                    let string_bytes_len = end_offset - offset;
                    let current_field_string_bytes =
                        &string_bytes[current_offset..current_offset + string_bytes_len];
                    current_offset += string_bytes_len;

                    let current_field_string = current_field_string_bytes.to_vec();


                    *dynamic_obj_value =
                        serde_json::to_value(current_field_string).unwrap();

                } else {
                    println!("no value!");
                }
            }
        });

        let obj_with_persisted_dynamic = arc_objs_value.lock().unwrap().clone();

        obj_with_persisted_dynamic
    }
}

fn get_item_size<T>() -> usize {

    size_of::<T>()
}

#[cfg(test)]
mod test {
    use super::*;
    const COUNT: usize = 1000;

    #[derive(Serialize, Deserialize, Default, Debug, Clone, CheckDynamicSize)]
    pub struct ExampleStruct {
        my_number: usize,

        my_vec: Vec<usize>,


    }

    #[test]
    fn test_write_one() {

        let my_service = ObjectPersistOnDiskService::<ExampleStruct>::new(
            "data.bin".to_string(),
            "StringData.bin".to_string(),
            1024,
        )
        .unwrap();

        let i = 1;
        let my_obj = ExampleStruct {
            my_number: i,
            my_vec: vec![i],


        };
        my_service.add(my_obj);
    }

    #[test]
    fn test_add_one() {
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


        };
        println!("size of my obj: {}", size_of_val(&my_obj));
        my_service.add(my_obj);
    }

    #[test]
    fn test_read_one() {
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
    fn test_string_write_bulk() {

        let my_service = ObjectPersistOnDiskService::<ExampleStruct>::new(
            "data.bin".to_string(),
            "StringData.bin".to_string(),
            1024,
        )
        .unwrap();
        let mut objs_list = std::vec::Vec::new();
        for i in 0..COUNT {
            
            let my_obj = ExampleStruct {
                my_number: i,
                
                my_vec: vec![i],
                
            };
            objs_list.push(my_obj);
        }
        my_service.add_bulk(objs_list);
    }

    #[test]
    fn test_size_of_struct() {
        let my_read_service = ObjectPersistOnDiskService::<ExampleStruct>::new(
            "data.bin".to_string(),
            "StringData.bin".to_string(),
            1024,
        )
        .unwrap();
        let mut objs_list = std::vec::Vec::new();
        for i in 1..COUNT {
            
            let my_obj = ExampleStruct {
                my_number: i,
                
                my_vec: vec![i],
            };
            objs_list.push(my_obj.clone());
            
        }
        
        
        
        let my_structure_size = size_of::<ExampleStruct>();
        let my_vec_structure_size = size_of::<Vec<ExampleStruct>>();
        let my_string_size = size_of::<String>();
        let my_str_size = size_of::<&str>();
        let my_char_size = size_of::<char>();
        let my_bool_size = size_of::<bool>();
        let objs = my_read_service.read(1);
        println!("{:?}", objs);
        dbg!(
            my_structure_size,
            my_vec_structure_size,
            my_str_size,
            my_string_size,
            my_char_size,
            my_bool_size,
            objs
        );

    }

    #[test]
    fn test_add_bulk() {
        
        let write_service = ObjectPersistOnDiskService::<ExampleStruct>::new(
            "data.bin".to_string(),
            "StringData.bin".to_string(),
            1024,
        )
        .unwrap();
        let mut objs_list = std::vec::Vec::new();
        for i in 0..COUNT {
            
            let my_obj = ExampleStruct {
                my_number: i,
                
                my_vec: vec![i],
                

            };
            objs_list.push(my_obj.clone());
            println!("size of ExampleStruct:{}", size_of::<ExampleStruct>());
        }
        write_service.add_bulk(objs_list);
    }

    #[test]
    fn test_read_bulk() {
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
    fn test_io_bulk() {
        
        let io_service = ObjectPersistOnDiskService::<ExampleStruct>::new(
            "data.bin".to_string(),
            "StringData.bin".to_string(),
            1024,
        )
        .unwrap();

        let mut objs_list = std::vec::Vec::new();
        for i in 0..COUNT {
            

            let my_obj = ExampleStruct {
                my_number: i,
                
                my_vec: vec![i],
            };
            objs_list.push(my_obj.clone());
        }
        io_service.add_bulk(objs_list);
        let objs = io_service.read_bulk(0, COUNT);
        assert_eq!(objs.len(), COUNT);
        
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
            
            my_vec: vec![i],
            
            
            
        };

        assert!(example.is_dynamic_structure());

        
        let result: Vec<String> = example.get_dynamic_fields();
        println!("{:?}", result);
    }
}
