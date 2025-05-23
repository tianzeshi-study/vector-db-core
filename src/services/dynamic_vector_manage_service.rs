use rayon::prelude::*;
use serde::{
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
    time::Instant,
};

use crate::services::{
    file_access_service::FileAccessService,
    string_repository::StringRepository,
};

const LENGTH_MARKER_SIZE: usize = size_of::<u64>();

pub struct DynamicVectorManageService<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Send,
{
    length: Arc<Mutex<u64>>,
    structure_file: Mutex<FileAccessService>,
    string_repository: StringRepository,
    _marker: PhantomData<T>,
}

impl<T> DynamicVectorManageService<T>
where
    T: Serialize
        + for<'de> Deserialize<'de>
        + 'static
        + std::fmt::Debug
        + Clone
        + Send
        + Sync,
{
    pub fn new(
        structure_file_path: String,
        string_file_path: String,
        initial_size_if_not_exists: u64,
    ) -> io::Result<Self> {
        let structure_file_access =
            FileAccessService::new(structure_file_path, initial_size_if_not_exists);
        let string_repository =
            StringRepository::new(string_file_path.clone(), initial_size_if_not_exists);
        let length = {
            let buffer = structure_file_access.read_in_file(0, LENGTH_MARKER_SIZE);

            assert!(buffer.len() >= 8, "Buffer length must be at least 4 bytes.");

            let length = u64::from_le_bytes(buffer[0..8].try_into().unwrap());

            Arc::new(Mutex::new(length))
        };
        Ok(Self {
            length,
            structure_file: Mutex::new(structure_file_access),
            string_repository,
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

    fn save_dynamic(&self, obj: T) -> (u64, u64) {
        let bytes = bincode::serialize(&obj).expect("Serialization failed");
        let (start_offset, end_offset) = self
            .string_repository
            .write_string_content_and_get_offset(bytes);

        (start_offset, end_offset)
    }

    pub fn save_dynamic_bulk(&self, objs: Vec<T>) -> Vec<(u64, u64)> {
        let start = Instant::now();

        let (bytes, length_list): (Vec<u8>, Vec<u64>) = objs
            .par_iter()
            .map(|obj| {
                let serialized = bincode::serialize(&obj).expect("Serialization failed");
                let len = serialized.len() as u64;
                (serialized, len)
            })
            .fold(
                || (Vec::new(), Vec::new()),
                |mut acc, (serialized, len)| {
                    acc.0.extend(serialized);
                    acc.1.push(len);
                    acc
                },
            )
            .reduce(
                || (Vec::new(), Vec::new()),
                |(mut bytes1, mut lengths1), (bytes2, lengths2)| {
                    bytes1.extend(bytes2);
                    lengths1.extend(lengths2);
                    (bytes1, lengths1)
                },
            );

        let collect_length_list_duration = start.elapsed();
        println!(
            "collect length list took: {:?}",
            collect_length_list_duration
        );

        let (start_offset, _) = self
            .string_repository
            .write_string_content_and_get_offset(bytes);
        let write_vector_content_duration = start.elapsed();
        println!(
            "persist vector content  on  dist took: {:?}",
            write_vector_content_duration - collect_length_list_duration
        );

        let offsets_list = length_list
            .into_iter()
            .scan(start_offset, |current_offset, length| {
                let start = *current_offset;
                let end = start + length;
                *current_offset = end;
                Some((start, end))
            })
            .collect::<Vec<(u64, u64)>>();

        let collect_offsets_duration = start.elapsed();
        println!(
            "collect offsets took: {:?}",
            collect_offsets_duration - write_vector_content_duration
        );

        offsets_list
    }

    fn load_dynamic(&self, start_offset: u64, end_offset: u64) -> T {
        let bytes: Vec<u8> = self
            .string_repository
            .load_string_content(start_offset, end_offset);
        let obj: T = bincode::deserialize(&bytes).expect("deserialization failed");
        obj
    }

    pub fn load_dynamic_bulk(
        &self,
        start_offset_and_end_offset_list: Vec<(u64, u64)>,
    ) -> Vec<T> {
        let start_offset = start_offset_and_end_offset_list[0].0;
        let end_offset = start_offset_and_end_offset_list
            [start_offset_and_end_offset_list.len() - 1]
            .1;
        let bytes: Vec<u8> = self
            .string_repository
            .load_string_content(start_offset, end_offset);
        let length_list: Vec<u64> = start_offset_and_end_offset_list
            .par_iter()
            .map(|obj| obj.1 - obj.0)
            .collect();

        let mut start = 0;
        let byte_vectors: Vec<Vec<u8>> = length_list
            .into_iter()
            .map(|length| {
                let length = length as usize;

                assert!(
                    start + length <= bytes.len(),
                    "Invalid length_list or bytes!"
                );

                let segment = bytes[start..start + length].to_vec();

                start += length;

                segment
            })
            .collect();

        let objs: Vec<T> = byte_vectors
            .par_iter()
            .map(|obj| bincode::deserialize(obj).expect("Serialization failed"))
            .collect();

        objs
    }

    pub fn save(&self, obj: T) {
        let index_to_write = {
            let mut length = self.length.lock().unwrap();
            let index = *length;
            *length += 1;
            self.save_length(*length);
            index
        };
        let file_offset =
            index_to_write * LENGTH_MARKER_SIZE as u64 * 2 + LENGTH_MARKER_SIZE as u64;
        let (start_offset, end_offset) = self.save_dynamic(obj);
        let mut bytes_offset: Vec<u8> = start_offset.to_le_bytes().to_vec();
        let bytes_total_length: Vec<u8> = end_offset.to_le_bytes().to_vec();
        bytes_offset.extend(bytes_total_length.iter());
        let file_guard = self.structure_file.lock().unwrap();
        file_guard.write_in_file(file_offset, &bytes_offset);
    }

    pub fn load(&self, index: u64) -> T {
        let file_offset =
            2 * LENGTH_MARKER_SIZE as u64 * index + LENGTH_MARKER_SIZE as u64;

        let file_guard = self.structure_file.lock().unwrap();
        let marker_data: Vec<u8> =
            file_guard.read_in_file(file_offset, 2 * LENGTH_MARKER_SIZE);
        assert_eq!(marker_data.len(), 16);
        let start_offset_bytes = &marker_data[0..8];
        let end_offset_bytes = &marker_data[8..16];
        let start_offset = u64::from_le_bytes(start_offset_bytes.try_into().unwrap());
        let end_offset = u64::from_le_bytes(end_offset_bytes.try_into().unwrap());

        let obj: T = self.load_dynamic(start_offset, end_offset - start_offset);

        obj
    }

    pub fn _save_bulk1(&self, objs: Vec<T>) {
        let (index_to_write, _length) = {
            let count = objs.len();
            let mut length = self.length.lock().unwrap();
            let index = *length;
            *length += count as u64;
            self.save_length(*length);
            (index, *length)
        };
        let file_offset =
            index_to_write * LENGTH_MARKER_SIZE as u64 * 2 + LENGTH_MARKER_SIZE as u64;
        let start = Instant::now();
        let start_offset_and_end_offset: Vec<(u64, u64)> = self.save_dynamic_bulk(objs);
        let save_dynamic_duration = start.elapsed();
        println!(
            "save {} dynamic objs  total cost: {:?}",
            start_offset_and_end_offset.len(),
            save_dynamic_duration
        );
        let offset_buffer: Vec<u8> = start_offset_and_end_offset
            .par_iter()
            .map(|obj| {
                let start_offset = obj.0;
                let end_offset = obj.1;
                let mut bytes_offset: Vec<u8> = start_offset.to_le_bytes().to_vec();
                let mut bytes_total_length: Vec<u8> = end_offset.to_le_bytes().to_vec();
                // bytes_offset.extend(bytes_total_length.iter());
                bytes_offset.append(&mut bytes_total_length);
                bytes_offset
            })
            .flatten()
            .collect::<Vec<u8>>();
        let collect_offsets_duration = start.elapsed();
        println!(
            "collect offsets bytes took: {:?}",
            collect_offsets_duration - save_dynamic_duration
        );
        let file_guard = self.structure_file.lock().unwrap();
        file_guard.write_in_file(file_offset, &offset_buffer);
        let write_offsets_duration = start.elapsed();
        println!(
            "write offsets took: {:?}",
            write_offsets_duration - collect_offsets_duration
        );
    }

    pub fn save_bulk(&self, objs: Vec<T>) {
        let (index_to_write, _length) = {
            let count = objs.len();
            let mut length = self.length.lock().unwrap();
            let index = *length;
            *length += count as u64;
            self.save_length(*length);
            (index, *length)
        };
        let file_offset =
            index_to_write * LENGTH_MARKER_SIZE as u64 * 2 + LENGTH_MARKER_SIZE as u64;
        let start = Instant::now();
        let start_offset_and_end_offset: Vec<(u64, u64)> = self.save_dynamic_bulk(objs);
        let save_dynamic_duration = start.elapsed();
        println!(
            "save {} dynamic objs  total cost: {:?}",
            start_offset_and_end_offset.len(),
            save_dynamic_duration
        );
        let offset_buffer: Vec<u8> = start_offset_and_end_offset
            .into_par_iter()
            .flat_map(|(start_offset, end_offset)| {
                let mut local_buffer = [0u8; 16];
                local_buffer[..8].copy_from_slice(&start_offset.to_le_bytes());
                local_buffer[8..].copy_from_slice(&end_offset.to_le_bytes());
                local_buffer.to_vec()
            })
            .collect();
        let collect_offsets_duration = start.elapsed();
        println!(
            "collect offsets bytes took: {:?}",
            collect_offsets_duration - save_dynamic_duration
        );
        let file_guard = self.structure_file.lock().unwrap();
        file_guard.write_in_file(file_offset, &offset_buffer);
        let write_offsets_duration = start.elapsed();
        println!(
            "write offsets took: {:?}",
            write_offsets_duration - collect_offsets_duration
        );
    }

    pub fn load_bulk(&self, index: u64, count: u64) -> Vec<T> {
        let file_offset =
            2 * LENGTH_MARKER_SIZE as u64 * index + LENGTH_MARKER_SIZE as u64;
        let file_guard = self.structure_file.lock().unwrap();
        let marker_data: Vec<u8> =
            file_guard.read_in_file(file_offset, 2 * LENGTH_MARKER_SIZE * count as usize);

        let total_marker_length = 16 * count;

        assert_eq!(marker_data.len() as u64, total_marker_length);

        let start_offset_and_end_offset_list: Vec<(u64, u64)> = marker_data
            .chunks_exact(16)
            .map(|chunk| {
                let part1 = u64::from_le_bytes(
                    chunk[0..8].try_into().expect("Failed to parse u64!"),
                );

                let part2 = u64::from_le_bytes(
                    chunk[8..16].try_into().expect("Failed to parse u64!"),
                );
                (part1, part2)
            })
            .collect();
        let objs: Vec<T> = self.load_dynamic_bulk(start_offset_and_end_offset_list);

        objs
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const COUNT: usize = 1000;

    #[derive(Serialize, Deserialize, Default, Debug, Clone)]
    pub struct ExampleStruct {
        id: usize,
        my_vec: Vec<usize>,
        my_vec1: Vec<usize>,
        my_vec2: Vec<usize>,
    }

    fn remove_file(path: &str) {
        if std::path::Path::new(&path).exists() {
            std::fs::remove_file(&path).expect("Unable to remove file");
        }
    }

    #[test]
    fn test_save_one() {
        remove_file("Dynamic0.bin");
        remove_file("StringDynamic0.bin");
        let my_service = DynamicVectorManageService::<ExampleStruct>::new(
            "DynamicX.bin".to_string(),
            "StringDynamicX.bin".to_string(),
            1024,
        )
        .unwrap();

        let i = 0;
        let vec_test = vec![i];
        let my_obj = ExampleStruct {
            id: i,
            my_vec: vec_test.clone(),
            my_vec1: vec_test.clone(),
            my_vec2: vec_test.clone(),
        };

        println!("size of my obj: {}", size_of_val(&my_obj));

        my_service.save(my_obj);
    }

    #[test]
    fn test_load_one() {
        remove_file("Dynamic1.bin");
        remove_file("StringDynamic1.bin");
        let my_service = DynamicVectorManageService::<ExampleStruct>::new(
            "Dynamic1.bin".to_string(),
            "StringDynamic1.bin".to_string(),
            1024,
        )
        .unwrap();

        let i = 0;
        let vec_test = vec![i];
        let my_obj = ExampleStruct {
            id: i,
            my_vec: vec_test.clone(),
            my_vec1: vec_test.clone(),
            my_vec2: vec_test.clone(),
        };

        println!("size of my obj: {}", size_of_val(&my_obj));

        my_service.save(my_obj);

        let obj = my_service.load(0);
        assert_eq!(0, obj.id);
        println!("read one result:\n {:?}", obj);
        let length = my_service.get_length();
        assert_eq!(length, 1);
    }

    #[test]
    fn test_save_bulk() {
        remove_file("Dynamic2.bin");
        remove_file("StringDynamic2.bin");

        let write_service = DynamicVectorManageService::<ExampleStruct>::new(
            "Dynamic2.bin".to_string(),
            "StringDynamic2.bin".to_string(),
            1024,
        )
        .unwrap();
        let mut objs_list = std::vec::Vec::new();
        for i in 0..COUNT {
            let vec_test = vec![i];
            let my_obj = ExampleStruct {
                id: i,
                my_vec: vec_test.clone(),
                my_vec1: vec_test.clone(),
                my_vec2: vec_test.clone(),
            };
            objs_list.push(my_obj.clone());
        }
        let start = Instant::now();
        write_service.save_bulk(objs_list);
        let save_bulk_duration = start.elapsed();
        println!("save bulk    took: {:?}", save_bulk_duration);
    }

    #[test]
    fn test_load_bulk() {
        remove_file("Dynamic3.bin");
        remove_file("StringDynamic3.bin");
        let read_service = DynamicVectorManageService::<ExampleStruct>::new(
            "Dynamic3.bin".to_string(),
            "StringDynamic3.bin".to_string(),
            1024,
        )
        .unwrap();
        let mut objs_list = std::vec::Vec::new();
        for i in 0..COUNT {
            let vec_test = vec![i];
            let my_obj = ExampleStruct {
                id: i,
                my_vec: vec_test.clone(),
                my_vec1: vec_test.clone(),
                my_vec2: vec_test.clone(),
            };
            objs_list.push(my_obj.clone());
        }
        let start = Instant::now();
        read_service.save_bulk(objs_list);
        let save_bulk_duration = start.elapsed();
        println!("save bulk    took: {:?}", save_bulk_duration);
        let start = Instant::now();
        let objs = read_service.load_bulk(0, COUNT as u64);
        let load_bulk_duration = start.elapsed();
        println!("load bulk    took: {:?}", load_bulk_duration);
        println!(
            "last read obj: {:?}\n total count {}",
            objs[objs.len() - 1],
            objs.len()
        );
        assert_eq!(objs[objs.len() - 1].id, COUNT - 1);
        let length = read_service.get_length();
        assert_eq!(length, COUNT as u64);
    }

    #[test]
    fn test_get_dynamic_length() {
        remove_file("Dynamic4.bin");
        remove_file("StringDynamic4.bin");
        let read_service = DynamicVectorManageService::<ExampleStruct>::new(
            "Dynamic4.bin".to_string(),
            "StringDynamic4.bin".to_string(),
            1024,
        )
        .unwrap();
        let length = read_service.get_length();
        println!("length: {}", length);
    }
}
