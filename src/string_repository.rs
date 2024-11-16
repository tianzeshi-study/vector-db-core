use std::convert::TryInto;
use std::sync::{Arc, Mutex};

use crate::file_access_service::FileAccessService;

const END_OFFSET_SIZE: usize = std::mem::size_of::<u64>(); // 文件头保留的偏移量空间大小

/// `StringRepository` 用于存储和读取字符串
pub struct StringRepository {
    file_access: FileAccessService,
    file_end_offset: Arc<Mutex<u64>>,
    // expand_size_lock: Arc<Mutex<()>>,
}

impl StringRepository {
    /// 初始化 `StringRepository`，并设置文件结束偏移量
    pub fn new(string_file_path: String, initial_size_if_not_exists: u64) -> Self {
        let file_access =
            FileAccessService::new(string_file_path.clone(), initial_size_if_not_exists);
        // println!("string repository initial_size_if_not_exists: {}, path: {:?}", initial_size_if_not_exists, string_file_path);
        // let file_end_offset = Self::get_string_file_end_offset(&file_access);
        // let file_end_offset = Self::get_string_file_end_offset();

        let file_end_offset = {
            let buffer = file_access.read_in_file(0, END_OFFSET_SIZE);
            assert!(buffer.len() >= 4, "Buffer length must be at least 4 bytes.");

            let length = u64::from_le_bytes(buffer[0..8].try_into().unwrap());

            Arc::new(Mutex::new(length))
        };

        // println!("file_end_offset: {:?}", file_end_offset);

        Self {
            file_access,
            file_end_offset,
            // expand_size_lock: Arc::new(Mutex::new(())),
        }
    }

    /// 获取文件的结束偏移量
    fn _get_string_file_end_offset(file_access: &FileAccessService) -> u64 {
        // fn get_string_file_end_offset(&self) -> u64 {
        let buffer = file_access.read_in_file(0, END_OFFSET_SIZE);
        // let buffer = self.file_access.read_in_file(0, END_OFFSET_SIZE);
        // println!("buffer in string repository : {} ", &buffer.len());
        // let offset = 8;
        let offset = u64::from_le_bytes(buffer.try_into().unwrap());
        // let offset = u64::from_le_bytes(buffer.try_into().unwrap());
        if offset <= END_OFFSET_SIZE as u64 {
            END_OFFSET_SIZE as u64
        } else {
            offset
        }
    }

    /// 写入字符串内容并返回其偏移量和长度
    pub fn _write_string_content_and_get_offset(&mut self, bytes_vector: Vec<u8>) -> (u64, u64) {
        // dbg!(&bytes_vector);
                // let current_offset;
                let current_offset = self.file_end_offset.lock().unwrap().clone();
                dbg!(current_offset);
                self.file_access.write_in_file(END_OFFSET_SIZE as u64+ current_offset + 1 , &bytes_vector);
                *self.file_end_offset.lock().unwrap() += bytes_vector.len() as u64;

                self.file_access.write_in_file(0, &self.file_end_offset.lock().unwrap().to_le_bytes());
                (current_offset as u64, current_offset + bytes_vector.len() as u64)
    }
    
    pub fn write_string_content_and_get_offset(&self, bytes_vector: Vec<u8>) -> (u64, u64) {
        // dbg!(&bytes_vector);
        let current_offset = {
        let mut current_offset = self.file_end_offset.lock().unwrap();
        dbg!(&current_offset);
        self.file_access
            .write_in_file(END_OFFSET_SIZE as u64 + current_offset.clone()  + 1, &bytes_vector);
        // *self.file_end_offset.lock().unwrap() += bytes_vector.len() as u64;
        *current_offset += bytes_vector.len() as u64;

        self.file_access
            .write_in_file(0, &current_offset.clone().to_le_bytes());
            
            current_offset.clone()
    };
    dbg!(&current_offset);
    
        (
            current_offset -  bytes_vector.len() as u64,
            current_offset ,
        )
    }

    /// 根据偏移量和长度加载字符串内容
    // pub fn load_string_content(&self, offset: i64, length: usize) -> Option<String> {
    pub fn load_string_content(&self, offset: u64, length: u64) -> Vec<u8> {
        // let offset  = offset +END_OFFSET_SIZE as u64;
        let offset = offset + 1;
        dbg!(&offset, &END_OFFSET_SIZE);
        let string_bytes: Vec<u8> = self
            .file_access
            .read_in_file(END_OFFSET_SIZE as u64 + offset as u64, length as usize);
        // Some(String::from_utf8(string_bytes).expect("Invalid UTF-8 sequence"))
        // dbg!(&string_bytes);
        string_bytes
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_write_dynamic_repository() {
        // 创建服务实例
        let mut my_service = StringRepository::new("test_dynamic_repository.bin".to_string(), 1024);
        let bytes_vector: Vec<u8> = "hello, world".to_string().as_bytes().to_vec();
        let result = my_service.write_string_content_and_get_offset(bytes_vector);
        println!("{:?}", result);
    }

    #[test]
    fn test_load_dynamic_repository() {
        let my_service = StringRepository::new("test_dynamic_repository.bin".to_string(), 1024);
        let string_bytes = my_service.load_string_content(0, 24);
        let result = String::from_utf8(string_bytes.clone()).expect("Invalid UTF-8 sequence");
        println!("result: {}", result);
    }
}
