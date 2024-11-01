use std::sync::{Arc, Mutex};
use std::convert::TryInto;

use crate::file_access_service::FileAccessService;

const END_OFFSET_SIZE: usize = std::mem::size_of::<u64>(); // 文件头保留的偏移量空间大小

/// `StringRepository` 用于存储和读取字符串
pub struct StringRepository {
    file_access: FileAccessService,
    file_end_offset: u64,
    expand_size_lock: Arc<Mutex<()>>,
}

impl StringRepository {
    /// 初始化 `StringRepository`，并设置文件结束偏移量
    pub fn new(string_file_path: String, initial_size_if_not_exists: u64) -> Self {
        let file_access = FileAccessService::new(string_file_path, initial_size_if_not_exists);
        let file_end_offset = Self::get_string_file_end_offset(&file_access);
        Self {
            file_access,
            file_end_offset,
            expand_size_lock: Arc::new(Mutex::new(())),
        }
    }

    /// 获取文件的结束偏移量
    fn get_string_file_end_offset(file_access: &FileAccessService) -> u64 {
        let buffer = file_access.read_in_file(0, END_OFFSET_SIZE);
        println!("buffer in string repository : {} ", &buffer.len());
        let offset = 4;
        // let offset = u64::from_le_bytes(buffer.try_into().unwrap());
        // let offset = u64::from_le_bytes(buffer.try_into().unwrap());
        if offset <= END_OFFSET_SIZE as u64 {
            END_OFFSET_SIZE as u64
        } else {
            offset
        }
    }

    /// 写入字符串内容并返回其偏移量和长度
    pub fn write_string_content_and_get_offset(&mut self, str: Option<&str>) -> (i64, usize) {
        match str {
            Some("") => (-1, 0),
            None => (-2, 0),
            Some(s) => {
                let string_bytes = s.as_bytes();
                let current_offset;
                {
                    // 使用锁防止文件扩展的并发问题
                    let _lock = self.expand_size_lock.lock().unwrap();
                    current_offset = self.file_end_offset;
                    self.file_end_offset += string_bytes.len() as u64;
                }
                self.file_access.write_in_file(current_offset, string_bytes);
                self.file_access
                    .write_in_file(0, &self.file_end_offset.to_le_bytes());
                (current_offset as i64, string_bytes.len())
            }
        }
    }

    /// 根据偏移量和长度加载字符串内容
    pub fn load_string_content(&self, offset: i64, length: usize) -> Option<String> {
        match offset {
            -1 => Some(String::new()), // 空字符串
            -2 => None,                 // `null`字符串
            _ => {
                let string_bytes = self
                    .file_access
                    .read_in_file(offset as u64, length);
                Some(String::from_utf8(string_bytes).expect("Invalid UTF-8 sequence"))
            }
        }
    }
}
