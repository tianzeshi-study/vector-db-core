use std::sync::{Arc, Mutex};
use std::convert::TryInto;

use crate::file_access_service::FileAccessService;

const END_OFFSET_SIZE: usize = std::mem::size_of::<u64>(); // 文件头保留的偏移量空间大小

pub struct SavedDynamic {
    pub offset: u64,
    pub total_length: u64,
}

pub struct ObjectWithPersistedDynamic<T> {
    pub object: T,
    pub i_enumerable: Vec<SavedDynamic>,
}

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
    pub fn write_string_content_and_get_offset(&mut self, bytes_vector: Vec<u8>) -> (u64, u64) {
        // let str  = Some(&vector);
        // let void_string = "".to_string();
        // match str {
            // Some("".to_string()) => (-1, 0),
            // Some(&void_string) => (-1, 0), 
            // Some() => (-1, 0),
            // None => (-2, 0),
            // Some(s) => {
                // let string_bytes = vector;
                let current_offset;
                {
                    // 使用锁防止文件扩展的并发问题
                    let _lock = self.expand_size_lock.lock().unwrap();
                    current_offset = self.file_end_offset;
                    // self.file_end_offset += string_bytes.len() as u64;
                    self.file_end_offset += bytes_vector.len() as u64;
                }
                self.file_access.write_in_file(current_offset, &bytes_vector);
                self.file_access.write_in_file(0, &self.file_end_offset.to_le_bytes());
                (current_offset as u64,current_offset + bytes_vector.len() as u64)
            // }
        // }
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

# [cfg(test)]
mod test {
    use super:: * ;
    const  COUNT: usize = 10;
    
    // #[derive(Serialize, Deserialize, Default, Debug, Clone, CheckDynamicSize)]
        pub struct ExampleStruct {
            my_number: usize,
            my_string: String,
            // my_boolean: bool,
        }
        
    
    # [test]
    fn test_dynamic_repository() {
        // 创建服务实例        
        let mut my_service = StringRepository::new("test_dynamic_repository.bin".to_string(), 1024);
        // 示例添加对象
        let i = 1;
        let my_obj = ExampleStruct {
            my_number: i,
            my_string: format!("hello, world!{i}").to_string(),
            // my_boolean:  i %4 ==0,
        };
        // my_service.add(my_obj);
        // let bytes_vector    : Vec<u8> =vec![0,64];
        let bytes_vector    : Vec<u8> = "hello, world".to_string().as_bytes().to_vec();
        let result = my_service.write_string_content_and_get_offset(bytes_vector);
        println!("{:?}", result);
    }
    
    

}

