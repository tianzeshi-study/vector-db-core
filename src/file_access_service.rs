use std::fs::{
    File,
    OpenOptions
};
use std::io::{
    Read,
    Seek,
    SeekFrom,
    Write
};
use std::sync::{
    Mutex,
    Arc
};

// 定义一个文件访问服务结构体
pub struct FileAccessService {
    path: String, // 存储文件路径
    current_size: Arc < Mutex < u64 >> , // 存储当前文件大小，使用Arc和Mutex实现线程安全
}

impl FileAccessService {
    // 构造函数，初始化文件路径和文件大小
    pub fn new(path: String, initial_size_if_not_exists: u64)->Self {
        // 若文件不存在，创建文件并设置初始大小
        if !std::path::Path::new( & path)
            .exists() {
                let fs = File::create( & path).expect("Unable to create file");
                fs.set_len(initial_size_if_not_exists).expect("Unable to set file length");
            }

        // 获取文件当前大小
        let current_size = std::fs::metadata( & path).expect("Unable to get file metadata").len();
        FileAccessService {
            path,
            current_size: Arc::new(Mutex::new(current_size)),
        }
    }

    // 向文件指定偏移量写入数据
    pub fn write_in_file( & self, offset: u64, data:  & [u8]) {
        let mut current_size = self.current_size.lock().unwrap(); // 获取当前大小的锁

        // 检查写入数据是否超过当前文件大小
        if offset + data.len()as u64 >  * current_size {
            // 文件扩展逻辑
            let fs = OpenOptions::new().write(true).open( & self.path).expect("Unable to open file");
            fs.set_len( * current_size * 2).expect("Unable to extend file size");
             * current_size *= 2; // 更新当前大小
        }

        // 写入数据
        let mut fs = OpenOptions::new().write(true).open( & self.path).expect("Unable to open file");
        fs.seek(SeekFrom::Start(offset)).expect("Unable to seek to offset");
        fs.write_all(data).expect("Unable to write data");
    }

    // 从文件指定偏移量读取数据
    pub fn read_in_file( & self, offset: u64, length: usize)->Vec < u8 > {
        let current_size = self.current_size.lock().unwrap(); // 获取当前大小的锁

// length +=4;
        // 检查读取的范围是否超出当前文件大小
        // if  length as u64 >  * current_size {
        if offset + length as u64 >  * current_size {
            // dbg!(offset, length, current_size);
            panic!("Exceeded the file size while reading");
        }

        // 读取数据
        let mut fs = OpenOptions::new().read(true).open( & self.path).expect("Unable to open file");
        fs.seek(SeekFrom::Start(offset)).expect("Unable to seek to offset");
        let mut buffer = vec![0;length];
        // dbg!(&buffer.len());
        // dbg!(offset, length, current_size);
        // println!("cache buffer to read: {}", &buffer.len());
        fs.read_exact( & mut buffer).expect("Failed to read the expected length of data");
        buffer
    }
}

 # [cfg(test)]
mod test {
    use super::*;
     # [test]
    fn test_file_access_service() {
        let service = FileAccessService::new("example.db".to_string(), 1024);

        // 写入示例数据
        service.write_in_file(0, b"Hello, Rust!");

        // 读取示例数据
        let data = service.read_in_file(0, 12);
        println!("Read data: {:?}", String::from_utf8(data).expect("Invalid UTF-8 data"));
    }
}

