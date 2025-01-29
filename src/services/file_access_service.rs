use std::{
    fs::{
        File,
        OpenOptions,
    },
    io::{
        self,
        Read,
        Seek,
        SeekFrom,
        Write,
    },
    sync::{
        Arc,
        Mutex,
    },
};

pub struct FileAccessService {
    path: String,
    current_size: Arc<Mutex<u64>>,
}

impl FileAccessService {
    pub fn new(path: String, initial_size_if_not_exists: u64) -> Self {
        if !std::path::Path::new(&path).exists() {
            let fs = File::create(&path).expect("Unable to create file");
            fs.set_len(initial_size_if_not_exists)
                .expect("Unable to set file length");
        }

        let current_size = std::fs::metadata(&path)
            .expect("Unable to get file metadata")
            .len();
        FileAccessService {
            path,
            current_size: Arc::new(Mutex::new(current_size)),
        }
    }

    pub fn write_in_file(&self, offset: u64, data: &[u8]) {
        let mut current_size = self.current_size.lock().unwrap();

        while offset + data.len() as u64 > *current_size {
            let fs = OpenOptions::new()
                .write(true)
                .open(&self.path)
                .expect("Unable to open file");
            fs.set_len(*current_size * 2)
                .expect("Unable to extend file size");
            *current_size *= 2;
        }

        let mut fs = OpenOptions::new()
            .write(true)
            .open(&self.path)
            .expect("Unable to open file");
        fs.seek(SeekFrom::Start(offset))
            .expect("Unable to seek to offset");
        fs.write_all(data).expect("Unable to write data");
    }


    pub fn read_in_file(&self, offset: u64, length: usize) -> Vec<u8> {
        let current_size = &self.get_updated_current_file_size();

        if offset + length as u64 > *current_size {
            panic!(
                "offset: {}  and length: {} exceeded the file size: {}  while reading",
                offset, &length, &current_size
            );
        }

        let mut fs = OpenOptions::new()
            .read(true)
            .open(&self.path)
            .expect("Unable to open file");
        fs.seek(SeekFrom::Start(offset))
            .expect("Unable to seek to offset");
        let mut buffer = vec![0; length];

        fs.read_exact(&mut buffer)
            .expect("Failed to read the expected length of data");
        buffer
    }

    fn get_updated_current_file_size(&self) -> u64 {
        let current_size = std::fs::metadata(&self.path)
            .expect("Unable to get file metadata")
            .len();

        let mut size = self.current_size.lock().unwrap();
        *size = current_size;
        *size
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_file_access_service() {
        let service = FileAccessService::new("example.db".to_string(), 1024);

        service.write_in_file(0, b"Hello, Rust!");

        let data = service.read_in_file(0, 12);
        println!(
            "Read data: {:?}",
            String::from_utf8(data).expect("Invalid UTF-8 data")
        );
    }
}
