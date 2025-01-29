use std::{
    convert::TryInto,
    sync::{
        Arc,
        Mutex,
    },
};

use crate::services::file_access_service::FileAccessService;

const END_OFFSET_SIZE: usize = std::mem::size_of::<u64>();

pub struct StringRepository {
    file_access: FileAccessService,
    file_end_offset: Arc<Mutex<u64>>,
}

impl StringRepository {
    pub fn new(string_file_path: String, initial_size_if_not_exists: u64) -> Self {
        let file_access =
            FileAccessService::new(string_file_path.clone(), initial_size_if_not_exists);

        let file_end_offset = {
            let buffer = file_access.read_in_file(0, END_OFFSET_SIZE);
            assert!(buffer.len() >= 4, "Buffer length must be at least 4 bytes.");

            let length = u64::from_le_bytes(buffer[0..8].try_into().unwrap());

            Arc::new(Mutex::new(length))
        };

        Self {
            file_access,
            file_end_offset,
        }
    }

    fn _get_string_file_end_offset(file_access: &FileAccessService) -> u64 {
        let buffer = file_access.read_in_file(0, END_OFFSET_SIZE);

        let offset = u64::from_le_bytes(buffer.try_into().unwrap());

        if offset <= END_OFFSET_SIZE as u64 {
            END_OFFSET_SIZE as u64
        } else {
            offset
        }
    }

    pub fn _write_string_content_and_get_offset(
        &mut self,
        bytes_vector: Vec<u8>,
    ) -> (u64, u64) {
        let current_offset = *self.file_end_offset.lock().unwrap();

        self.file_access
            .write_in_file(END_OFFSET_SIZE as u64 + current_offset + 1, &bytes_vector);
        *self.file_end_offset.lock().unwrap() += bytes_vector.len() as u64;

        self.file_access
            .write_in_file(0, &self.file_end_offset.lock().unwrap().to_le_bytes());
        (current_offset, current_offset + bytes_vector.len() as u64)
    }

    pub fn write_string_content_and_get_offset(
        &self,
        bytes_vector: Vec<u8>,
    ) -> (u64, u64) {
        let current_offset = {
            let mut current_offset = self.file_end_offset.lock().unwrap();

            self.file_access.write_in_file(
                END_OFFSET_SIZE as u64 + *current_offset + 1,
                &bytes_vector,
            );

            *current_offset += bytes_vector.len() as u64;

            self.file_access
                .write_in_file(0, &(*current_offset).to_le_bytes());

            *current_offset
        };

        (current_offset - bytes_vector.len() as u64, current_offset)
    }

    pub fn load_string_content(&self, offset: u64, length: u64) -> Vec<u8> {
        let offset = offset + 1;

        let string_bytes: Vec<u8> = self
            .file_access
            .read_in_file(END_OFFSET_SIZE as u64 + offset, length as usize);

        string_bytes
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_write_dynamic_repository() {
        let my_service =
            StringRepository::new("test_dynamic_repository.bin".to_string(), 1024);
        let bytes_vector: Vec<u8> = "hello, world".to_string().as_bytes().to_vec();
        let result = my_service.write_string_content_and_get_offset(bytes_vector);
        println!("{:?}", result);
    }

    #[test]
    fn test_load_dynamic_repository() {
        let my_service =
            StringRepository::new("test_dynamic_repository.bin".to_string(), 1024);
        let string_bytes = my_service.load_string_content(0, 24);
        let result =
            String::from_utf8(string_bytes.clone()).expect("Invalid UTF-8 sequence");
        println!("result: {}", result);
    }
}
