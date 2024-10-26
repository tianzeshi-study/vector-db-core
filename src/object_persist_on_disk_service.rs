use serde::{Serialize, Deserialize};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::sync::{Arc, Mutex};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::marker::PhantomData;

use crate::cached_file_access_service::CachedFileAccessService;
use crate::string_repository::StringRepository; 

const LENGTH_MARKER_SIZE: usize = std::mem::size_of::<i32>(); // We reserve the first 4 bytes for Length

/// 提供了将对象序列化和反序列化到磁盘的服务
/// 
/// - `T`: 要序列化的对象类型，必须实现 `Serialize` 和 `DeserializeOwned` 特征
pub struct ObjectPersistOnDiskService<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Default,
{
    length: Arc<Mutex<i32>>, // 线程安全的对象数量
    structure_file: Mutex<File>, // 结构文件的文件句柄
    _marker: PhantomData<T>, // 用于存储对象类型的占位符
}

pub struct ObjectPersistOnDiskService1<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Default,
{
    length: Arc<Mutex<i32>>, // 线程安全的对象数量
    structure_file: Mutex<CachedFileAccessService>, // 结构文件的文件句柄
    string_repository: StringRepository,
    _marker: PhantomData<T>, // 用于存储对象类型的占位符
}


impl<T> ObjectPersistOnDiskService1<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Default,
{
    pub fn new(structureFilePath: String, stringFilePath: String, initialSizeIfNotExists: u64) -> io::Result<Self> {
        let structure_file_access = CachedFileAccessService::new(structureFilePath, initialSizeIfNotExists, 1024*1024, 512);
        let string_repository = StringRepository::new(stringFilePath, initialSizeIfNotExists);
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
            _marker: PhantomData,
        })
    }
    
    /// 获取当前对象数量
    fn get_length(&self) -> i32 {
        let structure_file_guard =self.structure_file.lock().unwrap(); 
        let buffer = structure_file_guard.read_in_file(0, LENGTH_MARKER_SIZE);
        // 确保 buffer 的长度至少为 4
        assert!(buffer.len() >= 4, "Buffer length must be at least 4 bytes.");

    // 将前 4 个字节转换为 i32，假设使用小端字节序
    let length = i32::from_le_bytes(buffer[0..4].try_into().unwrap());

        // *self.length.lock().unwrap()
        length
    }
}


impl<T> ObjectPersistOnDiskService<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Default,
{
    /// 构造函数，初始化结构文件和对象数量
    pub fn new(structure_file_path: &str) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(structure_file_path)?;

        // 确保文件有足够大小，写入初始化长度
        let length = {
            let length = file.read_i32::<LittleEndian>().unwrap_or(0);
            file.seek(SeekFrom::Start(0))?;
            file.write_i32::<LittleEndian>(length)?;
            Arc::new(Mutex::new(length))
        };

        Ok(Self {
            length,
            structure_file: Mutex::new(file),
            _marker: PhantomData,
        })
    }
    



    /// 获取当前对象数量
    fn get_length(&self) -> i32 {
        *self.length.lock().unwrap()
    }

    /// 保存当前对象数量
    fn save_length(&self, length: i32) {
        let mut file = self.structure_file.lock().unwrap();
        file.seek(SeekFrom::Start(0)).expect("Seek failed.");
        file.write_i32::<LittleEndian>(length).expect("Write failed.");
    }

    /// 序列化对象到字节数组
    fn serialize_object(obj: &T) -> Vec<u8> {
        bincode::serialize(obj).expect("Serialization failed")
    }

    /// 从字节数组反序列化为对象
    fn deserialize_object(data: &[u8]) -> T {
        bincode::deserialize(data).expect("Deserialization failed")
    }

    /// 将对象写入指定索引位置
    pub fn write_index(&self, index: i32, obj: T) {
        let data = Self::serialize_object(&obj);
        let offset = (4 + (index as usize * data.len())) as u64;

        let mut file = self.structure_file.lock().unwrap();
        file.seek(SeekFrom::Start(offset)).expect("Seek failed.");
        file.write_all(&data).expect("Write failed.");
    }

    /// 读取指定索引位置的对象
    pub fn read_index(&self, index: i32) -> T {
        let offset = (4 + (index * std::mem::size_of::<T>() as i32)) as u64;

        let mut file = self.structure_file.lock().unwrap();
        file.seek(SeekFrom::Start(offset)).expect("Seek failed.");

        let mut buffer = vec![0; std::mem::size_of::<T>()];
        file.read_exact(&mut buffer).expect("Read failed.");
        Self::deserialize_object(&buffer)
    }

    /// 添加单个对象
    pub fn add(&self, obj: T) {
        let index_to_write = {
            let mut length = self.length.lock().unwrap();
            let index = *length;
            *length += 1;
            self.save_length(*length);
            index
        };
        self.write_index(index_to_write, obj);
    }
}




 # [cfg(test)]
mod test {
    use super:: * ;
     # [test]
    fn test_object_persist_on_disk_service() {
        // 创建服务实例
        let service = ObjectPersistOnDiskService:: < MyStruct > ::new("data.bin").unwrap();

// 示例对象结构，实现 `Serialize` 和 `Deserialize` 特征
#[derive(Serialize, Deserialize, Default)]
struct MyStruct {
    id: i32,
    name: String,
}


        // 示例添加对象
        let obj = MyStruct {
            id: 0,
            name: String::from("hello")
        };
        let obj1 = MyStruct {
            id: 1,
            name: String::from("hello")
        };
        service.add(obj);
        service.add(obj1);

        // 示例读取对象
        let read_obj = service.read_index(0);
        // let read_obj1 = service.read_index(1);
        assert_eq!(read_obj.id, 0);
        assert_eq!(read_obj.name, "hello");
        println!("读取的对象: id={}, name={}", read_obj.id, read_obj.name);
        dbg!(read_obj.id);
        dbg!(read_obj.name);
    }
}

