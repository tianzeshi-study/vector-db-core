#[cfg(test)]
mod test {
    use vector_db_core::*;
    use serde::{Serialize, Deserialize};
    use std::time::Instant;
    const COUNT:usize = 300000;
    // use std::marker::PhantomData;
    
            #[derive(Serialize, Deserialize, Default, Debug, Clone, CheckDynamicSize)]
        pub struct DynamicStruct {
            my_usize: usize,
            my_u64: u64,
            my_u32: u32,
            my_u16: u16,
            my_u8: u8,
            my_boolean: bool,
            my_usize_vec: Vec<usize>,
            my_64_vec: Vec<u64>,
            my_32_vec: Vec<u32>,
        }
        

#[test]
fn test_dynamic_add_one() {
    let i = COUNT;
    let my_obj:DynamicStruct   = DynamicStruct {
            my_usize: 443,
            my_u64: 53,
            my_u32: 4399,
            my_u16: 3306,
            my_u8: 22,
            my_boolean: true,
            my_usize_vec: vec![i],
            my_32_vec: vec![i as u32],            
            my_64_vec: vec![i as u64],
        };
        let my_service = ObjectPersistOnDiskService:: < DynamicStruct> ::new("data.bin".to_string(), "StringDynamicData.bin".to_string(), 1024).unwrap();
        my_service.add(my_obj);
}

#[test]
fn test_dynamic_add_bulk() {
    let mut my_vec = vec![1,2,3,4,5];
    let mut objs = Vec::new();
    let my_service = ObjectPersistOnDiskService:: < DynamicStruct> ::new("data.bin".to_string(), "StringDynamicData.bin".to_string(), 1024).unwrap();
    for i in 0..COUNT {
    let my_obj:DynamicStruct   = DynamicStruct {
            my_usize: 443,
            my_u64: 53,
            my_u32: 4399,
            my_u16: 3306,
            my_u8: 22,
            my_boolean: true,
            my_usize_vec: vec![i],
            my_32_vec: vec![i as u32],            
            my_64_vec: vec![i as u64],
        };

        // my_vec.push(i as u64 *1000);

        objs.push(my_obj.clone());
    }
    let start = Instant::now(); // 记录开始时间
        my_service.add_bulk(objs);
    let duration = start.elapsed(); // 计算时间差
    println!("add  {} items   took: {:?}",COUNT  ,  duration);
}

#[test]
fn test_dynamic_read_bulk() {

    let my_service = ObjectPersistOnDiskService:: < DynamicStruct> ::new("data.bin".to_string(), "StringDynamicData.bin".to_string(), 1024).unwrap();
    let start = Instant::now(); // 记录开始时间
    my_service.read_bulk(0, COUNT);
    let duration = start.elapsed(); // 计算时间差
    println!("read {} items   took: {:?}",COUNT  ,  duration);
}

}