#[cfg(test)]
mod test {
    use vector_db_core::*;
    use serde::{Serialize, Deserialize};
    use super::*;
    const COUNT:usize = 1000000;
    use std::marker::PhantomData;
    
            #[derive(Serialize, Deserialize, Default, Debug, Clone, CheckDynamicSize)]
        pub struct DynamicStruct
        // <T> 
        // where 
        // T: Serialize + Default+Send,
        {
        // T:  for<'de> Deserialize<'de> + Default+Send,
            // _marker: PhantomData<T>,
            my_usize: usize,
            my_u64: u64,
            my_u32: u32,
            my_u16: u16,
            my_u8: u8,
            my_boolean: bool,
            // my_string: String,
            // my_vec: Vec<u64>,
            // my_vec: T,
            // my_vec: Vec<usize>,
            // my_vec: Vec<u64>,
            // my_array: [usize],
        }
        
        impl
        // <T> 
        DynamicStruct
        // <T>
        // where 
        // T: Clone+Send,
        {
            fn hello(&self ) {
                println!("hello");
            } 
        } 



#[test]
fn test_dynamic_add_one() {
    let my_obj:DynamicStruct   = DynamicStruct {
            my_usize: 443,
            my_u64: 53,
            my_u32: 4399,
            my_u16: 3306,
            my_u8: 22,
            my_boolean: true,
            // my_string: "good luck!".to_string(),
            // my_vec: vec!["hello".to_string(), "world".to_string()],
            // my_vec: vec![1,2,3,4,5],
            // my_array: [1,2,3,4,5],
        };
        let my_service = ObjectPersistOnDiskService:: < DynamicStruct> ::new("TestDynamicData.bin".to_string(), "TestDynamicDataDynamic.bin".to_string(), 1024).unwrap();
        my_service.add(my_obj);
}

#[test]
fn test_dynamic_add_string() {
    let my_obj: DynamicStruct   = DynamicStruct {
            my_usize: 443,
            my_u64: 53,
            my_u32: 4399,
            my_u16: 3306,
            my_u8: 22,
            my_boolean: true,
            // my_string: "good luck!".to_string(),
            // my_vec: vec!["hello".to_string(), "world".to_string()],
            // my_vec: vec!["hello".to_string(), "world".to_string()],
            // my_array: [1,2,3,4,5],
        };
        let my_service = ObjectPersistOnDiskService:: < DynamicStruct> ::new("TestDynamicData.bin".to_string(), "TestDynamicDataDynamic.bin".to_string(), 1024).unwrap();
        my_service.add(my_obj);
}

#[test]
fn test_dynamic_add_one_by_one() {
    let mut my_vec = vec![1,2,3,4,5];
    let mut objs = Vec::new();
    let my_service = ObjectPersistOnDiskService:: < DynamicStruct> ::new("TestDynamicData.bin".to_string(), "TestDynamicDataDynamic.bin".to_string(), 1024).unwrap();
    for i in 0..COUNT {
    let my_obj: DynamicStruct   = DynamicStruct {
            my_usize: 443,
            my_u64: 53,
            my_u32: 4399,
            my_u16: 3306,
            my_u8: 22,
            my_boolean: true,
            // my_string: "good luck!".to_string(),
            // my_vec: vec!["hello".to_string(), "world".to_string()],
            // my_vec: my_vec.clone(),
            // my_array: [1,2,3,4,5],
        };
        // my_vec.push(i as u64 *1000);

        objs.push(my_obj.clone());
        my_service.add(my_obj);
    }
        // let my_service = ObjectPersistOnDiskService:: < DynamicStruct> ::new("TestDynamicData.bin".to_string(), "TestDynamicDataDynamic.bin".to_string(), 1024).unwrap();
        // my_service.add_bulk(objs);
}

#[test]
fn test_static_add_bulk() {
    let mut my_vec = vec![1,2,3,4,5];
    let mut objs = Vec::new();
    let my_service = ObjectPersistOnDiskService:: < DynamicStruct> ::new("TestDynamicData.bin".to_string(), "TestDynamicDataDynamic.bin".to_string(), 1024).unwrap();
    for i in 0..COUNT {
    let my_obj: DynamicStruct   = DynamicStruct {
            my_usize: 443,
            my_u64: 53,
            my_u32: 4399,
            my_u16: 3306,
            my_u8: 22,
            my_boolean: true,
            // my_string: "good luck!".to_string(),
            // my_vec: vec!["hello".to_string(), "world".to_string()],
            // my_vec: my_vec.clone(),
            // my_array: [1,2,3,4,5],
        };
        // my_vec.push(i as u64 *1000);

        objs.push(my_obj.clone());
    }
        my_service.add_bulk(objs);
}
#[test]
fn test_static_read_bulk() {

    let my_service = ObjectPersistOnDiskService:: < DynamicStruct> ::new("TestDynamicData.bin".to_string(), "TestDynamicDataDynamic.bin".to_string(), 1024).unwrap();
my_service.read_bulk(0, COUNT);
}

}

