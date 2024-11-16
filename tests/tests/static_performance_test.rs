#[cfg(test)]
mod test {
    use serde::{Deserialize, Serialize};
    use vector_db_core::*;

    const COUNT: usize = 1000000;

    #[derive(Serialize, Deserialize, Default, Debug, Clone, CheckDynamicSize)]
    pub struct StaticStruct
    {
        my_usize: usize,
        my_u64: u64,
        my_u32: u32,
        my_u16: u16,
        my_u8: u8,
        my_boolean: bool,
    }

    impl StaticStruct
    // <T>
    // where
    // T: Clone+Send,
    {
        fn _hello(&self) {
            println!("hello");
        }
    }

    #[test]
    fn test_dynamic_add_one() {
        let my_obj: StaticStruct = StaticStruct {
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
        let my_service = ObjectPersistOnDiskService::<StaticStruct>::new(
            "TestDynamicData.bin".to_string(),
            "TestDynamicDataDynamic.bin".to_string(),
            1024,
        )
        .unwrap();
        my_service.add(my_obj);
    }

    #[test]
    fn test_dynamic_add_string() {
        let my_obj: StaticStruct = StaticStruct {
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
        let my_service = ObjectPersistOnDiskService::<StaticStruct>::new(
            "TestDynamicData.bin".to_string(),
            "TestDynamicDataDynamic.bin".to_string(),
            1024,
        )
        .unwrap();
        my_service.add(my_obj);
    }

    #[test]
    fn test_dynamic_add_only_one_by_one() {
        let mut objs = Vec::new();
        let my_service = ObjectPersistOnDiskService::<StaticStruct>::new(
            "TestDynamicData.bin".to_string(),
            "TestDynamicDataDynamic.bin".to_string(),
            1024,
        )
        .unwrap();
        for i in 0..10 {
            let my_obj: StaticStruct = StaticStruct {
                my_usize: 443 + i,
                my_u64: 53 + i as u64,
                my_u32: 4399 + i as u32,
                my_u16: 3306 + i as u16,
                my_u8: 22 + i as u8,
                my_boolean: true,
            };
            // my_vec.push(i as u64 *1000);

            objs.push(my_obj.clone());
            my_service.add(my_obj);
        }
        // let my_service = ObjectPersistOnDiskService:: < StaticStruct> ::new("TestDynamicData.bin".to_string(), "TestDynamicDataDynamic.bin".to_string(), 1024).unwrap();
        // my_service.add_bulk(objs);
    }

    #[test]
    fn test_static_add_bulk() {
        let mut objs = Vec::new();
        let my_service = ObjectPersistOnDiskService::<StaticStruct>::new(
            "TestDynamicData.bin".to_string(),
            "TestDynamicDataDynamic.bin".to_string(),
            1024,
        )
        .unwrap();
        for i in 0..COUNT {
            let my_obj: StaticStruct = StaticStruct {
                my_usize: 443 + i,
                my_u64: 53,
                my_u32: 4399,
                my_u16: 3306,
                my_u8: 22,
                my_boolean: true,
            };
            // my_vec.push(i as u64 *1000);

            objs.push(my_obj.clone());
        }
        my_service.add_bulk(objs);
    }
    #[test]
    fn test_static_read_bulk() {
        let my_service = ObjectPersistOnDiskService::<StaticStruct>::new(
            "TestDynamicData.bin".to_string(),
            "TestDynamicDataDynamic.bin".to_string(),
            1024,
        )
        .unwrap();
        my_service.read_bulk(0, COUNT);
    }
}
