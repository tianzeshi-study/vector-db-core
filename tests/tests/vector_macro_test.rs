#[cfg(test)]
mod test {
    use vector_db_core::*;
    use serde::{Serialize, Deserialize};

    
    
    #[derive(Serialize, Deserialize, Default, Debug, Clone, CheckDynamicSize)]
        pub struct ExampleStruct {
            my_number: usize,
            // my_string: String,
            my_vec: Vec<usize>,
            // my_vec: Vec<V>,
            // my_boolean: bool,
        }
        
        


#[test]
fn test_dynamic_macro() {
    let i =10;
    let example = ExampleStruct {
        my_number: i,
        // my_string: format!("hello, {i} world!").to_string(),
        my_vec: vec![i],
        // my_numbers: vec![1, 2, 3],
        // name: String::from("Example"),
        // fixed_number: 42,
    };
    
    assert!(example.is_dynamic_structure());

    // example.check_dynamic_fields();
    let result: Vec<String> = example.get_dynamic_fields();
    println!("{:?}", result);
    
    // let values_result: Vec<String> = example.get_dynamic_values();
    let values_result: Vec<Vec<u8>> = example.get_dynamic_values();
    println!("{:?}", values_result);
    let field_map = example.get_dynamic_map();
    println!("{:?}", field_map);
}
   

}