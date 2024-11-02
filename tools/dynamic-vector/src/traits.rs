pub trait DynamicVector {
    fn is_dynamic_structure(&self) -> bool;
    fn get_dynamic_fields(&self) -> Vec<String>;
}