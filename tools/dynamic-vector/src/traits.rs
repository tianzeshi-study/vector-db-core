pub trait DynamicVector {
    fn is_dynamic_type(&self) -> bool;
    fn get_dynamic_fields(&self) -> Vec<String>;
}