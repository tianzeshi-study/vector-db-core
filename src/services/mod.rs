mod file_access_service;
mod string_repository;
pub mod dynamic_vector_manage_service;
pub mod static_vector_manage_service;
pub mod object_persist_on_disk_service;

pub use dynamic_vector_manage_service::*;
pub use static_vector_manage_service::*;
pub use self::object_persist_on_disk_service::*;
