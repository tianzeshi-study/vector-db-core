#![feature(linked_list_cursors)]

// mod dynamic_vector_manage_service;
// mod static_vector_manage_service;
// mod file_access_service;
// mod object_persist_on_disk_service;
// mod string_repository;

mod database_with_cache;
mod vector_engine;

pub mod services;

// pub use dynamic_vector_manage_service::*;
// pub use object_persist_on_disk_service::*;
// pub use static_vector_manage_service::*;
pub use database_with_cache::*;
pub use vector_engine::*;