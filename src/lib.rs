#![feature(linked_list_cursors)]

mod cache;
mod vector_engine;

mod services;

pub use cache::{ReadableCache,WritableCache};
pub use services::{
    dynamic_vector_manage_service::*,
    // object_persist_on_disk_service::*,
    static_vector_manage_service::*,
};
pub use vector_engine::VectorEngine;

pub use dynamic_vector::{
    CheckDynamicSize,
    DynamicVector,
    VectorCandidate,
};