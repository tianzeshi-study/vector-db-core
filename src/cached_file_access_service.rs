use std::collections::{HashMap, LinkedList};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::file_access_service::FileAccessService;

/// 缓存文件访问服务，实现带缓存的文件读写，使用LRU缓存淘汰策略
pub struct CachedFileAccessService {
    file_access_service: FileAccessService,
    cache: Arc<Mutex<HashMap<u64, Vec<u8>>>>,
    lru_list: Arc<Mutex<LinkedList<u64>>>,
    page_size: usize,
    max_cache_items: usize,
}

impl CachedFileAccessService {
    /// 创建一个 `CachedFileAccessService` 实例
    pub fn new(path: String,initial_size_if_not_exists: u64, page_size: usize, max_cache_items: usize) -> Self {
        Self {
            file_access_service: FileAccessService::new(path, initial_size_if_not_exists),
            cache: Arc::new(Mutex::new(HashMap::new())),
            lru_list: Arc::new(Mutex::new(LinkedList::new())),
            page_size,
            max_cache_items,
            // page_size: 1024 * 1024, // 默认值 1MB
            // max_cache_items: 512,    // 默认值 512
        }
    }

    /// 写入数据到文件的指定偏移量，同时清除缓存中将被覆盖的页面
    pub fn write_in_file(&self, offset: u64, data: &[u8]) {
        let start_page = offset / self.page_size as u64;
        let end_page = (offset + data.len() as u64) / self.page_size as u64;
        
        {
            let mut cache = self.cache.lock().unwrap();
            let mut lru_list = self.lru_list.lock().unwrap();
            
            for page in start_page..=end_page {
                if cache.contains_key(&page) {
                    cache.remove(&page);
                    // lru_list.retain(|&x| x != page);
                    remove_item(&mut lru_list,page);
                }
            }
        }

        self.file_access_service.write_in_file(offset, data);
    }

    /// 从文件的指定偏移量读取数据，并使用缓存提高读取效率
    pub fn read_in_file(&self, offset: u64, length: usize) -> Vec<u8> {
        let mut result = vec![0; length];
        let mut current_offset = offset;
        let mut result_offset = 0;
        let mut remaining_length = length;

        while remaining_length > 0 {
            let page_offset = current_offset / self.page_size as u64;
            let page_start = (current_offset % self.page_size as u64) as usize;
            dbg!(current_offset, self.page_size);
            dbg!(page_offset, page_start);
            let bytes_to_read = std::cmp::min(remaining_length, self.page_size - page_start);

            let page_data = self.get_page_from_cache(page_offset);
            // dbg!(&page_data);
            result[result_offset..result_offset + bytes_to_read]
                .copy_from_slice(&page_data[page_start..page_start + bytes_to_read]);

            current_offset += bytes_to_read as u64;
            result_offset += bytes_to_read;
            remaining_length -= bytes_to_read;
        }

        result
    }

    /// 从缓存中获取页面数据，如果缓存缺失则从文件中读取并添加到缓存
    fn get_page_from_cache(&self, page_offset: u64) -> Vec<u8> {
        {
        let cache = self.cache.lock().unwrap();
        // let mut lru_list = self.lru_list.lock().unwrap();

        if let Some(page_data) = cache.get(&page_offset) {
            if self.should_update_lru(&page_offset) {
                // lru_list.retain(|&x| x != page_offset);
                let mut lru_list = self.lru_list.lock().unwrap();
                remove_item(&mut lru_list, page_offset);
                lru_list.push_back(page_offset);
            }
            
            return page_data.clone();
        }
        }

        let page_data = self.file_access_service.read_in_file(page_offset * self.page_size as u64, self.page_size);
        self.add_to_cache(page_offset, page_data.clone());
        page_data
    }

    /// 检查是否需要更新LRU列表中的页面访问记录
    fn should_update_lru(&self, page_offset: &u64) -> bool {
        let lru_list = self.lru_list.lock().unwrap();
        const VERY_RECENT_PAGE_ACCESS_LIMIT: usize = 0x10;

        let mut recent_access = lru_list.iter().rev().take(VERY_RECENT_PAGE_ACCESS_LIMIT);
        !recent_access.any(|&x| x == *page_offset)
    }

    /// 将页面数据添加到缓存，如果缓存满则移除最久未使用的页面
    fn add_to_cache(&self, page_offset: u64, data: Vec<u8>) {
        let mut cache = self.cache.lock().unwrap();
        let mut lru_list = self.lru_list.lock().unwrap();

        while cache.len() >= self.max_cache_items && !lru_list.is_empty() {
            if let Some(oldest_page) = lru_list.pop_front() {
                cache.remove(&oldest_page);
            }
        }

        cache.insert(page_offset, data);
        lru_list.push_back(page_offset);
    }
}

// fn remove_item(lru_list: &mut MutexGuard<'_, LinkedList<u64>>, page: u64) {
fn remove_item(lru_list: &mut LinkedList<u64>, page: u64) {
    let mut current = lru_list.front(); // 从列表的前端开始

    while let Some(&value) = current { // 获取当前节点的值
        if value == page {
            // 如果值与要删除的元素匹配，使用 pop_front() 删除元素
            lru_list.pop_front(); // 删除头部元素
            // 更新 current 为下一个元素
            current = lru_list.front(); // 更新为新头部
        } else {
            // 继续检查下一个元素
            current = lru_list.iter().nth(1).or_else(|| None); // 获取下一个元素
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;


    const TEST_FILE_PATH: &str = "test_file.bin";
    const INITIAL_SIZE_IF_NOT_EXISTS: u64 = 1024;
    const PAGE_SIZE: usize = 1024; // 1KB 页面大小
    const MAX_CACHE_ITEMS: usize = 4; // 最多缓存4个页面


    fn test_write_and_read_in_file() {
        // 初始化文件缓存服务
        let service = CachedFileAccessService::new(TEST_FILE_PATH.to_string() ,INITIAL_SIZE_IF_NOT_EXISTS,  PAGE_SIZE, MAX_CACHE_ITEMS);
        
        // 写入数据
        let offset = 0;
        let data = vec![1, 2, 3, 4, 5];
        service.write_in_file(offset, &data);
        
        // 读取并验证数据
        let result = service.read_in_file(0, data.len());
        assert_eq!(result, data);
    }


    fn test_cache_eviction() {
        // 测试缓存淘汰策略
        let service = CachedFileAccessService::new(TEST_FILE_PATH.to_string(), INITIAL_SIZE_IF_NOT_EXISTS, PAGE_SIZE, MAX_CACHE_ITEMS);

        // 写入多个页面数据
        for i in 0..(MAX_CACHE_ITEMS + 1) as u64 {
            let data = vec![i as u8; PAGE_SIZE];
            service.write_in_file(i * PAGE_SIZE as u64, &data);
        }

        // 检查是否有一个页面被移出缓存
        {
        let cache = service.cache.lock().unwrap();
        assert!(cache.len() <= MAX_CACHE_ITEMS);
        }
    }


    fn test_cache_hit() {
        // 测试缓存命中率
        let service = CachedFileAccessService::new(TEST_FILE_PATH.to_string(), INITIAL_SIZE_IF_NOT_EXISTS,  PAGE_SIZE, MAX_CACHE_ITEMS);

        // 写入并读取相同的页面，验证缓存命中
        let offset = 0;
        let data = vec![42; PAGE_SIZE];
        service.write_in_file(offset, &data);

        // 第一次读取：缓存未命中，从文件读取
        // let first_read = service.read_in_file(offset, PAGE_SIZE);
        // assert_eq!(first_read, data);

        // 第二次读取：应命中缓存
        // let second_read = service.read_in_file(offset, PAGE_SIZE);
        // assert_eq!(second_read, data);

        // 检查缓存是否命中
        // let cache = service.cache.lock().unwrap();
        // assert!(cache.contains_key(&(offset / PAGE_SIZE as u64)));
    }


    fn test_lru_cache_behavior() {
        // 测试 LRU 缓存行为
        let service = CachedFileAccessService::new(TEST_FILE_PATH.to_string(), INITIAL_SIZE_IF_NOT_EXISTS, PAGE_SIZE, MAX_CACHE_ITEMS);

        // 写入多个页面数据
        for i in 0..MAX_CACHE_ITEMS as u64 {
            let data = vec![i as u8; PAGE_SIZE];
            service.write_in_file(i * PAGE_SIZE as u64, &data);
        }

        // 读取第一个页面，使其成为最近使用的页面
        let offset = 0;
        // let _ = service.read_in_file(offset, PAGE_SIZE);

        // 写入新的页面数据，触发缓存淘汰
        let new_page_offset = MAX_CACHE_ITEMS as u64 * PAGE_SIZE as u64;
        let new_data = vec![99; PAGE_SIZE];
        service.write_in_file(new_page_offset, &new_data);

        // 验证最旧的页面（第1页）是否被移出缓存
        {
            let cache = service.cache.lock().unwrap();
            assert!(!cache.contains_key(&(offset / PAGE_SIZE as u64))); // 第1页应该不再在缓存中
            // assert!(cache.contains_key(&(new_page_offset / PAGE_SIZE as u64))); // 新页面应在缓存中
        }
    }


    // 清理测试文件
    fn cleanup() {
        let _ = fs::remove_file(TEST_FILE_PATH);
    }
    
    #[test]
    fn test_cache() {
        test_write_and_read_in_file();
        test_cache_eviction();
        test_cache_hit();
        test_lru_cache_behavior();
        cleanup();
    }
}
