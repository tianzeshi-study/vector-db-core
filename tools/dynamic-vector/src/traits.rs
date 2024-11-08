pub trait DynamicVector {
    fn is_dynamic_structure(&self) -> bool;
    fn get_dynamic_fields(&self) -> Vec<String>;
    fn get_dynamic_values(&self) -> Vec<Vec<u8>>;
    }
    
    pub trait VectorCandidate {
    /// 将类型转换为小端字节数组，返回一个 Vec<u8>
    fn to_bytes_vector(&self) -> Vec<u8>;
}

// 为原生整数类型 usize 实现特征
impl VectorCandidate for usize {
    fn to_bytes_vector(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

// 为原生整数类型 isize 实现特征
impl VectorCandidate for isize {
    fn to_bytes_vector(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

// 为其他整型类型实现特征
macro_rules! impl_to_bytes_for_integers {
    ($($t:ty),*) => {
        $(
            impl VectorCandidate for $t {
                fn to_bytes_vector(&self) -> Vec<u8> {
                    self.to_le_bytes().to_vec()
                }
            }
        )*
    };
}

// 批量实现 u64, u32, u16, u8, i64, i32, i16, i8 的特征
impl_to_bytes_for_integers!(u64, u32, u16, u8, i64, i32, i16, i8);

// 为布尔类型实现特征
impl VectorCandidate for bool {
    fn to_bytes_vector(&self) -> Vec<u8> {
        vec![*self as u8]
    }
}

// 为 Vec<T> 实现特征，要求 T 实现 VectorCandidate 特征
impl<T: VectorCandidate> VectorCandidate for Vec<T> {
    fn to_bytes_vector(&self) -> Vec<u8> {
        // 将 Vec<T> 中的每个元素转换为字节数组，然后合并到一个 Vec<u8>
        self.iter()
            .flat_map(|item| item.to_bytes_vector()) // 展开每个元素的 Vec<u8>
            .collect()
    }
}


// 为 String 实现 VectorCandidate
impl VectorCandidate for String {
    fn to_bytes_vector(&self) -> Vec<u8> {
        self.as_bytes().to_vec() // 将字符串的字节数组直接返回
    }
}

// 为 [T] 实现 VectorCandidate，要求 T 实现 VectorCandidate
impl<T: VectorCandidate> VectorCandidate for [T] {
    fn to_bytes_vector(&self) -> Vec<u8> {
        self.iter()
            .flat_map(|item| item.to_bytes_vector()) // 对每个元素调用 to_bytes_vector
            .collect()
    }
}

// 为 &[T] 实现 VectorCandidate，要求 T 实现 VectorCandidate
impl<T: VectorCandidate> VectorCandidate for &[T] {
    fn to_bytes_vector(&self) -> Vec<u8> {
        self.iter()
            .flat_map(|item| item.to_bytes_vector()) // 对每个元素调用 to_bytes_vector
            .collect()
    }
}

