use proc_macro::TokenStream; // 用于定义过程宏输入和输出的类型
use quote::quote; // 用于将 Rust 代码片段转换成可编译的代码块
use syn::{parse_macro_input, Data, DeriveInput, Fields, Type}; // 解析 Rust 语法树和类型



// 定义一个过程宏 `CheckDynamicSize`，它自动为标记的结构体生成代码，检查每个字段是否是动态大小类型
#[proc_macro_derive(CheckDynamicSize)]
pub fn check_dynamic_size_derive(input: TokenStream) -> TokenStream {
    // 解析输入的语法树，将过程宏输入（TokenStream）解析为 DeriveInput 数据结构
    let input = parse_macro_input!(input as DeriveInput);

    // 获取结构体的名称
    let name = input.ident;
    
    let dynamic_field_map = if let Data::Struct(data) = input.data.clone() {
    if let Fields::Named(fields) = data.fields {
        // 生成动态字段名称，只收集动态大小字段
        let names: Vec<_> = fields.named.iter().map(|f| {
            let field_name = &f.ident;
            let field_type = &f.ty; // 获取字段类型
            // let vec_type = Type::Verbatim(quote! { Vec<usize> });


            quote! {

            // if stringify!(#field_type) == stringify!(#vec_type) {
            if is_dynamic_size::<#field_type>(&self.#field_name) {

                // println!("type:{:?}", stringify!(#field_type));
                // let value = self.#field_name.to_bytes_vector().clone();


(stringify!(#field_name), self.#field_name.to_bytes_vector())
// (stringify!(#field_name),value )
// self.#field_name.to_bytes_vector()
            } else {
                // "".to_string()
                // (&#field_name, self.#field_name.to_bytes_vector().clone())
                ("None", vec![0 as u8])

            }
        }
            // }
        })
        .collect();

        quote! { vec![#(#names),*] } // 返回一个字符串数组
    } else {
        quote! { vec![] } // 如果没有具名字段，返回空数组
    }
} else {
    quote! { vec![] } // 如果输入数据不是结构体，返回空数组
};




    let dynamic_field_values = if let Data::Struct(data) = input.data.clone() {
    if let Fields::Named(fields) = data.fields {
        // 生成动态字段名称，只收集动态大小字段
        let names: Vec<_> = fields.named.iter().map(|f| {
            let field_name = &f.ident;
            let field_type = &f.ty; // 获取字段类型
            let vec_type = Type::Verbatim(quote! { Vec<usize> });


            quote! {

            // if stringify!(#field_type) == stringify!(#vec_type) {
            if is_dynamic_size::<#field_type>(&self.#field_name) {

                // println!("type:{:?}", stringify!(#field_type));


self.#field_name.to_bytes_vector()
            } else {

                vec![0 as u8]

            }
        }
            // }
        })
        .filter( |item|  !item.is_empty())
        .collect();

        quote! { vec![#(#names),*] } // 返回一个字符串数组
    } else {
        quote! { vec![] } // 如果没有具名字段，返回空数组
    }
} else {
    quote! { vec![] } // 如果输入数据不是结构体，返回空数组
};




    let dynamic_field_names = if let Data::Struct(data) = input.data {
    if let Fields::Named(fields) = data.fields {
        // 生成动态字段名称，只收集动态大小字段
        let names: Vec<_> = fields.named.iter().map(|f| {
            let field_name = &f.ident;
            let field_type = &f.ty; // 获取字段类型
            // 检查字段是否为动态大小类型
            quote! {
            if is_dynamic_size::<#field_type>(&self.#field_name) {
                // Some(quote! { stringify!(#field_name).to_string() }) // 只有动态大小字段的名称被收集
                stringify!(#field_name).to_string() 
            } else {
                "".to_string() 
            }
        }
        })
        .filter( |item|  !item.is_empty())
        .collect();

        quote! { vec![#(#names),*] } // 返回一个字符串数组
    } else {
        quote! { vec![] } // 如果没有具名字段，返回空数组
    }
} else {
    quote! { vec![] } // 如果输入数据不是结构体，返回空数组
};



    // 使用 `quote!` 宏生成最终代码：为结构体实现 `check_dynamic_fields` 方法
    let expanded = quote! {
            use std::collections::HashMap;
        trait DynamicSized {fn is_dynamic_size(&self) -> bool;}

impl DynamicSized for bool {fn is_dynamic_size(&self) -> bool {false}}
        impl DynamicSized for usize {fn is_dynamic_size(&self) -> bool {false}}
        impl DynamicSized for u8 {fn is_dynamic_size(&self) -> bool {false}}
        impl DynamicSized for u16 {fn is_dynamic_size(&self) -> bool {false}}
        impl DynamicSized for u32 {fn is_dynamic_size(&self) -> bool {false}}
        impl DynamicSized for u64 {fn is_dynamic_size(&self) -> bool {false}}
        impl DynamicSized for i8 {fn is_dynamic_size(&self) -> bool {false}}
        impl DynamicSized for i16 {fn is_dynamic_size(&self) -> bool {false}}
        impl DynamicSized for i32 {fn is_dynamic_size(&self) -> bool {false}}
        impl DynamicSized for i64 {fn is_dynamic_size(&self) -> bool {false}}

        impl DynamicSized for str {fn is_dynamic_size(&self) -> bool {true}}
        impl DynamicSized for &str {fn is_dynamic_size(&self) -> bool {true}}
        impl DynamicSized for String {fn is_dynamic_size(&self) -> bool {true}}

        impl<T> DynamicSized for Vec<T> {fn is_dynamic_size(&self) -> bool {true}}
        impl<T> DynamicSized for [T] {fn is_dynamic_size(&self) -> bool {true}}
        impl<T> DynamicSized for &[T] {fn is_dynamic_size(&self) -> bool {true}}

        fn is_dynamic_size<T: DynamicSized>(value: &T) -> bool {
    value.is_dynamic_size()
}




        impl DynamicVector for #name {

            fn is_dynamic_structure(&self) -> bool {
                let check = self.get_dynamic_fields();
                if   check.len() != 0 {
                    return true;
                } else {
                    return false ;
                }
            }

fn get_dynamic_fields(&self) -> Vec<String> {
#dynamic_field_names.iter()
.filter( |item| !item.is_empty())
.cloned()
.collect()
}

fn get_dynamic_values(&self) -> Vec<Vec<u8>> {
#dynamic_field_values
.iter()
.filter( |item| !item.is_empty())
.cloned()
.collect()
}
        }
        
        impl #name{
fn get_dynamic_map(&self) -> HashMap<&str, Vec<u8>> {
    #dynamic_field_map
    .iter()
    .cloned()
    .collect::<HashMap<&str, Vec<u8>>>()
}

        }
    };

    // 将生成的代码转换为 TokenStream，以便 Rust 编译器进一步处理
    TokenStream::from(expanded)
}


