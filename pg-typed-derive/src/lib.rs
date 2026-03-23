//! Derive macro for `FromRow` trait.
//!
//! Usage:
//! ```ignore
//! #[derive(FromRow)]
//! struct User {
//!     id: i32,
//!     name: String,
//!     #[from_row(rename = "email_address")]
//!     email: String,
//!     bio: Option<String>,
//! }
//! ```

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Data, Fields, LitStr};

#[proc_macro_derive(FromRow, attributes(from_row))]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => panic!("FromRow only supports structs with named fields"),
        },
        _ => panic!("FromRow only supports structs"),
    };

    let field_extractions = fields.iter().map(|field| {
        let field_name = field.ident.as_ref().unwrap();
        let field_type = &field.ty;

        // Check for #[from_row(rename = "...")] attribute.
        let col_name = get_rename_attr(field)
            .unwrap_or_else(|| field_name.to_string());

        // Check if the field type is Option<T>.
        if is_option_type(field_type) {
            quote! {
                #field_name: row.get_opt_by_name(#col_name)?
            }
        } else {
            quote! {
                #field_name: row.get_by_name(#col_name)?
            }
        }
    });

    let expanded = quote! {
        impl #impl_generics pg_typed::FromRow for #name #ty_generics #where_clause {
            fn from_row(row: &pg_typed::Row) -> Result<Self, pg_typed::TypedError> {
                Ok(Self {
                    #(#field_extractions,)*
                })
            }
        }
    };

    TokenStream::from(expanded)
}

/// Extract the `rename = "..."` value from `#[from_row(rename = "...")]`.
fn get_rename_attr(field: &syn::Field) -> Option<String> {
    for attr in &field.attrs {
        if !attr.path().is_ident("from_row") {
            continue;
        }
        let mut rename_value = None;
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                let value = meta.value()?;
                let s: LitStr = value.parse()?;
                rename_value = Some(s.value());
            }
            Ok(())
        }).ok();
        if let Some(v) = rename_value {
            return Some(v);
        }
    }
    None
}

/// Check if a type is `Option<T>`.
fn is_option_type(ty: &syn::Type) -> bool {
    if let syn::Type::Path(type_path) = ty {
        if let Some(seg) = type_path.path.segments.last() {
            return seg.ident == "Option";
        }
    }
    false
}
