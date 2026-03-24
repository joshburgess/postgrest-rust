//! Derive macro for PostgreSQL domain types (newtypes over a base type).
//!
//! ```ignore
//! #[derive(PgDomain)]
//! struct Email(String);
//! ```
//!
//! The inner type must implement Encode, Decode, DecodeText, and PgType.
//! All trait impls delegate to the inner type, with `type_oid` returning
//! `Unspecified` so the server infers the domain's OID from context.

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields};

pub fn derive(input: DeriveInput) -> TokenStream {
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let inner_type = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                &fields.unnamed.first().unwrap().ty
            }
            _ => panic!(
                "PgDomain requires a tuple struct with exactly one field, e.g. `struct {}(String)`",
                name
            ),
        },
        _ => panic!("PgDomain only supports tuple structs"),
    };

    let expanded = quote! {
        impl #impl_generics pg_typed::Encode for #name #ty_generics #where_clause {
            fn type_oid(&self) -> pg_typed::TypeOid {
                pg_typed::TypeOid::Unspecified
            }

            fn encode(&self, buf: &mut pg_typed::BytesMut) {
                pg_typed::Encode::encode(&self.0, buf);
            }
        }

        impl #impl_generics pg_typed::Decode for #name #ty_generics #where_clause {
            fn decode(buf: &[u8]) -> Result<Self, pg_typed::TypedError> {
                Ok(Self(<#inner_type as pg_typed::Decode>::decode(buf)?))
            }
        }

        impl #impl_generics pg_typed::DecodeText for #name #ty_generics #where_clause {
            fn decode_text(s: &str) -> Result<Self, pg_typed::TypedError> {
                Ok(Self(<#inner_type as pg_typed::DecodeText>::decode_text(s)?))
            }
        }

        impl #impl_generics pg_typed::PgType for #name #ty_generics #where_clause {
            const OID: u32 = 0;
            const ARRAY_OID: u32 = 0;
        }
    };

    TokenStream::from(expanded)
}
