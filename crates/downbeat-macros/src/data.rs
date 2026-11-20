//! `#[derive(JobData)]`.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, LitStr};

pub(crate) fn expand(item: TokenStream) -> syn::Result<TokenStream> {
    let input: DeriveInput = syn::parse2(item)?;
    let name = &input.ident;
    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();
    let type_name = name.to_string();

    let mut fields = Vec::new();
    let mut not_compared = Vec::new();
    if let Data::Struct(data) = &input.data
        && let Fields::Named(named) = &data.fields
    {
        for field in &named.named {
            let ident = field.ident.as_ref().expect("named field");
            let serialized = serde_name(field).unwrap_or_else(|| ident.to_string().trim_start_matches("r#").to_owned());
            let compared = !has_not_compared(field)?;
            if !compared {
                not_compared.push(serialized.clone());
            }
            let ty = &field.ty;
            let ty_text = quote!(#ty).to_string().replace(' ', "");
            fields.push(quote! {
                ::downbeat::FieldShape { name: #serialized.to_owned(), type_name: #ty_text.to_owned(), compared: #compared }
            });
        }
    } else if let Some(attribute) = input.attrs.iter().find(|a| a.path().is_ident("job_data")) {
        return Err(syn::Error::new_spanned(attribute, "#[job_data(…)] belongs on a field of a struct"));
    }

    let identity = (!not_compared.is_empty()).then(|| {
        quote! {
            fn identity(&self) -> ::downbeat::__private::Value {
                let value = ::downbeat::__private::to_value(self).unwrap_or(::downbeat::__private::Value::Null);
                ::downbeat::__private::without_fields(value, &[#(#not_compared),*])
            }
        }
    });

    Ok(quote! {
        impl #impl_generics ::downbeat::JobData for #name #type_generics #where_clause {
            #identity
            fn shape() -> ::downbeat::Shape {
                ::downbeat::Shape { type_name: #type_name.to_owned(), fields: vec![#(#fields),*] }
            }
        }
    })
}

fn has_not_compared(field: &syn::Field) -> syn::Result<bool> {
    let mut found = false;
    for attribute in field.attrs.iter().filter(|a| a.path().is_ident("job_data")) {
        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("not_compared") {
                found = true;
                Ok(())
            } else {
                Err(meta.error("the only option is #[job_data(not_compared)]"))
            }
        })?;
    }
    Ok(found)
}

/// The name serde writes for a field, when `#[serde(rename = "…")]` changes it.
fn serde_name(field: &syn::Field) -> Option<String> {
    let mut renamed = None;
    for attribute in field.attrs.iter().filter(|a| a.path().is_ident("serde")) {
        // Other serde options are serde's business; only `rename` matters here.
        let _ = attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                let value: LitStr = meta.value()?.parse()?;
                renamed = Some(value.value());
            } else if meta.input.peek(syn::Token![=]) {
                let _: syn::Expr = meta.value()?.parse()?;
            }
            Ok(())
        });
    }
    renamed
}
