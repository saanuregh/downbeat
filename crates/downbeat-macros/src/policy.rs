//! The arguments of `#[job(…)]` and `#[command(…)]`: policies, and the job's name.

use proc_macro2::TokenStream;
use quote::quote;
use syn::punctuated::Punctuated;
use syn::{Expr, Ident, LitStr, Meta, Token};

/// What the attribute says.
pub(crate) struct Arguments {
    pub(crate) name: Option<LitStr>,
    /// For `one_per = field`: the field.
    pub(crate) one_per: Option<Ident>,
    /// Statements that set the policies on `policy`.
    pub(crate) settings: Vec<TokenStream>,
    pub(crate) preset: Option<Expr>,
}

const FLAGS: &[&str] = &["fresh_process", "one_at_a_time", "supports_dry_run"];
const VALUES: &[&str] = &[
    "name",
    "preset",
    "queue",
    "priority",
    "timeout",
    "attempts",
    "backoff",
    "retry_on",
    "recheck_every",
    "give_up_after",
    "one_per",
    "max_memory",
    "keep_runs_for",
    "keep_runs_at_most",
    "expect_success_every",
];

pub(crate) fn parse(attribute: TokenStream) -> syn::Result<Arguments> {
    let metas = syn::parse::Parser::parse2(Punctuated::<Meta, Token![,]>::parse_terminated, attribute)?;
    let mut arguments = Arguments { name: None, one_per: None, settings: Vec::new(), preset: None };
    let mut seen = Vec::new();
    for meta in metas {
        let key = meta.path().get_ident().map(ToString::to_string).unwrap_or_default();
        if seen.contains(&key) {
            return Err(syn::Error::new_spanned(meta.path(), format!("`{key}` is set twice")));
        }
        seen.push(key.clone());
        match meta {
            Meta::Path(path) => {
                let setting = match key.as_str() {
                    "fresh_process" => quote!(policy.fresh_process = true;),
                    "supports_dry_run" => quote!(policy.supports_dry_run = true;),
                    "one_at_a_time" => quote!(policy.turn = ::downbeat::Turn::OneAtATime;),
                    _ => return Err(unknown(&path, &key)),
                };
                arguments.settings.push(setting);
            }
            Meta::NameValue(pair) => {
                let value = pair.value;
                let setting = match key.as_str() {
                    "name" => {
                        arguments.name = Some(syn::parse2(quote!(#value))?);
                        continue;
                    }
                    "preset" => {
                        arguments.preset = Some(value);
                        continue;
                    }
                    "one_per" => {
                        let field: Ident = syn::parse2(quote!(#value)).map_err(|_| {
                            syn::Error::new_spanned(&value, "one_per names a field of the input: `one_per = broker_id`")
                        })?;
                        let text = field.to_string();
                        arguments.one_per = Some(field);
                        quote!(policy.turn = ::downbeat::Turn::OnePer(#text);)
                    }
                    "queue" => quote!(policy.queue = #value;),
                    "priority" => quote!(policy.priority = #value;),
                    "timeout" => quote!(policy.timeout = #value;),
                    "attempts" => quote!(policy.attempts = #value;),
                    "backoff" => quote!(policy.backoff = ::downbeat::Backoff::from(#value);),
                    "retry_on" => retry_on(&value)?,
                    "recheck_every" => quote!(policy.recheck_every = #value;),
                    "give_up_after" => quote!(policy.give_up_after = #value;),
                    "max_memory" => quote!(policy.max_memory = ::core::option::Option::Some(#value);),
                    "keep_runs_for" => quote!(policy.keep_runs_for = #value;),
                    "keep_runs_at_most" => quote!(policy.keep_runs_at_most = ::core::option::Option::Some(#value);),
                    "expect_success_every" => {
                        quote!(policy.expect_success_every = ::core::option::Option::Some(#value);)
                    }
                    _ => return Err(unknown(&pair.path, &key)),
                };
                arguments.settings.push(setting);
            }
            Meta::List(list) => return Err(unknown(&list.path, &key)),
        }
    }
    Ok(arguments)
}

/// `retry_on = [Error, Timeout]`.
fn retry_on(value: &Expr) -> syn::Result<TokenStream> {
    let Expr::Array(array) = value else {
        return Err(syn::Error::new_spanned(value, "write retry_on as a list: `retry_on = [Error, Timeout]`"));
    };
    let mut kinds = Vec::new();
    for element in &array.elems {
        let kind: Ident = syn::parse2(quote!(#element))
            .map_err(|_| syn::Error::new_spanned(element, "retry_on lists Error, Timeout, Crash or Memory"))?;
        if !["Error", "Timeout", "Crash", "Memory"].contains(&kind.to_string().as_str()) {
            return Err(syn::Error::new_spanned(
                element,
                "retry_on lists Error, Timeout, Crash or Memory; a panic is never retried",
            ));
        }
        kinds.push(quote!(::downbeat::RetryOn::#kind));
    }
    Ok(quote!(policy.retry_on = ::downbeat::RetryOnSet::of(&[#(#kinds),*]);))
}

fn unknown(path: &syn::Path, key: &str) -> syn::Error {
    let mut known: Vec<&str> = FLAGS.iter().chain(VALUES).copied().collect();
    known.sort_unstable();
    syn::Error::new_spanned(path, format!("`{key}` is not a policy; the policies are: {}", known.join(", ")))
}

/// The function that builds the job's policy.
pub(crate) fn policy_function(arguments: &Arguments) -> TokenStream {
    let base = arguments.preset.as_ref().map_or_else(|| quote!(::downbeat::Policy::DEFAULT), |preset| quote!(#preset));
    let settings = &arguments.settings;
    quote! {
        fn __policy() -> ::downbeat::Policy {
            #[allow(unused_imports)]
            use ::downbeat::{BytesExt as _, DurationExt as _};
            #[allow(unused_imports)]
            use ::downbeat::presets::*;
            #[allow(unused_mut)]
            let mut policy: ::downbeat::Policy = #base;
            #(#settings)*
            policy
        }
    }
}
