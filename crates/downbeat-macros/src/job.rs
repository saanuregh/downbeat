//! `#[job]` and `#[command]`: a function becomes a handle that implements `downbeat::Job`.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{FnArg, GenericArgument, ItemFn, PathArguments, ReturnType, Type};

use crate::policy::{self, Arguments};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Function,
    Command,
}

/// The parts of the function's signature the engine cares about.
struct Signature {
    input: Option<Type>,
    dependencies: Vec<Type>,
    output: Type,
}

pub(crate) fn expand(attribute: TokenStream, item: TokenStream, kind: Kind) -> syn::Result<TokenStream> {
    let arguments = policy::parse(attribute)?;
    let function: ItemFn = syn::parse2(item)?;
    let signature = match kind {
        Kind::Function => function_signature(&function)?,
        Kind::Command => command_signature(&function)?,
    };
    if !function.sig.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(&function.sig.generics, "a job function cannot be generic"));
    }

    let handle = &function.sig.ident;
    let visibility = &function.vis;
    let attributes = &function.attrs;
    let name = arguments.name.as_ref().map_or_else(|| handle.to_string(), syn::LitStr::value);
    let input_type = signature.input.clone().unwrap_or_else(|| syn::parse_quote!(()));
    let output_type = &signature.output;
    let policy_function = policy::policy_function(&arguments);
    let turn_value = turn_value(&arguments, &input_type);

    let mut call = function.clone();
    call.sig.ident = format_ident!("call");
    call.vis = syn::parse_quote!(pub);
    call.attrs.retain(|attribute| attribute.path().is_ident("allow"));
    call.attrs.push(syn::parse_quote!(#[allow(unknown_lints, clippy::unused_async, clippy::unused_async_trait_impl)]));

    let erased = match kind {
        Kind::Function => function_parts(&function, &signature, &input_type, output_type, &turn_value),
        Kind::Command => command_parts(&function, &signature, &input_type, output_type, &turn_value),
    };

    let documentation = format!("The `{name}` job. Refer to it by this handle in pipelines.");
    Ok(quote! {
        #(#attributes)*
        #[doc = #documentation]
        #[allow(non_camel_case_types)]
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        #visibility struct #handle;

        impl #handle {
            /// The job's function, as written.
            #[doc(hidden)]
            #call

            #policy_function
        }

        #erased

        ::downbeat::__private::inventory::submit! {
            ::downbeat::__private::Registered { name: #name }
        }

        impl ::downbeat::Job for #handle {
            type Input = #input_type;
            type Output = #output_type;
            const NAME: &'static str = #name;

            fn spec() -> ::downbeat::JobSpec {
                Self::__spec()
            }
        }
    })
}

/// `async fn name(run: Run, input: In, dep: &Dep, …) -> Result<Out>`
fn function_signature(function: &ItemFn) -> syn::Result<Signature> {
    if function.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            function.sig.fn_token,
            "a #[job] function is async; a #[command] function is not",
        ));
    }
    let mut parameters = function.sig.inputs.iter();
    match parameters.next() {
        Some(FnArg::Typed(first)) if last_segment(&first.ty).is_some_and(|segment| segment == "Run") => {}
        _ => {
            return Err(syn::Error::new_spanned(
                &function.sig,
                "a job function takes `run: Run` first, then its input (if any), then dependencies by reference",
            ));
        }
    }
    let (input, dependencies) = inputs_and_dependencies(parameters)?;
    let output = result_output(&function.sig.output, "Result")?;
    Ok(Signature { input, dependencies, output })
}

/// `fn name(input: In) -> Program<Out>`
fn command_signature(function: &ItemFn) -> syn::Result<Signature> {
    if function.sig.asyncness.is_some() {
        return Err(syn::Error::new_spanned(
            function.sig.asyncness,
            "a #[command] function only describes the program, so it is not async",
        ));
    }
    let (input, dependencies) = inputs_and_dependencies(function.sig.inputs.iter())?;
    if let Some(dependency) = dependencies.first() {
        return Err(syn::Error::new_spanned(
            dependency,
            "a #[command] function takes only its input; the program gets what it needs from its environment",
        ));
    }
    let output = result_output(&function.sig.output, "Program")?;
    Ok(Signature { input, dependencies, output })
}

fn inputs_and_dependencies<'a>(parameters: impl Iterator<Item = &'a FnArg>) -> syn::Result<(Option<Type>, Vec<Type>)> {
    let mut input = None;
    let mut dependencies = Vec::new();
    for parameter in parameters {
        let FnArg::Typed(typed) = parameter else {
            return Err(syn::Error::new_spanned(parameter, "a job is a free function, not a method"));
        };
        match &*typed.ty {
            Type::Reference(reference) => {
                if reference.mutability.is_some() {
                    return Err(syn::Error::new_spanned(reference, "dependencies are shared: take them as `&T`"));
                }
                dependencies.push((*reference.elem).clone());
            }
            ty if input.is_none() && dependencies.is_empty() => input = Some(ty.clone()),
            ty => {
                return Err(syn::Error::new_spanned(
                    ty,
                    "a job or command takes one input, by value, before its dependencies; a job with several parents takes them as one tuple",
                ));
            }
        }
    }
    Ok((input, dependencies))
}

/// `Out` from a return type written as `Wrapper<Out>`.
fn result_output(output: &ReturnType, wrapper: &str) -> syn::Result<Type> {
    let message = format!("the function must return `{wrapper}<Output>`");
    let ReturnType::Type(_, ty) = output else {
        return Err(syn::Error::new_spanned(output, message));
    };
    let Type::Path(path) = &**ty else {
        return Err(syn::Error::new_spanned(ty, message));
    };
    let segment = path.path.segments.last().ok_or_else(|| syn::Error::new_spanned(ty, &message))?;
    if segment.ident != wrapper {
        return Err(syn::Error::new_spanned(ty, message));
    }
    match &segment.arguments {
        PathArguments::AngleBracketed(generics) => match generics.args.first() {
            Some(GenericArgument::Type(output)) => Ok(output.clone()),
            _ => Err(syn::Error::new_spanned(ty, message)),
        },
        _ => Err(syn::Error::new_spanned(ty, message)),
    }
}

fn last_segment(ty: &Type) -> Option<String> {
    match ty {
        Type::Path(path) => path.path.segments.last().map(|segment| segment.ident.to_string()),
        _ => None,
    }
}

fn turn_value(arguments: &Arguments, input_type: &Type) -> TokenStream {
    let Some(field) = &arguments.one_per else { return quote!(::core::option::Option::None) };
    quote! {
        ::core::option::Option::Some(|value: &::downbeat::__private::Value| {
            let input: #input_type = ::downbeat::__private::decode_value(value)?;
            ::downbeat::__private::field_text(&input.#field)
        })
    }
}

fn function_parts(
    function: &ItemFn,
    signature: &Signature,
    input_type: &Type,
    output_type: &Type,
    turn_value: &TokenStream,
) -> TokenStream {
    let handle = &function.sig.ident;
    let visibility = &function.vis;
    let dependency_names: Vec<_> =
        (0..signature.dependencies.len()).map(|index| format_ident!("__dependency_{index}")).collect();
    let dependency_types = &signature.dependencies;
    let (decode, input_argument, input_parameter) = if signature.input.is_some() {
        (
            quote!(let __input: #input_type = ::downbeat::__private::decode_input(__input)?;),
            quote!(__input,),
            quote!(input: #input_type,),
        )
    } else {
        (quote!(let _ = __input;), quote!(), quote!())
    };
    let test_input = if signature.input.is_some() { quote!(input,) } else { quote!() };

    quote! {
        impl #handle {
            /// Calls the function directly, in this process, for a test.
            #visibility async fn test(
                self,
                run: ::downbeat::Run,
                #input_parameter
                #(#dependency_names: &#dependency_types),*
            ) -> ::downbeat::Outcome<#output_type> {
                ::downbeat::Outcome::from(Self::call(run, #test_input #(#dependency_names),*).await)
            }

            fn __execute(
                run: ::downbeat::Run,
                __input: ::downbeat::__private::Value,
                __dependencies: ::downbeat::__private::Arc<::downbeat::Dependencies>,
            ) -> ::downbeat::__private::BoxFuture<'static, ::downbeat::Result<::downbeat::__private::Value>> {
                ::std::boxed::Box::pin(async move {
                    #decode
                    #(let #dependency_names = __dependencies.get::<#dependency_types>().await?;)*
                    let output = Self::call(run, #input_argument #(&*#dependency_names),*).await?;
                    ::downbeat::__private::encode_output(&output)
                })
            }

            fn __spec() -> ::downbeat::JobSpec {
                ::downbeat::JobSpec::function::<Self>(
                    Self::__policy(),
                    vec![#(::downbeat::Dependency::of::<#dependency_types>()),*],
                    #turn_value,
                    Self::__execute,
                )
            }
        }
    }
}

fn command_parts(
    function: &ItemFn,
    signature: &Signature,
    input_type: &Type,
    output_type: &Type,
    turn_value: &TokenStream,
) -> TokenStream {
    let handle = &function.sig.ident;
    let visibility = &function.vis;
    let (decode, argument, parameter) = if signature.input.is_some() {
        (
            quote!(let input: #input_type = ::downbeat::__private::decode_value(&input)?;),
            quote!(input),
            quote!(input: #input_type),
        )
    } else {
        (quote!(let _ = input;), quote!(), quote!())
    };
    quote! {
        impl #handle {
            /// The program this command would start for an input, for a test.
            #visibility fn test(self, #parameter) -> ::downbeat::ProgramSpec {
                Self::call(#argument).into_spec()
            }

            fn __program(input: ::downbeat::__private::Value) -> ::core::result::Result<::downbeat::ProgramSpec, ::std::string::String> {
                #decode
                let program: ::downbeat::Program<#output_type> = Self::call(#argument);
                ::core::result::Result::Ok(program.into_spec())
            }

            fn __spec() -> ::downbeat::JobSpec {
                ::downbeat::JobSpec::command::<Self>(Self::__policy(), #turn_value, Self::__program)
            }
        }
    }
}
