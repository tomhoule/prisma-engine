extern crate proc_macro;

use darling::FromMeta;
use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{parse_macro_input, AttributeArgs, Ident, ItemFn};

#[derive(Debug, FromMeta)]
struct TestOneConnectorArgs {
    /// The name of the connector to test.
    connector: String,

    #[darling(default)]
    log: Option<String>,
}

const CONNECTOR_NAMES: &[&'static str] = &[
    "mysql_8",
    "mysql",
    "postgres9",
    "postgres",
    "postgres11",
    "postgres12",
    "mysql_mariadb",
    "sqlite",
];

#[derive(Debug, FromMeta)]
struct TestEachConnectorArgs {
    /// Comma-separated list of connectors to exclude.
    #[darling(default)]
    ignore: Option<String>,

    #[darling(default)]
    starts_with: Option<String>,

    #[darling(default)]
    log: Option<String>,
}

impl TestEachConnectorArgs {
    fn connectors_to_test(&self) -> impl Iterator<Item = &&str> {
        let ignore = self.ignore.as_ref().map(String::as_str);
        let starts_with = self.starts_with.as_ref().map(String::as_str);

        CONNECTOR_NAMES
            .iter()
            .filter(move |connector_name| match ignore {
                Some(ignore) => !connector_name.starts_with(&ignore),
                None => true,
            })
            .filter(move |connector_name| match starts_with {
                Some(pat) => connector_name.starts_with(pat),
                None => true,
            })
    }
}

#[proc_macro_attribute]
pub fn test_each_connector(attr: TokenStream, input: TokenStream) -> TokenStream {
    let attributes_meta: syn::AttributeArgs = parse_macro_input!(attr as AttributeArgs);
    let args = TestEachConnectorArgs::from_list(&attributes_meta);

    let mut test_function = parse_macro_input!(input as ItemFn);
    strip_test_attribute(&mut test_function);
    let async_test: bool = test_function.sig.asyncness.is_some();

    let tests = match (args, async_test) {
        (Ok(args), false) => test_each_connector_wrapper_functions(&args, &test_function),
        (Ok(args), true) => test_each_connector_async_wrapper_functions(&args, &test_function),
        (Err(err), _) => panic!("{}", err),
    };

    let output = quote! {
        #(#tests)*

        #test_function
    };

    output.into()
}

fn test_each_connector_wrapper_functions(
    args: &TestEachConnectorArgs,
    test_function: &ItemFn,
) -> Vec<proc_macro2::TokenStream> {
    let test_fn_name = &test_function.sig.ident;
    let test_fn_name_str = format!("{}", test_fn_name);

    let mut tests = Vec::with_capacity(CONNECTOR_NAMES.len());

    for connector in args.connectors_to_test() {
        let connector_test_fn_name = Ident::new(&format!("{}_on_{}", test_fn_name, connector), Span::call_site());
        let connector_api_factory = Ident::new(&format!("{}_test_api", connector), Span::call_site());

        let test = quote! {
            #[test]
            fn #connector_test_fn_name() {
                let api = #connector_api_factory(#test_fn_name_str);
                #test_fn_name(&api)
            }
        };

        tests.push(test);
    }

    tests
}

fn test_each_connector_async_wrapper_functions(
    args: &TestEachConnectorArgs,
    test_function: &ItemFn,
) -> Vec<proc_macro2::TokenStream> {
    let test_fn_name = &test_function.sig.ident;
    let test_fn_name_str = format!("{}", test_fn_name);

    let mut tests = Vec::with_capacity(CONNECTOR_NAMES.len());

    let optional_logging_import = args.log.as_ref().map(|_| quote!(use tracing_futures::WithSubscriber;));
    let optional_logging = args.log.as_ref().map(|log_config| {
        quote! { .with_subscriber(test_setup::logging::test_tracing_subscriber(#log_config)) }
    });

    let optional_unwrap = if function_returns_result(&test_function) {
        Some(quote!(.unwrap()))
    } else {
        None
    };

    for connector in args.connectors_to_test() {
        let connector_test_fn_name = Ident::new(&format!("{}_on_{}", test_fn_name, connector), Span::call_site());
        let connector_api_factory = Ident::new(&format!("{}_test_api", connector), Span::call_site());

        let test = quote! {
            #[test]
            fn #connector_test_fn_name() {
                #optional_logging_import

                let fut = async {
                    let api = #connector_api_factory(#test_fn_name_str).await;
                    #test_fn_name(&api).await#optional_unwrap
                }#optional_logging;

                test_setup::runtime::run_with_tokio(fut)
            }
        };

        tests.push(test);
    }

    tests
}

#[proc_macro_attribute]
pub fn test_one_connector(attr: TokenStream, input: TokenStream) -> TokenStream {
    let attributes_meta: syn::AttributeArgs = parse_macro_input!(attr as AttributeArgs);
    let args = TestOneConnectorArgs::from_list(&attributes_meta).unwrap();

    let mut test_function = parse_macro_input!(input as ItemFn);
    strip_test_attribute(&mut test_function);

    let async_test: bool = test_function.sig.asyncness.is_some();
    let test_impl_name = &test_function.sig.ident;
    let test_impl_name_str = format!("{}", test_impl_name);
    let test_fn_name = Ident::new(
        &format!("{}_on_{}", &test_function.sig.ident, args.connector),
        Span::call_site(),
    );
    let api_factory = Ident::new(&format!("{}_test_api", args.connector), Span::call_site());
    let optional_unwrap = if function_returns_result(&test_function) {
        Some(quote!(.unwrap()))
    } else {
        None
    };

    let output = if async_test {
        let optional_logging_import = args.log.as_ref().map(|_| quote!(use tracing_futures::WithSubscriber;));
        let optional_logging = args.log.as_ref().map(|log_config| {
            quote! { .with_subscriber(test_setup::logging::test_tracing_subscriber(#log_config)) }
        });

        quote! {
            #[test]
            fn #test_fn_name() {
                #optional_logging_import

                let fut = async {
                    let api = #api_factory(#test_impl_name_str).await;
                    #test_impl_name(&api)#optional_logging.await#optional_unwrap
                };

                test_setup::runtime::run_with_tokio(fut)
            }

            #test_function
        }
    } else {
        quote! {
            #[test]
            fn #test_fn_name() {
                let api = #api_factory(#test_impl_name_str);

                #test_impl_name(&api)
            }

            #test_function
        }
    };

    output.into()
}

fn function_returns_result(func: &ItemFn) -> bool {
    match func.sig.output {
        syn::ReturnType::Default => false,
        // just assume it's a result
        syn::ReturnType::Type(_, _) => true,
    }
}

/// We do this because Intellij only recognizes functions annotated with #[test] *before* macro expansion as tests. This way we can add it manually, and the test macro will strip it.
fn strip_test_attribute(function: &mut ItemFn) {
    let new_attrs = function
        .attrs
        .drain(..)
        .filter(|attr| attr.path.segments.iter().last().unwrap().ident != "test")
        .collect();

    function.attrs = new_attrs;
}

#[proc_macro_attribute]
pub fn test_sled(_attr: TokenStream, input: TokenStream) -> TokenStream {
    let test_function = parse_macro_input!(input as ItemFn);

    let test_impl_name = &test_function.sig.ident;
    let test_impl_name_str = format!("{}", test_impl_name);

    let test_fn_name = Ident::new(&format!("{}_on_sled", &test_function.sig.ident), Span::call_site());

    let tokens = quote! {
        #[test]
        fn #test_fn_name() {
            let fut = async {
                let api = SledApi::new(#test_impl_name_str).await;
                #test_impl_name(&api).await
            };

            async_std::task::block_on(fut).unwrap();
        }

        #test_function
    };

    tokens.into()
}
