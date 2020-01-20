use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{parse_macro_input, AttributeArgs, Ident, ItemFn};

pub fn test_dis_impl(attr: TokenStream, input: TokenStream) -> TokenStream {
    let attributes_meta: syn::AttributeArgs = parse_macro_input!(attr as AttributeArgs);
    let attrs = match read_attrs(&attributes_meta) {
        Ok(attrs) => attrs,
        Err(err) => return err.to_compile_error().into(),
    };

    let mut test_function = parse_macro_input!(input as ItemFn);
    let test_fn_impl_name = Ident::new(&format!("{}_impl", test_function.sig.ident), Span::call_site());
    let test_fn_name = std::mem::replace(&mut test_function.sig.ident, test_fn_impl_name.clone());
    let test_fn_name_str = test_fn_name.to_string();

    let (optional_logging_import, optional_logging) = if let Some(log) = attrs.log {
        (
            Some(quote!(
                use tracing_futures::WithSubscriber;
            )),
            Some(quote!(.with_subscriber(test_setup::logging::test_tracing_subscriber(#log)))),
        )
    } else {
        (None, None)
    };

    let output = quote! {
        #[test]
        fn #test_fn_name() {
            #optional_logging_import

            let fut = async {
                let api = test_api(#test_fn_name_str).await;
                #test_fn_impl_name(api).await
            }#optional_logging;

            test_setup::runtime::run_with_tokio(fut).unwrap()
        }


        #test_function
    };

    output.into()
}

struct TestDisAttrs {
    log: Option<String>,
}

fn read_attrs(meta: &syn::AttributeArgs) -> Result<TestDisAttrs, syn::Error> {
    let mut attrs = TestDisAttrs { log: None };

    for attr in meta {
        match attr {
            syn::NestedMeta::Lit(_) => panic!("this is lit"),
            syn::NestedMeta::Meta(syn::Meta::NameValue(namevalue)) => {
                match (namevalue.path.get_ident().map(|ident| ident), &namevalue.lit) {
                    (Some(ident), syn::Lit::Str(s)) if ident == "log" => {
                        attrs.log = Some(s.value());
                    }
                    (Some(ident), _) => return Err(syn::Error::new_spanned(ident, "unknown attribute")),
                    (None, _) => return Err(syn::Error::new_spanned(&namevalue.path, "non-ident path")),
                }
            }
            attr => {
                return Err(syn::Error::new_spanned(
                    attr,
                    format!("unexpected attribute {:?}", attr),
                ))
            }
        }
    }

    Ok(attrs)
}
