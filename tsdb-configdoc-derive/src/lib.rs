use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, Fields, LitStr, parse_macro_input};

#[proc_macro_derive(ConfigDoc, attributes(configdoc))]
pub fn derive_config_doc(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = &input.ident;

    let fields = match input.data {
        syn::Data::Struct(data_struct) => match data_struct.fields {
            Fields::Named(fields_named) => fields_named.named,
            _ => panic!("ConfigDoc requires named fields"),
        },
        _ => panic!("ConfigDoc can only be derived for structs"),
    };

    let field_lines = fields.iter().map(|field| {
        let field_name = field.ident.as_ref().unwrap();
        let field_name_str = field_name.to_string();

        let mut description_field = syn::LitStr::new("", field_name.span());
        let mut long_description_field = syn::LitStr::new("", field_name.span());

        for attr in &field.attrs {
            if attr.path().is_ident("configdoc") {
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("description") {
                        let value: LitStr = meta.value()?.parse()?;
                        description_field = value;
                    } else if meta.path.is_ident("long_description") {
                        let value: LitStr = meta.value()?.parse()?;
                        long_description_field = value;
                    }
                    Ok(())
                })
                .expect("Failed to parse #[configdoc] attribute");
            }
        }

        let long_desc_quote = if long_description_field.value().is_empty() {
            quote! {}
        } else {
            let long_desc_string = long_description_field.value();

            let desc_lines = long_desc_string.lines().map(|line| {
                quote! {
                    lines.push(format!("# {}", #line));
                }
            });

            quote! {
                lines.push("#".to_string());
                #(#desc_lines)*
                lines.push("#".to_string());
            }
        };

        quote! {
            lines.push(format!("# {}", #description_field));
            #long_desc_quote
            lines.push(format!("{} = {:?}", #field_name_str, &self.#field_name));
            lines.push(String::new());
        }
    });

    let expanded = quote! {
        impl #struct_name {
            pub fn to_documented_toml(&self) -> String {
                let mut lines = Vec::new();
                lines.push("# Sample Configuration for new TSDB".to_string());
                lines.push(String::new());
                #(#field_lines)*
                lines.join("\n")
            }
        }
    };

    TokenStream::from(expanded)
}
