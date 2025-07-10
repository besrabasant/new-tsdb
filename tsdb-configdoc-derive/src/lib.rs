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

        let mut desc = syn::LitStr::new("", field_name.span());
        let mut long_desc = syn::LitStr::new("", field_name.span());

        for attr in &field.attrs {
            if attr.path().is_ident("configdoc") {
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("description") {
                        let value: LitStr = meta.value()?.parse()?;
                        desc = value;
                    } else if meta.path.is_ident("long_description") {
                        let value: LitStr = meta.value()?.parse()?;
                        long_desc = value;
                    }
                    Ok(())
                })
                .expect("Failed to parse #[configdoc] attribute");
            }
        }

        // Build long-description comments if present
        let long_desc_block = if !long_desc.value().is_empty() {
            let lines = long_desc
                .value()
                .lines()
                .map(|l| LitStr::new(l, field_name.span()))
                .collect::<Vec<_>>();
            quote! {
                lines.push("#".to_string());
                #(
                    lines.push(format!("# {}", #lines));
                )*
            }
        } else {
            quote! {}
        };

       // Use toml::to_string to serialize the field value exactly as Serde would
        quote! {
            lines.push(format!("# {}", #desc));
            #long_desc_block
            // Serialize field value via toml::Value for correct formatting
            let val = toml::Value::try_from(&self.#field_name)
                .map(|v| v.to_string())
                .unwrap_or_else(|_| format!("{:?}", &self.#field_name));
            lines.push(format!("{} = {}", #field_name_str, val));
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
