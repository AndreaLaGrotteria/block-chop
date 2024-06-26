use proc_macro::TokenStream;
use std::env;

const PACKING_BYTES: usize = 128;

#[proc_macro]
pub fn message(_: TokenStream) -> TokenStream {
    let message_size = env::var("CHOP_CHOP_MESSAGE_SIZE").unwrap_or("8".to_string());

    let message_size: usize = message_size
        .parse()
        .expect("Environment variable `CHOP_CHOP_MESSAGE_SIZE` must be an integer.");

    if message_size % 8 != 0 {
        panic!("Environment variable `CHOP_CHOP_MESSAGE_SIZE` must be a multiple of 8.");
    }

    let packing = std::cmp::max(1, PACKING_BYTES / message_size);

    format!(
        "
            pub const MESSAGE_SIZE: usize = {message_size};
            pub(crate) const PACKING: usize = {packing}; 

            #[derive(Debug, Clone, Copy, PartialEq, Eq)]
            pub struct Message {{
                pub bytes: [u8; {message_size}]
            }}

            impl Default for Message {{
                fn default() -> Self {{ 
                    Message {{ bytes: [0u8; {message_size}] }} 
                }}
            }}
        "
    )
    .parse()
    .unwrap()
}
