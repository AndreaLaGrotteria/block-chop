use crate::Message;

pub mod auctions;
pub mod payments;
pub mod pixel_war;

pub enum Application {
    Random,
    Auction,
    Payments,
    PixelWar,
}

pub fn create_message(application: Application) -> fn(u64, u64) -> Message {
    match application {
        Application::Random => |_, _| -> Message { rand::random() },
        Application::Payments => |source, directory_size| -> Message {
            payments::Payment::generate(source, directory_size, 10)
                .to_message()
                .1
        },
        Application::Auction => |source, _| -> Message {
            auctions::Request::generate(source, source / 65536, 10)
                .to_message()
                .1
        },
        Application::PixelWar => {
            |source, _| -> Message { pixel_war::Paint::random(source).to_message().1 }
        }
    }
}
