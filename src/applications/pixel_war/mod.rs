mod paint;
mod processor;
mod processor_settings;

pub use processor::Processor;
pub use processor_settings::ProcessorSettings;

pub type Color = (u8, u8, u8);
use paint::Paint;
