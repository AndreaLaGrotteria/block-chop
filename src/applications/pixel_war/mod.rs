mod color;
mod coordinates;
mod paint;
mod processor;
mod processor_settings;

pub use color::Color;
pub use coordinates::Coordinates;
pub use paint::Paint;
pub use processor::Processor;
pub use processor_settings::ProcessorSettings;

pub const CANVAS_EDGE: usize = 2048;
