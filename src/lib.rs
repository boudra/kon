#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

pub mod arrow;
pub mod avro;
pub mod csv;
pub mod deserializer;
pub mod erlang;
pub mod error;
pub mod excel;
pub mod flatten;
pub mod html;
pub mod io;
pub mod json;
pub mod reader;
pub mod sql;
pub mod util;
pub mod xml;

pub use crate::erlang::ReaderOptions;
pub use crate::erlang::WriterOptions;
pub use crate::error::Error;
pub use crate::error::Result;
pub use crate::reader::DataType;
pub use crate::reader::Field;
pub use crate::reader::Schema;
pub use crate::reader::Value;
pub use crate::reader::ValueReader as Reader;

// #[test]
// fn csv_test() -> Result<()> {
//     use std::time::Instant;

//     let mut reader = crate::reader::new_reader(
//         "/Users/moboudra/Downloads/e9f4c80e-1fb5-4f4e-b8ac-c476ea99fd9f",
//         crate::ReaderOptions::Csv(crate::csv::reader::Options {
//             schema: None,
//             has_headers: false,
//             encoding: None,
//             delimiter: b',',
//         }),
//     )?;

//     let start = Instant::now();
//     let mut last = start;

//     let mut reader = reader.into_rows()?;
//     println!("{:?}", reader.fields());

//     while let Some(value) = reader.next()? {
//         println!("{:?}", value.collect::<Vec<_>>());
//     }

//     println!("finished in {:?}ms", start.elapsed().as_millis());

//     Ok(())
// }
