use crate::{
    reader::{Value, ValueReader},
    Error, Result,
};

use std::io::{Read, Seek};

use apache_avro::Reader as AvroReader;

pub struct Reader<'a, R> {
    reader: AvroReader<'a, R>,
    value: Option<apache_avro::types::Value>,
}

#[derive(Debug, Default, serde::Deserialize)]
pub struct Options {}

impl<'a, R: Seek + Read + Send + Sync> ValueReader for Reader<'a, R> {
    fn next(&mut self) -> Result<Option<Value<'_>>> {
        if let Some(avro_value) = self.reader.next().transpose()? {
            self.value = Some(avro_value);

            apache_avro::from_value::<Value>(self.value.as_ref().unwrap())
                .map(Some)
                .map_err(Error::from)
        } else {
            Ok(None)
        }
    }

    fn reset(&mut self) {
        self.reader.reset().unwrap();
    }
}

impl<'a, R: Read + Send + Sync> Reader<'a, R> {
    pub fn new(reader: R, _opts: Options) -> Result<Self> {
        Ok(Self {
            reader: AvroReader::new(reader)?,
            value: None,
        })
    }
}

// #[test]
// fn avro_test() {
//     let path = "/Users/moboudra/Downloads/twitter.avro";
//     let file = File::open(path).unwrap();
//     let buf_reader = std::io::BufReader::new(file);

//     let mut reader = Reader::new(buf_reader, Default::default()).unwrap();

//     println!("{:?}", reader.next().unwrap());
// }
