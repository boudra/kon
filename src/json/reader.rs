use crate::{
    error::Error,
    reader::{Value, ValueReader},
    Result,
};

use serde::Deserialize;

pub struct RecordReader<R: AsRef<[u8]>> {
    offset: usize,
    reader: R,
    expect_end: bool,
    flatten_objects: bool,
}

#[derive(Debug, Default, serde::Deserialize)]
pub enum JsonMode {
    #[default]
    Auto,
    NdJson,
    Array,
}

#[derive(Debug, Default, serde::Deserialize)]
pub struct Options {
    #[serde(default)]
    pub mode: JsonMode,
    pub flatten_objects: bool,
}

pub fn read_value<'a, T: Deserialize<'a>>(
    reader: &mut &'a [u8],
    expect_end: &mut bool,
) -> Result<Option<T>> {
    if *expect_end {
        match read_skipping_ws(reader) {
            Some(b',') => {
                *reader = &reader[1..];
                deserialize_single(reader).map(Some)
            }
            Some(b']') => {
                *reader = &reader[1..];

                match read_skipping_ws(reader) {
                    Some(_) => {
                        *expect_end = false;
                        deserialize_single(reader).map(Some)
                    }
                    None => Ok(None),
                }
            }
            Some(other) => Err(invalid_data(
                format!(
                    "Expected `,` or `]` after a value, found `{}`",
                    *other as char
                )
                .as_str(),
            )),
            None => Err(invalid_data(
                "Expected `,` or `]` after a value, found end of file.",
            )),
        }
    } else {
        match read_skipping_ws(reader) {
            Some(b'[') => {
                *reader = &reader[1..];
                match read_skipping_ws(reader) {
                    Some(b']') => {
                        *reader = &reader[1..];
                        Ok(None)
                    }
                    // Some(other) => todo!("{}", other),
                    Some(_other) => {
                        *expect_end = true;
                        deserialize_single(reader).map(Some)
                    }
                    None => Err(invalid_data("Expected a JSON value, found end of stream")),
                }
            }
            Some(_) => deserialize_single(reader).map(Some),
            None => Ok(None),
        }
    }
}

impl<R: AsRef<[u8]> + Send + Sync> ValueReader for RecordReader<R> {
    fn next(&mut self) -> Result<Option<Value<'_>>> {
        let mut slice = &self.reader.as_ref()[self.offset..];

        let value = match read_value::<Value>(&mut slice, &mut self.expect_end) {
            Ok(Some(value)) => {
                if self.flatten_objects {
                    Ok(Some(value.flatten_object()))
                } else {
                    Ok(Some(value))
                }
            }
            ret => ret,
        };

        self.offset = self.reader.as_ref().len() - slice.len();

        value
    }

    fn reset(&mut self) {
        self.offset = 0;
        self.expect_end = false;
    }
}

impl<R: AsRef<[u8]> + Send + Sync + 'static> RecordReader<R> {
    pub fn new(inner: R, opts: Options) -> Result<Self> {
        Ok(Self {
            offset: 0,
            reader: inner,
            flatten_objects: opts.flatten_objects,
            expect_end: false,
        })
    }
}

fn read_skipping_ws<'a>(reader: &mut &'a [u8]) -> Option<&'a u8> {
    loop {
        let (byte, rem) = reader.split_first()?;
        if !byte.is_ascii_whitespace() {
            return Some(byte);
        }
        *reader = rem;
    }
}

fn invalid_data(msg: &str) -> Error {
    Error::CustomError(msg.to_string())
}

fn deserialize_single<'de, T: Deserialize<'de>>(reader: &mut &'de [u8]) -> Result<T> {
    let mut de = serde_json::Deserializer::from_reader(reader);

    de.disable_recursion_limit();

    T::deserialize(&mut de).map_err(|e| Error::CustomError(format!("{}", e)))
}
