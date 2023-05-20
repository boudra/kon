use crate::reader::ValueReader;
use crate::Result;

use serde::Serialize;
use std::io::Write;

#[derive(Debug, serde::Deserialize)]
pub enum Mode {
    #[serde(rename = "normal")]
    ArrayCompact,
    #[serde(rename = "pretty")]
    ArrayPretty,
    #[serde(rename = "ndjson")]
    NdJson,
}

#[derive(Debug, serde::Deserialize)]
pub struct Options {
    pub mode: Mode,
}

pub fn write<W: Write, R: ValueReader>(mut inner: W, mut reader: R, opts: Options) -> Result<()> {
    match opts.mode {
        Mode::ArrayPretty => {
            inner.write_all(b"[\n")?;
            let mut is_first = true;

            while let Some(row) = reader.next()? {
                if is_first {
                    is_first = false;
                } else {
                    inner.write_all(b",\n")?;
                }

                let mut s = serde_json::Serializer::pretty(inner);

                row.serialize(&mut s)?;

                inner = s.into_inner();
            }

            inner.write_all(b"\n]\n")?;
        }
        Mode::ArrayCompact => {
            inner.write_all(b"[")?;
            let mut is_first = true;

            while let Some(row) = reader.next()? {
                if is_first {
                    is_first = false;
                } else {
                    inner.write_all(b",")?;
                }

                let mut s = serde_json::Serializer::new(inner);

                row.serialize(&mut s)?;
                inner = s.into_inner();
            }

            inner.write_all(b"]")?;
        }
        Mode::NdJson => {
            while let Some(row) = reader.next()? {
                let mut s = serde_json::Serializer::new(inner);
                row.serialize(&mut s)?;
                inner = s.into_inner();
                inner.write_all(b"\n")?;
            }
        }
    };

    Ok(())
}
