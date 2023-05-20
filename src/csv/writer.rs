use crate::reader::ValueReader;
use crate::{Result, Value};
use std::io::Write;

#[derive(Debug, serde::Deserialize)]
pub struct Options {
    pub delimiter: u8,
}

pub fn write_value(value: &Value, buf: &mut Vec<u8>) -> Result<()> {
    match value {
        Value::Int(i) => write!(buf, "{}", i).map_err(Into::into),
        Value::String(s) => write!(buf, "{}", s).map_err(Into::into),
        Value::Float(s) => write!(buf, "{}", s).map_err(Into::into),
        Value::Bool(s) => write!(buf, "{}", s).map_err(Into::into),
        Value::Object(o) => serde_json::to_writer(buf, &o).map_err(Into::into),
        Value::Array(o) => serde_json::to_writer(buf, &o).map_err(Into::into),
        Value::Null => Ok(()),
        Value::Binary(_) => todo!(),
    }
}

pub fn write<W: Write, R: ValueReader>(inner: W, reader: R, opts: Options) -> Result<()> {
    let mut rows = reader.into_rows()?;
    let mut writer = ::csv::WriterBuilder::new()
        .delimiter(opts.delimiter)
        .from_writer(inner);

    let mut buf = vec![];

    for f in rows.fields() {
        buf.clear();
        write_value(
            &Value::String(std::borrow::Cow::Borrowed(&f.name)),
            &mut buf,
        )?;
        writer.write_field(&buf)?;
    }

    writer.write_record(None::<&[u8]>)?;

    while let Some(row) = rows.next()? {
        for value in row {
            buf.clear();
            write_value(&value?, &mut buf)?;
            writer.write_field(&buf)?;
        }
        writer.write_record(None::<&[u8]>)?;
    }
    Ok(())
}
