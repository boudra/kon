use crate::reader::ValueReader;
use crate::{Result, Value};
use std::io::Write;

pub fn write_value<W: Write>(writer: &mut W, buf: &mut String, value: &Value) -> Result<()> {
    match value {
        Value::Int(i) => write!(writer, "{}", i),
        Value::String(s) => html_escape::encode_text_to_writer(s, writer),
        Value::Float(s) => write!(writer, "{}", s),
        Value::Bool(true) => writer.write_all(b"true"),
        Value::Bool(false) => writer.write_all(b"false"),
        Value::Object(o) => {
            unsafe { serde_json::to_writer(buf.as_mut_vec(), &o)? };
            html_escape::encode_text_to_writer(buf, writer)
        }
        Value::Array(o) => {
            unsafe { serde_json::to_writer(buf.as_mut_vec(), &o)? };
            html_escape::encode_text_to_writer(buf, writer)
        }
        Value::Null => write!(writer, "NULL"),
        Value::Binary(_) => todo!(),
    }
    .map_err(Into::into)
}

pub fn write<W: Write, R: ValueReader>(mut writer: W, reader: R) -> Result<()> {
    let mut buf = String::new();
    let mut rows = reader.into_rows()?;
    let ws = regex::Regex::new(r"[^\w]+").unwrap();

    writeln!(writer, r#"<?xml version="1.0" encoding="UTF-8" ?>"#)?;
    writeln!(writer, "<root>")?;

    let fields = rows
        .fields()
        .iter()
        .map(|f| ws.replace_all(&f.name, "_").into_owned())
        .collect::<Vec<_>>();

    while let Some(row) = rows.next()? {
        writeln!(writer, "  <row>")?;
        for (value, field) in row.zip(&fields) {
            write!(writer, "    <{}>", field)?;
            write_value(&mut writer, &mut buf, &value?)?;
            writeln!(writer, "</{}>", field)?;
        }
        writeln!(writer, "  </row>")?;
    }

    writeln!(writer, "</root>")?;

    Ok(())
}
