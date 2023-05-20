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

    writeln!(writer, "<table>")?;
    writeln!(writer, "  <thead>")?;
    writeln!(writer, "    <tr>")?;

    let mut rows = reader.into_rows()?;

    for field in rows.fields() {
        write!(writer, "      <td>")?;
        write_value(
            &mut writer,
            &mut buf,
            &Value::String(std::borrow::Cow::Borrowed(&field.name)),
        )?;
        writeln!(writer, "</td>")?;
    }

    writeln!(writer, "    </tr>")?;
    writeln!(writer, "  </thead>")?;

    writeln!(writer, "  <tbody>")?;
    while let Some(row) = rows.next()? {
        writeln!(writer, "    <tr>")?;

        for value in row {
            buf.clear();
            write!(writer, "      <td>")?;
            write_value(&mut writer, &mut buf, &value?)?;
            writeln!(writer, "</td>")?;
        }
        writeln!(writer, "    </tr>")?;
    }

    writeln!(writer, "  </tbody>")?;
    writeln!(writer, "</table>")?;

    Ok(())
}
