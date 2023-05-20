use crate::reader::ValueReader;
use crate::{DataType, Field, Result, Value};

use std::io::Write;

#[derive(Debug, serde::Deserialize)]
pub enum Syntax {
    #[serde(rename = "postgres")]
    Postgres,
    #[serde(rename = "mysql")]
    Mysql,
    #[serde(rename = "sqlite")]
    Sqlite,
}

#[derive(Debug, serde::Deserialize)]
pub struct Options {
    pub syntax: Syntax,
    pub create_table: bool,
    pub table_name: String,
}

pub fn write_value<W: Write>(writer: &mut W, buf: &mut String, value: &Value) -> Result<()> {
    match value {
        Value::Int(i) => write!(writer, "{}", i),
        Value::String(s) => write!(writer, "\'{}\'", s.replace('\'', "''")),
        Value::Float(s) => write!(writer, "{}", s),
        Value::Bool(true) => writer.write_all(b"TRUE"),
        Value::Bool(false) => writer.write_all(b"FALSE"),
        Value::Object(o) => {
            unsafe { serde_json::to_writer(buf.as_mut_vec(), &o)? };
            write!(writer, "\'{}\'", buf.replace('\'', "''"))
        }
        Value::Array(o) => {
            unsafe { serde_json::to_writer(buf.as_mut_vec(), &o)? };
            write!(writer, "\'{}\'", buf.replace('\'', "''"))
        }
        Value::Null => write!(writer, "NULL"),
        Value::Binary(_) => todo!(),
    }
    .map_err(Into::into)
}

fn write_identifier<W: Write>(writer: &mut W, syntax: &Syntax, id: &str) -> Result<()> {
    match syntax {
        Syntax::Mysql => write!(writer, "`{}`", id),
        Syntax::Postgres => write!(writer, "\"{}\"", id),
        Syntax::Sqlite => write!(writer, "[{}]", id),
    }
    .map_err(Into::into)
}

fn data_type(syntax: &Syntax, dt: &DataType) -> &'static str {
    match syntax {
        Syntax::Postgres => match dt {
            DataType::Object(_) => "json",
            DataType::Array(_) => "json",
            DataType::Int => "bigint",
            DataType::Float => "double precision",
            DataType::Bool => "boolean",
            DataType::String => "text",
            DataType::Binary => "bytea",
            DataType::Null => data_type(syntax, &DataType::String),
        },
        Syntax::Mysql => match dt {
            DataType::Object(_) => "JSON",
            DataType::Array(_) => "JSON",
            DataType::Int => "BIGINT",
            DataType::Float => "DOUBLE",
            DataType::Bool => "BOOLEAN",
            DataType::String => "VARCHAR(1024)",
            DataType::Binary => "BLOB",
            DataType::Null => data_type(syntax, &DataType::String),
        },
        Syntax::Sqlite => match dt {
            DataType::Object(_) => "TEXT",
            DataType::Array(_) => "TEXT",
            DataType::Int => "INT",
            DataType::Float => "REAL",
            DataType::Bool => "INT",
            DataType::String => "TEXT",
            DataType::Binary => "BLOB",
            DataType::Null => data_type(syntax, &DataType::String),
        },
    }
}

pub fn write_create_table<W: Write>(
    mut writer: &mut W,
    syntax: &Syntax,
    table_name: &str,
    fields: &Vec<Field>,
) -> Result<()> {
    let mut is_first = true;
    write!(writer, "CREATE TABLE ")?;
    write_identifier(&mut writer, syntax, table_name)?;
    writeln!(writer, " (")?;

    for field in fields {
        if is_first {
            is_first = false;
        } else {
            writer.write_all(b",\n")?;
        }

        writer.write_all(b"  ")?;
        write_identifier(&mut writer, syntax, &field.name)?;
        writer.write_all(b" ")?;
        writer.write_all(data_type(syntax, &field.data_type).as_bytes())?;
        if field.is_nullable {
            writer.write_all(b" NULL")?;
        }
    }

    writer.write_all(b"\n);\n\n")?;

    Ok(())
}

pub fn write<W: Write, R: ValueReader>(mut writer: W, reader: R, opts: Options) -> Result<()> {
    let mut rows = reader.into_rows()?;

    if opts.create_table {
        write_create_table(&mut writer, &opts.syntax, &opts.table_name, rows.fields())?;
    }

    writer.write_all(b"INSERT INTO ")?;
    write_identifier(&mut writer, &opts.syntax, &opts.table_name)?;
    writer.write_all(b" VALUES\n")?;

    let mut is_first_row = true;
    let mut buf = String::new();

    while let Some(row) = rows.next()? {
        let mut is_first_value = true;

        if is_first_row {
            is_first_row = false;
        } else {
            writer.write_all(b",\n")?;
        }

        writer.write_all(b"(")?;
        for value in row {
            let value = value?;

            if is_first_value {
                is_first_value = false;
            } else {
                writer.write_all(b",")?;
            }
            buf.clear();
            write_value(&mut writer, &mut buf, &value)?;
        }
        writer.write_all(b")")?;
    }

    writer.write_all(b";\n")?;

    Ok(())
}
