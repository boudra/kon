use crate::{
    error::Error,
    error::Result,
    reader::{self, Object, Value},
    DataType,
};

use std::{borrow::Cow, collections::HashSet, io::Cursor};

pub struct Reader {
    index: usize,
    range: calamine::Range<calamine::DataType>,
    headers: Vec<String>,
}

#[derive(Debug, serde::Deserialize)]
pub enum Format {
    #[serde(rename = "xls")]
    Xls,
    #[serde(rename = "xlsx")]
    Xlsx,
    #[serde(rename = "ods")]
    Ods,
}

#[derive(Debug, serde::Deserialize)]
pub struct Options {
    pub sheet_name: String,
    pub format: Format,
}

impl Reader {
    pub fn new<T: AsRef<[u8]>>(data: T, options: Options) -> Result<Self> {
        use calamine::{Ods, Reader, Sheets, Xls, Xlsx};
        let cursor = Cursor::new(data);

        let mut reader = match options.format {
            Format::Ods => calamine::open_workbook_from_rs::<Ods<_>, _>(cursor)
                .map(Sheets::Ods)
                .map_err(|x| Error::CustomError(format!("{}", x))),
            Format::Xls => calamine::open_workbook_from_rs::<Xls<_>, _>(cursor)
                .map(Sheets::Xls)
                .map_err(|x| Error::CustomError(format!("{}", x))),
            Format::Xlsx => calamine::open_workbook_from_rs::<Xlsx<_>, _>(cursor)
                .map(Sheets::Xlsx)
                .map_err(|x| Error::CustomError(format!("{}", x))),
        }?;

        let range = if let Some(range) = reader.worksheet_range(&options.sheet_name) {
            range.map_err(|x| Error::CustomError(format!("{}", x)))?
        } else {
            return Err(Error::CustomError(format!(
                "Sheet with the name `{}` not found",
                options.sheet_name
            )));
        };

        let _types: Vec<HashSet<DataType>> = vec![HashSet::new(); range.width()];
        let headers = match range.rows().next() {
            None => vec![],
            Some(row) => row
                .iter()
                .enumerate()
                .map(|(i, value)| match value {
                    calamine::DataType::String(s) => s.clone(),
                    _ => format!("column_{}", i),
                })
                .collect(),
        };

        Ok(Self {
            headers,
            index: 1,
            range,
        })
    }
}

impl reader::ValueReader for Reader {
    fn reset(&mut self) {
        self.index = 1;
    }

    fn next(&mut self) -> Result<Option<Value<'_>>> {
        if self.index >= self.range.height() {
            return Ok(None);
        }

        let row = self
            .headers
            .iter()
            .map(|h| Cow::Borrowed(h.as_str()))
            .zip(self.range[self.index].iter().map(|value| match value {
                calamine::DataType::Empty => Value::Null,
                calamine::DataType::String(s) => Value::String(std::borrow::Cow::Borrowed(s)),
                calamine::DataType::Float(f) | calamine::DataType::DateTime(f) => Value::Float(*f),
                calamine::DataType::Int(i) => Value::Int(*i),
                calamine::DataType::Error(_) => Value::Null,
                calamine::DataType::Bool(b) => Value::Bool(*b),
            }))
            .collect::<Object>();

        self.index += 1;

        Ok(Some(Value::Object(row)))
    }
}
