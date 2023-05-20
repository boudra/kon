use std::{borrow::Cow, hash::Hash};

use crate::{error::Result, flatten::flatten, Error, ReaderOptions};
use indexmap::map::IndexMap;
use indexmap::set::IndexSet;
use serde::{Deserialize, Serialize};

pub type Map<K, V> = IndexMap<K, V>;
pub type Object<'a> = Map<Cow<'a, str>, Value<'a>>;

#[macro_export]
macro_rules! value {
    ($($json:tt)+) => {{
        serde_json::from_str::<Value>(stringify!($($json)+)).unwrap()
    }};
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(untagged)]
pub enum Value<'a> {
    Null,
    Bool(bool),
    String(Cow<'a, str>),
    Int(i64),
    Float(f64),
    Array(Vec<Value<'a>>),
    Object(Object<'a>),
    Binary(Vec<u8>),
}

impl<'a> Default for Value<'a> {
    fn default() -> Value<'a> {
        Value::Null
    }
}

impl<'a> Value<'a> {
    pub fn is_object(&self) -> bool {
        self.as_object().is_some()
    }

    pub fn as_object(&self) -> Option<&Map<Cow<'a, str>, Value<'a>>> {
        match self {
            Value::Object(map) => Some(map),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            Value::Float(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<&bool> {
        match self {
            Value::Bool(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<&Cow<'a, str>> {
        match self {
            Value::String(v) => Some(v),
            _ => None,
        }
    }

    pub fn flatten_object(self) -> Self {
        match self {
            Value::Object(map) => Value::Object(flatten(map)),
            other => other,
        }
    }

    pub fn coerce<'b: 'a>(&mut self, dt: &'b DataType, recursive: bool) -> Result<()> {
        coerce_value(self, dt, recursive)?;
        Ok(())
    }

    pub fn unwrap_object(&self) -> Result<&Object> {
        match self {
            Value::Object(map) => Ok(map),
            _ => Err("Expected value to be an object".into()),
        }
    }

    pub fn is_array(&self) -> bool {
        self.as_array().is_some()
    }

    pub fn as_array(&self) -> Option<&Vec<Value<'a>>> {
        match self {
            Value::Array(map) => Some(map),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(tag = "name", content = "children")]
pub enum DataType {
    Null,
    Bool,
    String,
    Binary,
    Int,
    Float,
    Array(Box<DataType>),
    Object(Vec<Field>),
}

impl DataType {
    pub fn unwrap_object(&self) -> Result<&Vec<Field>> {
        match self {
            DataType::Object(map) => Ok(map),
            _ => Err("Expected an object".into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum FieldMeta {
    None,
    Json(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub is_nullable: bool,
    pub meta: FieldMeta,
}

pub type Schema = Vec<Field>;

impl Field {
    pub fn new_with_meta<T: Into<String>>(
        name: T,
        data_type: DataType,
        is_nullable: bool,
        meta: FieldMeta,
    ) -> Self {
        Self {
            name: name.into(),
            data_type,
            is_nullable,
            meta,
        }
    }
    pub fn new<T: Into<String>>(name: T, data_type: DataType, is_nullable: bool) -> Self {
        Self::new_with_meta(name, data_type, is_nullable, FieldMeta::None)
    }

    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

#[inline]
pub fn coerce_value<'a>(
    mut value: &mut Value<'a>,
    data_type: &'a DataType,
    recursive: bool,
) -> Result<()> {
    match (&mut value, data_type) {
        (Value::Null, DataType::Null) => Ok(()),
        (Value::String(_), DataType::String) => Ok(()),
        (Value::Int(_), DataType::Int) => Ok(()),
        (Value::Float(_), DataType::Float) => Ok(()),
        (Value::Bool(true), DataType::Int) => {
            *value = Value::Int(1);
            Ok(())
        }
        (Value::Bool(false), DataType::Int) => {
            *value = Value::Int(0);
            Ok(())
        }
        (Value::Int(a), DataType::Float) => {
            *value = Value::Float(*a as f64);
            Ok(())
        }
        (Value::Int(a), DataType::String) => {
            *value = Value::String(a.to_string().into());
            Ok(())
        }
        (Value::Float(a), DataType::String) => {
            *value = Value::String(a.to_string().into());
            Ok(())
        }
        (Value::Bool(_), DataType::Bool) => Ok(()),
        (Value::Object(ref mut object), DataType::Object(fields)) if recursive => {
            fields.iter().try_for_each(|f| {
                match object.get_mut(&Cow::Borrowed(f.name.as_str())) {
                    Some(value) => value.coerce(&f.data_type, recursive),
                    None => {
                        let mut value = Value::Null;
                        value.coerce(&f.data_type, recursive)?;
                        object.insert(Cow::Owned(f.name.clone()), value);
                        Ok(())
                    }
                }
            })?;

            fields.iter().enumerate().for_each(|(idx, f)| {
                let curr_idx = object
                    .get_index_of(&Cow::Borrowed(f.name.as_str()))
                    .unwrap();
                if idx != curr_idx {
                    object.swap_indices(idx, curr_idx);
                }
            });

            Ok(())
        }
        (Value::Object(_), DataType::Object(_)) => Ok(()),
        (Value::Array(ref mut values), DataType::Array(data_type)) => {
            for value in values {
                coerce_value(value, data_type, recursive)?
            }

            Ok(())
        }
        (Value::String(str), DataType::Int) => {
            *value = Value::Int(parse_integer(str.as_bytes())?);
            Ok(())
        }
        (Value::Null, _) => Ok(()),
        (other, DataType::String) => {
            *value = Value::String(serde_json::to_string(other)?.into());
            Ok(())
        }
        (a, b) => todo!("coerce {:?} to {:?}", a, b),
    }
}

#[inline]
fn parse_integer(bytes: &[u8]) -> Result<i64> {
    lexical_core::parse::<i64>(bytes).map_err(Into::into)
}

#[inline]
pub fn parse_bytes(bytes: &[u8]) -> Value {
    if let Ok(v) = parse_integer(bytes) {
        Value::Int(v)
    } else {
        Value::String(Cow::Borrowed(unsafe {
            std::str::from_utf8_unchecked(bytes)
        }))
    }
}

pub fn infer_value(value: &Value) -> DataType {
    match value {
        Value::Null => DataType::Null,
        Value::Int(_) => DataType::Int,
        Value::Float(_) => DataType::Float,
        Value::Bool(_) => DataType::Bool,
        Value::String(_str) => DataType::String,
        Value::Object(object) => DataType::Object(
            object
                .iter()
                .map(|(key, value)| {
                    let data_type = infer_value(value);
                    let is_null = data_type == DataType::Null;

                    Field::new(key.to_string(), data_type, is_null)
                })
                .collect(),
        ),
        Value::Array(array) => {
            let types: IndexSet<DataType> = array.iter().map(infer_value).collect();
            DataType::Array(Box::new(coerce_data_types(
                types.into_iter().collect::<Vec<_>>(),
            )))
        }
        v => todo!("{:?}", v),
    }
}

pub fn coerce_data_types(mut types: Vec<DataType>) -> DataType {
    let mut a = None;

    for b in types.iter_mut() {
        a = Some(match a {
            None => b.clone(),
            Some(ref mut a) => coerce_data_type(a, b),
        })
    }

    a.unwrap_or(DataType::Null)
}

pub fn coerce_data_type(a: &mut DataType, b: &mut DataType) -> DataType {
    match (a, b) {
        (lhs, rhs) if lhs == rhs => lhs.clone(),
        (DataType::Array(lhs), DataType::Array(rhs)) => {
            let inner = coerce_data_type(lhs, rhs);
            DataType::Array(Box::new(inner))
        }
        (DataType::Object(lhs), DataType::Object(rhs)) => {
            let mut fields = Vec::with_capacity(std::cmp::max(lhs.len(), rhs.len()));

            for mut a in lhs.drain(..) {
                let Some(index)= rhs.iter_mut().position(|f| f.name == a.name) else {
                    a.is_nullable = true;
                    fields.push(a);
                    continue;
                };

                let mut b = rhs.swap_remove(index);

                if b.data_type == DataType::Null {
                    a.is_nullable = true;
                } else {
                    a.data_type = coerce_data_type(&mut a.data_type, &mut b.data_type);
                }

                fields.push(a);
            }

            for mut b in rhs.drain(..) {
                b.is_nullable = true;
                fields.push(b);
            }

            DataType::Object(fields)
        }
        (DataType::Float, DataType::Int) => DataType::Float,
        (DataType::Int, DataType::Float) => DataType::Float,
        (DataType::Int, DataType::Bool) => DataType::Int,
        (DataType::Bool, DataType::Int) => DataType::Int,
        (_, _) => DataType::String,
    }
}

pub trait ValueReader: Send + Sync {
    fn next(&mut self) -> Result<Option<Value<'_>>>;

    fn infer_schema(&mut self) -> Result<DataType> {
        let mut dt: Option<DataType> = None;

        while let Some(value) = self.next()? {
            match dt.as_mut() {
                Some(dt) => {
                    *dt = coerce_data_type(dt, &mut infer_value(&value));
                }
                None => dt = Some(infer_value(&value)),
            }
        }

        self.reset();

        dt.ok_or_else(|| Error::InputError("No records found.".into()))
    }

    fn into_rows(mut self) -> Result<RowIterator<Self>>
    where
        Self: Sized,
    {
        match self.infer_schema()? {
            DataType::Object(fields) => Ok(RowIterator::new(self, fields)),
            _ => Err("Expected an object".into()),
        }
    }

    fn reset(&mut self);
}

impl<T: ValueReader + ?Sized> ValueReader for Box<T> {
    fn next(&mut self) -> Result<Option<Value<'_>>> {
        (**self).next()
    }
    fn reset(&mut self) {
        (**self).reset()
    }
}

pub struct RowIterator<R>(R, Vec<Field>);

impl<R> RowIterator<R>
where
    R: ValueReader,
{
    pub fn new(reader: R, fields: Vec<Field>) -> RowIterator<R> {
        Self(reader, fields)
    }

    pub fn fields(&self) -> &Vec<Field> {
        &self.1
    }

    pub fn reset(&mut self) {
        self.0.reset()
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<impl Iterator<Item = Result<Value<'_>>>>> {
        match self.0.next()? {
            None => Ok(None),
            Some(Value::Object(mut object)) => Ok(Some(self.1.iter().map(move |f| {
                match object.remove(&Cow::Borrowed(f.name.as_str())) {
                    Some(mut value) => {
                        value.coerce(&f.data_type, false)?;
                        Ok(value)
                    }
                    None => Ok(Value::Null),
                }
            }))),
            Some(_) => Err(Error::CustomError("Expected object".into())),
        }
    }
}

pub fn new_reader(path: &str, options: ReaderOptions) -> Result<Box<dyn ValueReader>> {
    let input_file = std::fs::File::open(path)?;
    let data = unsafe { memmap2::Mmap::map(&input_file)? };

    Ok(match options {
        ReaderOptions::Csv(opts) => Box::new(crate::csv::reader::Reader::new(
            std::io::Cursor::new(data),
            opts,
        )?) as Box<dyn ValueReader>,
        ReaderOptions::Json(opts) => {
            Box::new(crate::json::reader::RecordReader::new(data, opts)?) as Box<dyn ValueReader>
        }
        ReaderOptions::Excel(opts) => {
            Box::new(crate::excel::reader::Reader::new(data, opts)?) as Box<dyn ValueReader>
        }
        ReaderOptions::Avro(opts) => Box::new(crate::avro::reader::Reader::new(
            std::io::Cursor::new(data),
            opts,
        )?) as Box<dyn ValueReader>,
    })
}
