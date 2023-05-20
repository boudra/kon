use crate::reader::ValueReader;
use crate::{DataType, Error, Result, Value};

use apache_avro::schema::{Name, RecordField, RecordFieldOrder, Schema, UnionSchema};
use apache_avro::types::Value as AvroValue;
use apache_avro::{to_value, Writer};
use std::io::Write;

#[derive(Debug, serde::Deserialize)]
pub struct Options {
    only_schema: bool,
}

fn to_avro(v: Value) -> Result<AvroValue> {
    match v {
        Value::Object(object) => Ok(AvroValue::Record(
            object
                .into_iter()
                .map(|(key, value)| Ok((key.to_string(), to_avro(value)?)))
                .collect::<Result<Vec<_>>>()?,
        )),
        other => to_value(other).map_err(Error::from),
    }
}

fn schema_to_avro(dt: &DataType, name: String) -> Result<Schema> {
    match dt {
        DataType::Null => Ok(Schema::Null),
        DataType::Bool => Ok(Schema::Boolean),
        DataType::String => Ok(Schema::String),
        DataType::Binary => Ok(Schema::Bytes),
        DataType::Int => Ok(Schema::Long),
        DataType::Float => Ok(Schema::Double),
        DataType::Array(inner) => Ok(Schema::Array(Box::new(schema_to_avro(inner, name)?))),
        DataType::Object(fields) => {
            let record_fields = fields
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    let field_schema = if f.is_nullable && f.data_type != DataType::Null {
                        Schema::Union(UnionSchema::new(vec![
                            Schema::Null,
                            schema_to_avro(&f.data_type, format!("{}.{}", name, f.name))?,
                        ])?)
                    } else {
                        schema_to_avro(&f.data_type, format!("{}.{}", name, f.name))?
                    };

                    Ok(RecordField {
                        name: f.name.clone(),
                        doc: None,
                        default: None,
                        schema: field_schema,
                        order: RecordFieldOrder::Ascending,
                        position: i,
                        custom_attributes: Default::default(),
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(Schema::Record {
                name: Name::new(name.as_str())?,
                aliases: None,
                doc: None,
                lookup: record_fields
                    .iter()
                    .map(|f| (f.name.clone(), f.position))
                    .collect(),
                fields: record_fields,
                attributes: Default::default(),
            })
        }
    }
}

pub fn write<W: Write, R: ValueReader>(inner: W, mut reader: R, opts: Options) -> Result<()> {
    let dt = reader.infer_schema()?;
    let avro_schema = schema_to_avro(&dt, "Record".into()).map_err(Error::output)?;

    if opts.only_schema {
        serde_json::to_writer_pretty(inner, &avro_schema)?;
    } else {
        let mut writer = Writer::new(&avro_schema, inner);

        while let Some(mut value) = reader.next()? {
            value.coerce(&dt, true)?;
            let value = to_avro(value)?
                .resolve(&avro_schema)
                .map_err(Error::output)?;
            writer.append_value_ref(&value).map_err(Error::output)?;
        }

        writer.flush()?;
    }

    Ok(())
}
