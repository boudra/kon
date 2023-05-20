use std::borrow::Cow;

use arrow2::array::Array;

use arrow2::array::BooleanArray;
use arrow2::array::Float64Array;
use arrow2::array::Int32Array;
use arrow2::array::Int64Array;
use arrow2::array::MutableArray;
use arrow2::array::MutablePrimitiveArray;
use arrow2::array::MutableStructArray;
use arrow2::array::MutableUtf8Array;

use arrow2::array::StructArray;
use arrow2::array::Utf8Array;
use arrow2::datatypes::DataType as ArrowDataType;
use arrow2::datatypes::Field as ArrowField;

use crate::DataType;
use crate::Field;
use crate::Reader;
use crate::Result;
use crate::Value;

fn datatype_to_arrow(dt: &DataType) -> ArrowDataType {
    match dt {
        DataType::Bool => ArrowDataType::Boolean,
        DataType::Int => ArrowDataType::Int64,
        DataType::Float => ArrowDataType::Float64,
        DataType::String => ArrowDataType::Utf8,
        DataType::Object(fields) => ArrowDataType::Struct(
            fields
                .iter()
                .map(|f| {
                    ArrowField::new(
                        f.name.clone(),
                        datatype_to_arrow(&f.data_type),
                        f.is_nullable,
                    )
                })
                .collect(),
        ),
        _ => todo!(),
    }
}

pub fn datatype_from_arrow(dt: &ArrowDataType) -> DataType {
    match dt {
        ArrowDataType::Boolean => DataType::Bool,
        ArrowDataType::Int64 => DataType::Int,
        ArrowDataType::Int32 => DataType::Int,
        ArrowDataType::Float64 => DataType::Float,
        ArrowDataType::Utf8 => DataType::String,
        ArrowDataType::Struct(fields) => DataType::Object(
            fields
                .iter()
                .map(|f| {
                    Field::new(
                        f.name.clone(),
                        datatype_from_arrow(&f.data_type),
                        f.is_nullable,
                    )
                })
                .collect(),
        ),
        _ => todo!(),
    }
}

fn new_array(dt: &ArrowDataType) -> Box<dyn MutableArray> {
    match dt {
        ArrowDataType::Utf8 => Box::new(MutableUtf8Array::<i32>::new()),
        ArrowDataType::Int64 => Box::new(MutablePrimitiveArray::<i64>::new()),
        ArrowDataType::Float64 => Box::new(MutablePrimitiveArray::<f64>::new()),
        ArrowDataType::Struct(fields) => {
            let values = fields
                .iter()
                .map(|dt| new_array(&dt.data_type))
                .collect::<Vec<_>>();

            Box::new(MutableStructArray::new(dt.clone(), values))
        }
        _ => todo!(),
    }
}

fn push_array_value(array: &mut Box<dyn MutableArray>, value: &Value, dt: &ArrowDataType) {
    match dt {
        ArrowDataType::Utf8 => {
            array
                .as_mut_any()
                .downcast_mut::<MutableUtf8Array<i32>>()
                .unwrap()
                .push(value.as_string());
        }
        ArrowDataType::Int64 => {
            array
                .as_mut_any()
                .downcast_mut::<MutablePrimitiveArray<i64>>()
                .unwrap()
                .push(value.as_int());
        }
        ArrowDataType::Float64 => {
            array
                .as_mut_any()
                .downcast_mut::<MutablePrimitiveArray<f64>>()
                .unwrap()
                .push(value.as_float());
        }
        ArrowDataType::Struct(fields) => {
            let array = array
                .as_mut_any()
                .downcast_mut::<MutableStructArray>()
                .unwrap();

            array
                .mut_values()
                .iter_mut()
                .zip(value.as_object().unwrap())
                .zip(fields)
                .for_each(|((array, (_, v)), f)| push_array_value(array, v, &f.data_type))
        }
        _ => todo!(),
    }
}

pub fn get_arrow_value<'a, 'b>(array: &'a Box<dyn Array>, i: usize) -> Value<'b> {
    match array.data_type() {
        ArrowDataType::Utf8 => Value::String(Cow::Owned(
            array
                .as_any()
                .downcast_ref::<Utf8Array<i32>>()
                .unwrap()
                .value(i)
                .to_owned(),
        )),
        ArrowDataType::Boolean => Value::Bool(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(i),
        ),
        ArrowDataType::Int64 => Value::Int(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(i),
        ),
        ArrowDataType::Int32 => Value::Int(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(i) as i64,
        ),
        ArrowDataType::Float64 => Value::Float(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(i),
        ),
        ArrowDataType::Struct(fields) => Value::Object(
            array
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .values()
                .iter()
                .zip(fields)
                .map(|(array, f)| (Cow::Owned(f.name.clone()), get_arrow_value(array, i)))
                .collect(),
        ),
        ref v => todo!("{:?}", v),
    }
}

pub fn arrow_struct_to_object<'a, 'b>(
    array: &'a StructArray,
) -> impl Iterator<Item = impl Iterator<Item = (&'a String, Value<'b>)>> {
    let length = Array::len(array);

    (0..length).map(move |i| {
        array
            .fields()
            .iter()
            .zip(array.values())
            .map(move |(field, column)| (&field.name, get_arrow_value(column, i)))
    })
}

pub fn arrow_array_to_values<'a, 'b>(array: &'a Box<dyn Array>) -> Vec<Value<'b>> {
    match array.data_type() {
        ArrowDataType::Struct(_) => {
            let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();

            arrow_struct_to_object(struct_array)
                .map(|values| {
                    Value::Object(values.map(|(k, v)| (Cow::Owned(k.clone()), v)).collect())
                })
                .collect()
        }
        ref v => todo!("{:?}", v),
    }
}

pub fn to_arrow<R: Reader>(
    mut reader: R,
) -> Result<(
    ArrowDataType,
    impl Iterator<Item = arrow2::error::Result<Box<dyn Array>>>,
)> {
    let dt = reader.infer_schema()?;
    let arrow_dt = datatype_to_arrow(&dt);
    let chunk_size = 5000;
    let mut done = false;

    Ok((
        arrow_dt.clone(),
        std::iter::from_fn(move || {
            let mut count = 0;

            if done {
                return None;
            }

            let mut array = new_array(&arrow_dt);
            array.reserve(chunk_size);

            while count < chunk_size {
                match reader.next() {
                    Err(err) => {
                        return Some(Err(arrow2::error::Error::External(
                            String::new(),
                            Box::new(err),
                        )))
                    }
                    Ok(Some(ref mut value)) => {
                        value.coerce(&dt, true).unwrap();
                        push_array_value(&mut array, value, &arrow_dt);
                        count += 1;
                    }
                    Ok(None) => {
                        done = true;
                        break;
                    }
                };
            }

            if count == 0 {
                return None;
            }

            Some(Ok(array.as_box()))
        }),
    ))
}
