use crate::DataType;
use crate::Error;

use crate::arrow;
use crate::Field;
use crate::Reader;
use crate::Result;
use crate::Value;

use arrow2::array::Array;

use arrow2::array::StructArray;
use arrow2::datatypes::Field as ArrowField;

use std::ffi::c_void;
use std::ffi::CStr;
use std::ffi::CString;
use std::iter::once_with;
use std::sync::atomic::AtomicPtr;

mod ffi {
    #![allow(
        non_snake_case,
        non_camel_case_types,
        non_upper_case_globals,
        unused_mut,
        unused
    )]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

pub struct Connection {
    conn: AtomicPtr<std::ffi::c_void>,
    db: AtomicPtr<std::ffi::c_void>,
}

pub struct ArrowStreamFactory {
    pub chunks: Vec<Box<dyn Array>>,
    pub schema: arrow2::datatypes::DataType,
}

pub struct PrivateData {
    pub factory: *mut ArrowStreamFactory,
    pub index: usize,
}

extern "C" fn drop_arrow_stream_factory(c_reader_factory: *mut std::ffi::c_void) {
    unsafe {
        let factory = Box::from_raw(c_reader_factory as *mut ArrowStreamFactory);
        drop(factory);
    }
}

unsafe extern "C" fn get_schema(
    stream: *mut ffi::ArrowArrayStream,
    schema: *mut ffi::ArrowSchema,
) -> i32 {
    if stream.is_null() {
        return 2001;
    }
    let private = &*((*stream).private_data as *mut PrivateData);

    let field = ArrowField::new("", (*private.factory).schema.clone(), true);
    *schema = std::mem::transmute(arrow2::ffi::export_field_to_c(&field));
    0
}

unsafe extern "C" fn release_stream(stream: *mut ffi::ArrowArrayStream) {
    let factory = Box::from_raw((*stream).private_data as *mut PrivateData);
    drop(factory);
}

unsafe extern "C" fn last_error(_stream: *mut ffi::ArrowArrayStream) -> *const i8 {
    std::ptr::null()
}

unsafe extern "C" fn get_next(
    stream: *mut ffi::ArrowArrayStream,
    array: *mut ffi::ArrowArray,
) -> i32 {
    if stream.is_null() {
        return 2001;
    }
    let stream = &*stream;
    let private = &mut *(stream.private_data as *mut PrivateData);
    let factory = &mut *private.factory;

    match factory.chunks.get(private.index) {
        Some(item) => {
            *array = unsafe { std::mem::transmute(arrow2::ffi::export_array_to_c(item.clone())) };
            private.index += 1;
            0
        }
        None => {
            *array = std::mem::transmute(arrow2::ffi::ArrowArray::empty());
            0
        }
    }
}

unsafe extern "C" fn create_arrow_stream(
    c_reader_factory: *mut std::ffi::c_void,
    c_stream: *mut ffi::ArrowArrayStream,
) {
    let factory = c_reader_factory as *mut ArrowStreamFactory;
    let data = Box::new(PrivateData { factory, index: 0 });

    *c_stream = ffi::ArrowArrayStream {
        get_schema: Some(get_schema),
        get_next: Some(get_next),
        get_last_error: Some(last_error),
        release: Some(release_stream),
        private_data: Box::into_raw(data) as *mut c_void,
    };
}

impl Drop for Connection {
    fn drop(&mut self) {
        unsafe {
            ffi::destroy(self.db.get_mut(), self.conn.get_mut());
        }
    }
}

impl Connection {
    pub fn new() -> Result<Self> {
        unsafe {
            let mut conn: ffi::duckdb_connection = std::ptr::null_mut();
            let mut db: ffi::duckdb_database = std::ptr::null_mut();

            ffi::new_in_memory(&mut db, &mut conn);

            if conn.is_null() {
                Err(Error::DuckDbError("Failed to open connection".to_string()))
            } else {
                Ok(Connection {
                    db: AtomicPtr::new(db),
                    conn: AtomicPtr::new(conn),
                })
            }
        }
    }

    pub fn register<R: Reader>(&mut self, table: &str, reader: R) -> Result<()> {
        let c_table = CString::new(table).unwrap();
        let (schema, iter) = crate::arrow::writer::to_arrow(reader)?;

        let chunks = iter
            .map(|a| {
                let item = a.map_err(Error::from)?;
                Ok(item)
            })
            .collect::<Result<Vec<_>>>()?;

        let factory = Box::new(ArrowStreamFactory { chunks, schema });

        unsafe {
            ffi::register_arrow_stream(
                *self.conn.get_mut(),
                c_table.as_ptr(),
                Some(create_arrow_stream),
                Some(drop_arrow_stream_factory),
                Box::into_raw(factory) as *mut std::ffi::c_void,
            );
        }

        Ok(())
    }

    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        let c_sql = CString::new(sql).unwrap();
        let mut result = QueryResult(std::ptr::null_mut());

        unsafe {
            if ffi::duckdb_query_arrow(*self.conn.get_mut(), c_sql.as_ptr(), &mut result.0)
                == ffi::duckdb_state_DuckDBError
            {
                let c_err = ffi::duckdb_query_arrow_error(result.0);
                let message = CStr::from_ptr(c_err).to_string_lossy().to_string();
                return Err(Error::DuckDbError(message));
            }
        }

        Ok(result)
    }
}

pub struct QueryResult(ffi::duckdb_arrow);

impl QueryResult {
    pub fn records<'a, 'b>(
        &'a mut self,
    ) -> Option<(Vec<Field>, impl Iterator<Item = Vec<Value<'b>>> + '_)> {
        let first = self.next()?;

        let dt = match arrow::writer::datatype_from_arrow(first.data_type()) {
            DataType::Object(fields) => fields,
            _ => Vec::new(),
        };

        Some((
            dt,
            once_with(move || first).chain(self).flat_map(|array| {
                let length = Array::len(&array);

                (0..length).map(move |i| {
                    array
                        .values()
                        .iter()
                        .map(move |column| arrow::writer::get_arrow_value(column, i))
                        .collect::<Vec<_>>()
                })
            }),
        ))
    }
}

impl Drop for QueryResult {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe {
                let ptr = &mut self.0;
                ffi::duckdb_destroy_arrow(ptr as *mut *mut c_void);
            }
        }
    }
}

impl Iterator for QueryResult {
    type Item = StructArray;

    fn next(&mut self) -> Option<Self::Item> {
        let mut array = arrow2::ffi::ArrowArray::empty();
        let mut schema = arrow2::ffi::ArrowSchema::empty();

        unsafe {
            {
                let array = &mut &mut array;

                if ffi::duckdb_query_arrow_array(
                    self.0,
                    array as *mut _ as *mut *mut std::ffi::c_void,
                ) == ffi::duckdb_state_DuckDBError
                {
                    return None;
                }
            }

            {
                let array = &mut array as *mut _ as *mut ffi::ArrowArray;
                if (*array).length == 0 {
                    return None;
                }
            }

            let schema = &mut &mut schema;
            if ffi::duckdb_query_arrow_schema(
                self.0,
                schema as *mut _ as *mut *mut std::ffi::c_void,
            ) == ffi::duckdb_state_DuckDBError
            {
                return None;
            }

            let field = arrow2::ffi::import_field_from_c(schema).unwrap();

            let array = Box::from_raw(Box::into_raw(
                arrow2::ffi::import_array_from_c(array, field.data_type).unwrap(),
            ) as *mut StructArray);

            Some(*array)
        }
    }
}
