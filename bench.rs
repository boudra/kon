use std::borrow::Cow;

use arrow2::array::{Array, BooleanArray, Float64Array, Int64Array, Utf8Array};
use konbert::{reader::Object, Result, Value};

fn parse_json() -> Result<()> {
    use konbert::json::reader::RecordReader;
    use konbert::Reader;
    use std::time::Instant;

    let input_file = std::fs::File::open(
        "/Users/moboudra/Downloads/yelp_dataset/yelp_academic_dataset_review.json",
    )?;
    let data = unsafe { memmap2::Mmap::map(&input_file)? };

    let mut reader = RecordReader::new(data, Default::default())?;
    let mut count = 0;

    let start = Instant::now();
    let mut last = Instant::now();
    // while let true = reader.read(&mut value)? {
    while let Some(value) = reader.next()? {
        // println!("{:?}", value);
        count += 1;

        if count > 5000 {
            println!("batch in {:?}ms", last.elapsed().as_millis());
            count = 0;
            last = Instant::now();
        }
    }

    println!("finished {:?}ms", start.elapsed().as_millis());

    Ok(())
}

fn to_arrow() -> Result<()> {
    use std::time::Instant;

    let reader = konbert::reader::new_reader(
        "/Users/moboudra/Downloads/yelp_dataset/yelp_academic_dataset_review.json",
        konbert::ReaderOptions::Json(Default::default()),
    )?;

    let start = Instant::now();
    let mut last = start;

    let (dt, iter) = konbert::arrow::writer::to_arrow(reader)?;

    println!("schema: {:?}", dt);
    println!("to arrow in {:?}ms", last.elapsed().as_millis());

    last = Instant::now();
    for batch in iter {
        println!(
            "batch in {:?}ms {:?}",
            last.elapsed().as_millis(),
            batch?.len()
        );
        last = Instant::now();
    }

    println!("finished in {:?}ms", start.elapsed().as_millis());

    Ok(())
}

#[cfg(feature = "duckdb")]
fn duckdb() -> Result<()> {
    use arrow2::datatypes::DataType as ArrowDataType;
    use std::time::Instant;
    let mut conn = konbert::duckdb::Connection::new()?;

    let mut last = Instant::now();

    conn.register(
        "mytable",
        konbert::reader::new_reader(
            "/Users/moboudra/Downloads/yelp_dataset/yelp_academic_dataset_review.json",
            // "/Users/moboudra/dev/konbert/priv/examples/example.json",
            konbert::ReaderOptions::Json(Default::default()),
        )
        .unwrap(),
    )?;

    println!("registered in {:?}ms", last.elapsed().as_millis());

    last = Instant::now();

    let mut result = conn.query("select count(*) from mytable;")?;

    println!("queried in {:?}ms", last.elapsed().as_millis());

    while let Some(array) = result.next() {
        let length = Array::len(&array);

        // println!("len: {}", length);

        for i in 0..length {
            let mut object = Object::with_capacity(array.fields().len());

            for (field, column) in array.fields().iter().zip(array.values()) {
                let value = match field.data_type {
                    ArrowDataType::Utf8 => Value::String(std::borrow::Cow::Borrowed(
                        column
                            .as_any()
                            .downcast_ref::<Utf8Array<i32>>()
                            .unwrap()
                            .value(i),
                    )),
                    ArrowDataType::Boolean => Value::Bool(
                        column
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .unwrap()
                            .value(i),
                    ),
                    ArrowDataType::Int64 => Value::Int(
                        column
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(i),
                    ),
                    ArrowDataType::Float64 => Value::Float(
                        column
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap()
                            .value(i),
                    ),
                    ref v => todo!("{:?}", v),
                };

                object.insert(Cow::Borrowed(&field.name), value);
            }

            // println!("{:?}", object);
        }
    }

    Ok(())
}

fn main() {
    to_arrow().unwrap();
}
