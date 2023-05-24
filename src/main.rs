use kon::{avro, csv, html, json, sql, xml, Error, ReaderOptions, WriterOptions};

use serde_json::json;
use std::fs::File;

use std::io::BufWriter;

fn convert() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();

    match args[1].as_str() {
        "konbert" => {
            if args.len() != 6 {
                return Err(Error::CustomError(
                    "invalid number of arguments".to_string(),
                ));
            }

            let input_options: ReaderOptions = serde_json::from_str(&args[3])?;
            let reader = kon::reader::new_reader(&args[2], input_options)?;

            let output_file = File::create(&args[4])?;
            let writer = BufWriter::new(output_file);
            let output_options: WriterOptions = serde_json::from_str(&args[5])?;

            match output_options {
                WriterOptions::Csv(opts) => csv::writer::write(writer, reader, opts),
                WriterOptions::Json(opts) => json::writer::write(writer, reader, opts),
                WriterOptions::Sql(opts) => sql::writer::write(writer, reader, opts),
                WriterOptions::Html {} => html::writer::write(writer, reader),
                WriterOptions::Xml {} => xml::writer::write(writer, reader),
                WriterOptions::Avro(opts) => avro::writer::write(writer, reader, opts),
            }?;
        }

        _ => {
            return Err(Error::CustomError(
                "invalid number of arguments".to_string(),
            ));
        }
    }

    Ok(())
}

fn main() {
    match convert() {
        Err(e) => {
            println!(
                "{}",
                match e {
                    Error::CustomError(_) => json!(["parse_error", format!("{}", e)]),
                    Error::CsvError(_) => json!(["parse_error", format!("{}", e)]),
                    Error::InputError(_) => json!(["parse_error", format!("{}", e)]),
                    Error::OutputError(_) => json!(["parse_error", format!("{}", e)]),
                    _ => json!(format!("error: {}", e)),
                }
            );
            std::process::exit(1);
        }
        Ok(_) => {
            std::process::exit(0);
        }
    }
}
