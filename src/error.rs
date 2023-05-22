use serde::{Serialize, Serializer};
use std::fmt::Display;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("{0}")]
    CustomError(String),
    #[error("{0}")]
    CsvError(::csv::Error),
    #[error("{0}")]
    SerdeError(serde_json::Error),
    #[error("{0}")]
    IoError(std::io::Error),
    #[error("{0}")]
    ArrowError(arrow2::error::Error),
    #[error("Input Error: {0}")]
    InputError(String),
    #[error("Output Error: {0}")]
    OutputError(String),
}

impl Error {
    pub fn input<E: Display>(e: E) -> Error {
        Error::InputError(e.to_string())
    }

    pub fn output<E: Display>(e: E) -> Error {
        Error::OutputError(e.to_string())
    }
}

impl Serialize for Error {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl From<&str> for Error {
    fn from(e: &str) -> Error {
        Error::CustomError(e.to_string())
    }
}

impl From<lexical_core::Error> for Error {
    fn from(e: lexical_core::Error) -> Error {
        Error::InputError(format!("{}", e))
    }
}

impl From<apache_avro::Error> for Error {
    fn from(e: apache_avro::Error) -> Error {
        Error::InputError(format!("{}", e))
    }
}

impl From<simdutf8::basic::Utf8Error> for Error {
    fn from(e: simdutf8::basic::Utf8Error) -> Error {
        Error::InputError(format!("{}", e))
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Error {
        Error::SerdeError(e)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        Error::IoError(e)
    }
}

impl From<arrow2::error::Error> for Error {
    fn from(e: arrow2::error::Error) -> Error {
        Error::ArrowError(e)
    }
}

impl From<::csv::Error> for Error {
    fn from(e: ::csv::Error) -> Error {
        Error::CsvError(e)
    }
}
