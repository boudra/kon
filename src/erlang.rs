use crate::{avro, csv, excel, json, sql};

#[derive(serde::Deserialize)]
pub enum ReaderOptions {
    #[serde(rename = "csv")]
    Csv(csv::reader::Options),
    #[serde(rename = "json")]
    Json(json::reader::Options),
    #[serde(rename = "excel")]
    Excel(excel::reader::Options),
    #[serde(rename = "avro")]
    Avro(avro::reader::Options),
}

#[derive(serde::Deserialize)]
pub enum WriterOptions {
    #[serde(rename = "csv")]
    Csv(csv::writer::Options),
    #[serde(rename = "json")]
    Json(json::writer::Options),
    #[serde(rename = "sql")]
    Sql(sql::writer::Options),
    #[serde(rename = "html")]
    Html {},
    #[serde(rename = "xml")]
    Xml {},
    #[serde(rename = "avro")]
    Avro(avro::writer::Options),
}
