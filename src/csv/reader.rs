use crate::{
    io::LineLengthLimiter,
    reader::{Object, Value, ValueReader},
    Result, Schema,
};

use csv::ByteRecord;

use encoding_rs_io::DecodeReaderBytes;
use serde::de::Deserializer;
use serde::Deserialize;
use std::borrow::Cow;
use std::io::{Read, Seek};

#[derive(Debug)]
pub struct Encoding(pub &'static encoding_rs::Encoding);

impl<'de> Deserialize<'de> for Encoding {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let str = String::deserialize(deserializer)?;
        encoding_rs::Encoding::for_label_no_replacement(str.as_bytes())
            .ok_or_else(|| serde::de::Error::custom(format!("{} is not a valid encoding", str)))
            .map(Encoding)
    }
}

type CsvReader<R> = csv::Reader<DecodeReaderBytes<LineLengthLimiter<R>, Vec<u8>>>;

pub struct Reader<R> {
    start_pos: csv::Position,
    headers: Vec<String>,
    reader: CsvReader<R>,
    buffer: ByteRecord,
}

impl<R: Read + Seek + Send + Sync + 'static> Reader<R> {
    pub fn new(mut inner_reader: R, options: Options) -> Result<Self> {
        let mut bom_buffer = [0u8; 3];

        let read_bom = inner_reader.read(&mut bom_buffer)?;

        let (maybe_bom, skip_bom) = match encoding_rs::Encoding::for_bom(&bom_buffer[0..read_bom]) {
            Some((e, s)) => (Some(Encoding(e)), s as u64),
            None => (None, 0),
        };

        inner_reader.seek(std::io::SeekFrom::Start(skip_bom))?;

        let encoding = match options.encoding {
            Some(encoding) => encoding,
            None => {
                let encoding = maybe_bom
                    .map(Ok)
                    .unwrap_or_else(|| infer_encoding(&mut inner_reader))?;
                inner_reader.seek(std::io::SeekFrom::Start(skip_bom))?;
                encoding
            }
        };

        let line_limiter = LineLengthLimiter::new(inner_reader, 5 * 1000 * 1000);

        let decoder = encoding_rs_io::DecodeReaderBytesBuilder::new()
            .encoding(Some(encoding.0))
            .build(line_limiter);

        let mut reader = csv::ReaderBuilder::new()
            .has_headers(options.has_headers)
            .flexible(true)
            .delimiter(options.delimiter)
            .from_reader(decoder);

        let mut start_pos = reader.position().clone();

        let headers: Vec<String>;

        if options.has_headers {
            headers = reader
                .headers()?
                .iter()
                .map(String::from)
                .collect::<Vec<_>>();
            start_pos = reader.position().clone();
        } else {
            headers = (0..reader.headers()?.len())
                .map(|i| format!("column_{}", i + 1))
                .collect::<Vec<_>>();

            reader.seek(start_pos.clone())?;
        };

        Ok(Reader {
            start_pos,
            headers,
            buffer: ByteRecord::default(),
            reader,
        })
    }
}

impl<R: Read + Seek + 'static + Send + Sync> ValueReader for Reader<R> {
    fn reset(&mut self) {
        self.reader.seek(self.start_pos.clone()).unwrap()
    }

    fn next(&mut self) -> Result<Option<Value<'_>>> {
        let mut row = Object::default();

        if !self.reader.read_byte_record(&mut self.buffer)? {
            Ok(None)
        } else {
            row.reserve(self.headers.len());

            for (f, value) in self.headers.iter().zip(self.buffer.iter()) {
                row.insert(Cow::Borrowed(f), crate::reader::parse_bytes(value));
            }

            Ok(Some(Value::Object(row)))
        }
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct Options {
    pub delimiter: u8,
    pub has_headers: bool,
    pub encoding: Option<Encoding>,
    pub schema: Option<Schema>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            delimiter: b',',
            schema: None,
            has_headers: true,
            encoding: None,
        }
    }
}

fn infer_encoding<R: Read>(inner_reader: &mut R) -> Result<Encoding> {
    use chardetng::EncodingDetector;
    let encoding_limit_size = 500 * 1000;
    let mut buffer = [0; 8192];
    let mut encoding_size = 0;
    let _schema_limit_rows = 5000;

    let mut detector = EncodingDetector::new();

    loop {
        let nsize = inner_reader.read(&mut buffer)?;
        encoding_size += nsize;

        if nsize > 0 {
            detector.feed(&buffer, false);

            if encoding_size >= encoding_limit_size {
                break;
            }
        } else {
            detector.feed(&buffer, true);
            break;
        }
    }

    Ok(Encoding(detector.guess(None, true)))
}

#[test]
fn csv_parse() {
    // #[cfg(feature = "dhat-heap")]
    // let _profiler = dhat::Profiler::new_heap();

    let inner = std::io::Cursor::new(
        r#"
id,string,trailing
1,"dog",1
2,"ca
t",2
5,"hello ""world""",5
6,hello \"world\",6
surprise!,"{""this is"":""hello \""world\""""}",9"#,
    );

    let mut reader = Reader::new(inner, Default::default()).unwrap();

    println!("{:?}", reader.infer_schema());

    while let Ok(Some(row)) = reader.next() {
        println!("{:?}", row);
    }
}

#[test]
fn latin1() {
    let _inner = std::io::Cursor::<Vec<u8>>::new(vec![
        73, 100, 44, 110, 97, 109, 101, 10, 49, 44, 74, 234, 114, 111, 241, 111, 10,
    ]);

    // let _reader = Reader::new(inner, Default::default()).unwrap().boxed();

    // let mut count = 0;

    // let row = reader.next().unwrap().unwrap();

    // assert_eq!(
    //     row,
    //     Row::from(vec![Value::Int(1), Value::String("Jêroño".to_string())])
    // );
}
