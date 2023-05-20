use crate::Error;
use std::io::{Read, Seek, SeekFrom};

pub struct LineLengthLimiter<R> {
    inner: R,
    current_length: usize,
    limit: usize,
}

impl<R: Read + Seek> LineLengthLimiter<R> {
    pub fn new(inner: R, limit: usize) -> Self {
        LineLengthLimiter {
            inner,
            limit,
            current_length: 0,
        }
    }
}

impl<R: Read> Read for LineLengthLimiter<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::result::Result<usize, std::io::Error> {
        let bytes_read = self.inner.read(buf)?;
        let found_index = buf[0..bytes_read].iter().position(|&b| b == b'\n');
        let index = found_index.unwrap_or(bytes_read);

        if self.current_length + index >= self.limit {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                Error::CustomError(format!(
                    "Line length exceeds {} MB",
                    self.limit / 1000 / 1000
                )),
            ));
        }

        if found_index.is_some() {
            self.current_length = bytes_read - index
        } else {
            self.current_length += index
        }

        Ok(bytes_read)
    }
}

impl<R: Seek> Seek for LineLengthLimiter<R> {
    fn seek(&mut self, from: SeekFrom) -> std::result::Result<u64, std::io::Error> {
        self.inner.seek(from)
    }
}
