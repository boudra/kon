use std::io::{Read, Seek, SeekFrom};

mod line_length_limiter;

pub use line_length_limiter::LineLengthLimiter;

pub struct PartialChunkedFileReader<R> {
    inner: R,
    size: u64,
}

impl<R: Read + Seek> PartialChunkedFileReader<R> {
    pub fn new(inner: R, size: u64) -> Self {
        PartialChunkedFileReader { inner, size }
    }
}

impl<R: Read + Seek> Read for PartialChunkedFileReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::result::Result<usize, std::io::Error> {
        if self.size == self.inner.stream_position()? {
            Ok(0)
        } else {
            loop {
                let bytes_read = self.inner.read(buf)?;
                if bytes_read > 0 {
                    // TODO: Rewind if the read chunk is empty, since the file contents could potentially contain a null byte this could introduce a bug
                    if buf[0] == 0u8 && buf[1] == 0u8 {
                        self.inner.seek(SeekFrom::Current(-(bytes_read as i64)))?;
                    } else {
                        break Ok(bytes_read);
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }
    }
}

impl<R: Seek> Seek for PartialChunkedFileReader<R> {
    fn seek(&mut self, from: SeekFrom) -> std::result::Result<u64, std::io::Error> {
        self.inner.seek(from)
    }
}
