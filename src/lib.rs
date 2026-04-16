use std::{
    error::Error,
    fmt, fs,
    io::{self, Seek, Write},
    path::Path,
};

#[derive(Debug, PartialEq, Eq)]
pub struct OpenOptions {
    chunk_size: u64,
}

impl Default for OpenOptions {
    fn default() -> Self {
        OpenOptions { chunk_size: 1024 }
    }
}

#[derive(Debug)]
pub struct InvalidChunkSizeError {}

impl fmt::Display for InvalidChunkSizeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid chunk size")
    }
}

impl Error for InvalidChunkSizeError {}

impl OpenOptions {
    pub fn new() -> Self {
        OpenOptions { chunk_size: 0 }
    }

    pub fn chunk_size(&mut self, chunk_size: u64) -> &mut Self {
        self.chunk_size = chunk_size;
        self
    }

    pub fn open<P: AsRef<Path>>(&self, path: P) -> Result<Queue, Box<dyn Error>> {
        if self.chunk_size == 0 {
            return Result::Err(Box::new(InvalidChunkSizeError {}));
        }
        let mut f = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;

        match read_queue_header(&mut f) {
            Result::Err(err) => {
                if err.kind() != io::ErrorKind::UnexpectedEof {
                    Result::Err(Box::new(err))
                } else {
                    Result::Ok(Queue {
                        f,
                        chunk_size: self.chunk_size,
                        num_chunks: 0,
                        read_index: 0,
                    })
                }
            }
            Result::Ok(h) => {
                let mut q = Queue {
                    f,
                    chunk_size: h.chunk_size,
                    num_chunks: h.num_chunks,
                    read_index: h.read_index,
                };
                if h.chunk_size != self.chunk_size {
                    q.resize(self.chunk_size)?;
                }
                Result::Ok(q)
            }
        }
    }
}

#[derive(Debug)]
pub struct Queue {
    f: fs::File,
    chunk_size: u64,
    num_chunks: u64,
    read_index: u64,
}

impl Queue {
    pub fn append(&mut self, data: &[u8]) -> io::Result<()> {
        self.move_to_end()?;
        let chunks_written = write_data(&mut self.f, data, self.chunk_size)?;
        self.num_chunks += chunks_written;
        self.sync()
    }

    pub fn begin<'a>(&'a mut self) -> Reader<'a> {
        let c = self.read_index;
        Reader {
            q: self,
            read_index: c,
        }
    }

    fn move_to_offset(&mut self, offset: u64) -> io::Result<()> {
        let _ = self.f.seek(io::SeekFrom::Start(offset))?;
        io::Result::Ok(())
    }

    fn move_to_start(&mut self) -> io::Result<()> {
        self.move_to_offset(0)
    }

    fn move_to_chunk(&mut self, i: u64) -> io::Result<()> {
        self.move_to_offset(chunk_position(i, self.chunk_size))
    }

    fn move_to_end(&mut self) -> io::Result<()> {
        self.move_to_chunk(self.num_chunks)
    }

    fn get_header(&self) -> QueueHeader {
        QueueHeader {
            magic_number: MAGIC_NUMBER,
            chunk_size: self.chunk_size,
            num_chunks: self.num_chunks,
            read_index: self.read_index,
        }
    }

    fn sync(&mut self) -> io::Result<()> {
        if self.num_chunks == self.read_index {
            self.num_chunks = 0;
            self.read_index = 0;
        }
        self.move_to_start()?;
        let h = self.get_header();
        write_queue_header(&mut self.f, &h)?;
        self.f.flush()
    }

    fn resize(&mut self, new_size: u64) -> io::Result<()> {
        let buffer_offset = chunk_position(self.num_chunks, self.chunk_size);
        let mut current_chunk = self.read_index;
        let mut new_chunks: u64 = 0;
        while current_chunk < self.num_chunks {
            self.move_to_chunk(current_chunk)?;
            let (chunks_read, data) = read_data(&mut self.f, self.chunk_size)?;
            current_chunk += chunks_read;
            self.move_to_offset(buffer_offset + chunk_offset(new_chunks, new_size))?;
            let chunks_written = write_data(&mut self.f, &data, new_size)?;
            new_chunks += chunks_written;
        }
        for i in 0..new_chunks {
            self.move_to_offset(buffer_offset + chunk_offset(i, new_size))?;
            let (_, data) = read_data(&mut self.f, new_size)?;
            self.move_to_chunk(i)?;
            write_data(&mut self.f, &data, new_size)?;
        }

        self.chunk_size = new_size;
        self.num_chunks = new_chunks;
        self.read_index = 0;
        self.sync()
    }
}

impl Drop for Queue {
    fn drop(&mut self) {
        let _ = self.sync();
    }
}

#[derive(Debug)]
pub struct Reader<'a> {
    q: &'a mut Queue,
    read_index: u64,
}

impl<'a> Reader<'a> {
    pub fn read(&mut self) -> io::Result<Option<Vec<u8>>> {
        if self.read_index >= self.q.num_chunks {
            return io::Result::Ok(Option::None);
        }
        self.q.move_to_chunk(self.read_index)?;
        let (n, data) = read_data(&mut self.q.f, self.q.chunk_size)?;
        self.read_index += n;
        io::Result::Ok(Option::Some(data))
    }

    pub fn commit(&mut self) -> io::Result<()> {
        self.q.read_index = self.read_index;
        self.q.sync()
    }
}

#[repr(C)]
#[derive(PartialEq, Debug)]
struct QueueHeader {
    magic_number: [u8; 8],
    chunk_size: u64,
    num_chunks: u64,
    read_index: u64,
}

#[repr(C)]
#[derive(PartialEq, Debug)]
struct ChunkHeader {
    actual_size: u64,
    is_final: bool,
}

pub(crate) const MAGIC_NUMBER: [u8; 8] = [b'S', b'I', b'M', b'P', b'L', b'W', b'A', b'L'];
const QUEUE_HEADER_SIZE: usize = size_of::<QueueHeader>();
const CHUNK_HEADER_SIZE: usize = size_of::<ChunkHeader>();

pub(crate) fn read_queue_header<R: io::Read>(r: &mut R) -> io::Result<QueueHeader> {
    let mut buf: [u8; QUEUE_HEADER_SIZE] = [0; QUEUE_HEADER_SIZE];
    r.read_exact(buf.as_mut_slice())?;
    io::Result::Ok(QueueHeader {
        magic_number: *buf[0..8].as_array().unwrap(),
        chunk_size: u64::from_le_bytes(*buf[8..16].as_array().unwrap()),
        num_chunks: u64::from_le_bytes(*buf[16..24].as_array().unwrap()),
        read_index: u64::from_le_bytes(*buf[24..].as_array().unwrap()),
    })
}

fn write_queue_header<W: io::Write>(w: &mut W, h: &QueueHeader) -> io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(QUEUE_HEADER_SIZE);
    buf.write_all(MAGIC_NUMBER.as_slice())?;
    buf.write_all(h.chunk_size.to_le_bytes().as_slice())?;
    buf.write_all(h.num_chunks.to_le_bytes().as_slice())?;
    buf.write_all(h.read_index.to_le_bytes().as_slice())?;
    w.write_all(buf.as_slice())
}

fn read_chunk_header<R: io::Read>(r: &mut R) -> io::Result<ChunkHeader> {
    let mut buf: [u8; CHUNK_HEADER_SIZE] = [0; CHUNK_HEADER_SIZE];
    r.read_exact(buf.as_mut_slice())?;
    io::Result::Ok(ChunkHeader {
        actual_size: u64::from_le_bytes(*buf[..8].as_array().unwrap()),
        is_final: (u64::from_le_bytes(*buf[8..].as_array().unwrap()) > 0),
    })
}

fn write_chunk_header<W: io::Write>(w: &mut W, h: &ChunkHeader) -> io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(CHUNK_HEADER_SIZE);
    buf.write_all(h.actual_size.to_le_bytes().as_slice())?;
    if h.is_final {
        buf.write_all(1u64.to_le_bytes().as_slice())?;
    } else {
        buf.write_all(0u64.to_le_bytes().as_slice())?;
    }
    w.write_all(buf.as_slice())
}

fn write_data<W: io::Write>(w: &mut W, mut data: &[u8], chunk_size: u64) -> io::Result<u64> {
    assert!(chunk_size > 0);
    let mut chunks_written: u64 = 0;
    while (data.len() as u64) > chunk_size {
        write_chunk(w, &data[..(chunk_size as usize)], chunk_size, false)?;
        chunks_written += 1;
        data = &data[(chunk_size as usize)..];
    }
    write_chunk(w, data, chunk_size, true)?;
    io::Result::Ok(chunks_written + 1)
}

fn write_chunk<W: io::Write>(
    w: &mut W,
    data: &[u8],
    chunk_size: u64,
    is_final: bool,
) -> io::Result<()> {
    assert!(chunk_size > 0);
    write_chunk_header(
        w,
        &ChunkHeader {
            actual_size: data.len() as u64,
            is_final,
        },
    )?;
    let mut buf: Vec<u8> = Vec::with_capacity(chunk_size as usize);
    buf.write_all(data)?;
    buf.resize(chunk_size as usize, 0);
    w.write_all(&buf)
}

fn read_data<R: io::Read>(r: &mut R, chunk_size: u64) -> io::Result<(u64, Vec<u8>)> {
    let mut data: Vec<u8> = Vec::with_capacity(chunk_size as usize);
    let mut buf = vec![0; chunk_size as usize];
    let mut chunks_read: u64 = 0;
    loop {
        let h = read_chunk_header(r)?;
        r.read_exact(buf.as_mut_slice())?;
        data.write_all(&buf[..h.actual_size as usize])?;
        chunks_read += 1;
        if h.is_final {
            return io::Result::Ok((chunks_read, data));
        }
    }
}

fn chunk_stride(chunk_size: u64) -> u64 {
    CHUNK_HEADER_SIZE as u64 + chunk_size
}

fn chunk_offset(chunk_number: u64, chunk_size: u64) -> u64 {
    chunk_number * chunk_stride(chunk_size)
}

fn chunk_position(chunk_number: u64, chunk_size: u64) -> u64 {
    QUEUE_HEADER_SIZE as u64 + chunk_offset(chunk_number, chunk_size)
}

#[cfg(test)]
mod tests {
    use std::time;

    use super::*;

    #[test]
    fn constants() {
        assert_eq!(QUEUE_HEADER_SIZE, 32);
        assert_eq!(CHUNK_HEADER_SIZE, 16);
    }

    #[test]
    fn encode_queue_header() {
        let mut buf: Vec<u8> = Vec::new();
        let h = QueueHeader {
            magic_number: MAGIC_NUMBER,
            chunk_size: 1,
            num_chunks: 2,
            read_index: 3,
        };
        assert!(write_queue_header(&mut buf, &h).is_ok());
        assert_eq!(h, read_queue_header(&mut &buf[..]).unwrap());
    }

    #[test]
    fn encode_chunk_header() {
        let mut buf: Vec<u8> = Vec::new();
        let h = ChunkHeader {
            actual_size: 1,
            is_final: true,
        };
        assert!(write_chunk_header(&mut buf, &h).is_ok());
        assert_eq!(h, read_chunk_header(&mut &buf[..]).unwrap());
    }

    #[test]
    fn write_data_small() {
        let mut buf: Vec<u8> = Vec::new();
        let res = write_data(&mut buf, &[1, 2, 3], 4);
        assert!(res.is_ok());
        assert_eq!(1, res.unwrap());
        assert_eq!(
            vec![3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 0],
            buf
        );
    }

    #[test]
    fn write_data_large() {
        let mut buf: Vec<u8> = Vec::new();
        let res = write_data(&mut buf, &[1, 2, 3], 1);
        assert!(res.is_ok());
        assert_eq!(3, res.unwrap());
        assert_eq!(
            vec![
                1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 2, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 3
            ],
            buf
        );
    }

    #[test]
    fn read_written_data_small() {
        let mut buf: Vec<u8> = Vec::new();
        assert!(write_data(&mut buf, &[1, 2, 3], 4).is_ok());
        let res = read_data(&mut &buf[..], 4);
        assert!(res.is_ok());
        let (n, data) = res.unwrap();
        assert_eq!(1, n);
        assert_eq!(vec![1, 2, 3], data);
    }

    #[test]
    fn read_written_data_large() {
        let mut buf: Vec<u8> = Vec::new();
        assert!(write_data(&mut buf, &[1, 2, 3], 1).is_ok());
        let res = read_data(&mut &buf[..], 1);
        assert!(res.is_ok());
        let (n, data) = res.unwrap();
        assert_eq!(3, n);
        assert_eq!(vec![1, 2, 3], data);
    }

    fn tmp() -> String {
        format!(
            "/tmp/{}.wal",
            time::SystemTime::now()
                .duration_since(time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )
    }

    #[test]
    fn open_new_queue() {
        let q = OpenOptions::default().open(tmp());
        assert!(q.is_ok())
    }

    #[test]
    fn open_existing_queue() {
        let p = tmp();
        {
            let q = OpenOptions::default().open(&p);
            assert!(q.is_ok());
        }
        {
            let q = OpenOptions::default().open(&p);
            assert!(q.is_ok());
        }
    }

    #[test]
    fn read_from_existing_queue() {
        let p = tmp();
        {
            let q = OpenOptions::default().open(&p);
            assert!(q.is_ok());
            assert!(q.unwrap().append(&[1, 2, 3]).is_ok())
        }
        {
            let q = OpenOptions::default().open(&p);
            assert!(q.is_ok());
            let res = q.unwrap().begin().read();
            assert!(res.is_ok());
            let data = res.unwrap();
            assert!(data.is_some());
            assert_eq!(data.unwrap(), vec![1, 2, 3]);
        }
    }

    #[test]
    fn reread_from_existing_queue() {
        let p = tmp();
        {
            let q = OpenOptions::default().open(&p);
            assert!(q.is_ok());
            assert!(q.unwrap().append(&[1, 2, 3]).is_ok())
        }
        {
            let q = OpenOptions::default().open(&p);
            assert!(q.is_ok());
            let res = q.unwrap().begin().read();
            assert!(res.is_ok());
            let data = res.unwrap();
            assert!(data.is_some());
            assert_eq!(data.unwrap(), vec![1, 2, 3]);
        }
        {
            let q = OpenOptions::default().open(&p);
            assert!(q.is_ok());
            let res = q.unwrap().begin().read();
            assert!(res.is_ok());
            let data = res.unwrap();
            assert!(data.is_some());
            assert_eq!(data.unwrap(), vec![1, 2, 3]);
        }
    }

    #[test]
    fn reads_persist_after_closing() {
        let p = tmp();
        {
            let res = OpenOptions::default().open(&p);
            assert!(res.is_ok());
            let mut q = res.unwrap();
            assert!(q.append(&[1, 2, 3]).is_ok());
            assert!(q.append(&[4, 5, 6]).is_ok());
        }
        {
            let open_res = OpenOptions::default().open(&p);
            assert!(open_res.is_ok());
            let mut q = open_res.unwrap();
            let mut r = q.begin();
            let res = r.read();
            assert!(res.is_ok());
            let data = res.unwrap();
            assert!(data.is_some());
            assert_eq!(data.unwrap(), vec![1, 2, 3]);
            assert!(r.commit().is_ok());
        }
        {
            let open_res = OpenOptions::default().open(&p);
            assert!(open_res.is_ok());
            let mut q = open_res.unwrap();
            let mut r = q.begin();
            eprintln!("{:?}", r);
            let res = r.read();
            assert!(res.is_ok());
            let data = res.unwrap();
            assert!(data.is_some());
            assert_eq!(data.unwrap(), vec![4, 5, 6]);
        }
    }

    #[test]
    fn resize_smaller() {
        let p = tmp();
        {
            let res = OpenOptions::default().open(&p);
            assert!(res.is_ok());
            let mut q = res.unwrap();
            assert!(q.append(&[1, 2, 3]).is_ok());
        }
        {
            let open_res = OpenOptions::new().chunk_size(1).open(&p);
            assert!(open_res.is_ok());
            let mut q = open_res.unwrap();
            assert_eq!(1, q.chunk_size);
            assert_eq!(3, q.num_chunks);
            let mut r = q.begin();
            let res = r.read();
            assert!(res.is_ok());
            let data = res.unwrap();
            assert!(data.is_some());
            assert_eq!(data.unwrap(), vec![1, 2, 3]);
        }
    }

    #[test]
    fn resize_larger() {
        let p = tmp();
        {
            let res = OpenOptions::default().chunk_size(1).open(&p);
            assert!(res.is_ok());
            let mut q = res.unwrap();
            assert!(q.append(&[1, 2, 3]).is_ok());
        }
        {
            let open_res = OpenOptions::new().chunk_size(4).open(&p);
            assert!(open_res.is_ok());
            let mut q = open_res.unwrap();
            assert_eq!(4, q.chunk_size);
            assert_eq!(1, q.num_chunks);
            let mut r = q.begin();
            let res = r.read();
            assert!(res.is_ok());
            let data = res.unwrap();
            assert!(data.is_some());
            assert_eq!(data.unwrap(), vec![1, 2, 3]);
        }
    }
}
