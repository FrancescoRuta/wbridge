use bytes::BytesMut;

use crate::HEADER_SIZE;

pub trait DuplexConnection {
    type W<'a>: tokio::io::AsyncWrite + Unpin + Send + 'a where Self: 'a;
    type R<'a>: tokio::io::AsyncRead + Unpin + Send + 'a where Self: 'a;
    fn split<'a>(&'a mut self) -> (ConnectionWriteHalf<Self::W<'a>>, ConnectionReadHalf<Self::R<'a>>);
}

impl DuplexConnection for tokio::net::TcpStream {
    type W<'a> = tokio::net::tcp::WriteHalf<'a>;
    type R<'a> = tokio::net::tcp::ReadHalf<'a>;

    fn split<'a>(&'a mut self) -> (ConnectionWriteHalf<Self::W<'a>>, ConnectionReadHalf<Self::R<'a>>) {
        let (r, w) = self.split();
        (ConnectionWriteHalf(w), ConnectionReadHalf(r))
    }
}

#[cfg(unix)]
impl DuplexConnection for tokio::net::UnixStream {
    type W<'a> = tokio::net::unix::WriteHalf<'a>;
    type R<'a> = tokio::net::unix::ReadHalf<'a>;

    fn split<'a>(&'a mut self) -> (ConnectionWriteHalf<Self::W<'a>>, ConnectionReadHalf<Self::R<'a>>) {
        let (r, w) = self.split();
        (ConnectionWriteHalf(w), ConnectionReadHalf(r))
    }
}

pub struct ConnectionWriteHalf<T>(T);
pub struct ConnectionReadHalf<T>(T);


impl<T> ConnectionWriteHalf<T>
where
    T: tokio::io::AsyncWrite + Unpin
{
    
    pub async fn snd(&mut self, mut data: bytes::Bytes) -> Result<(), ()> {
        use tokio::io::AsyncWriteExt;
        self.0.write_all_buf(&mut data).await.map_err(|_| ())?;
        self.0.flush().await.map_err(|_| ())?;
        Ok(())
    }
    
}

impl<T> ConnectionReadHalf<T>
where
    T: tokio::io::AsyncRead + Unpin
{
    
    pub async fn rcv(&mut self) -> Option<bytes::Bytes> {
        use tokio::io::AsyncReadExt;
        let mut size = [0u8; 4];
        let mut cursor = 0;
        while cursor < size.len() {
            let len = self.0.read(&mut size).await.ok()?;
            cursor += len;
            if len == 0 {
                return None;
            }
        }
        let msg_size = u32::from_be_bytes(size);
        let size = msg_size as usize + HEADER_SIZE;
        let mut buffer = BytesMut::with_capacity(size);
        bytes::BufMut::put_u32(&mut buffer, msg_size);
        while buffer.len() < size {
            self.0.read_buf(&mut buffer).await.ok()?;
        }
        Some(buffer.freeze())
    }
    
}