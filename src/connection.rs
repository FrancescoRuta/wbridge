use std::pin::Pin;

use bytes::BytesMut;

use crate::HEADER_SIZE;


pub trait DuplexConnection {
    type W<'a>: tokio::io::AsyncWrite + Unpin + Send + 'a;
    type R<'a>: tokio::io::AsyncRead + Unpin + Send + 'a;
    fn run_split<'a, WhFn, RhFn>(self, write_half_future: WhFn, read_half_future: RhFn) -> tokio::task::JoinHandle<()>
    where
        Self: 'a,
        WhFn: for<'b> FnOnce(&'b mut ConnectionWriteHalf<Self::W<'b>>) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + 'b>> + Send + 'static,
        RhFn: for<'b> FnOnce(&'b mut ConnectionReadHalf<Self::R<'b>>) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + 'b>> + Send + 'static;
}

impl DuplexConnection for tokio::net::TcpStream {
    type W<'a> = tokio::net::tcp::WriteHalf<'a>;
    type R<'a> = tokio::net::tcp::ReadHalf<'a>;

    fn run_split<'a, WhFn, RhFn>(mut self, write_half_future: WhFn, read_half_future: RhFn) -> tokio::task::JoinHandle<()>
    where
        Self: 'a,
        WhFn: for<'b> FnOnce(&'b mut ConnectionWriteHalf<Self::W<'b>>) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + 'b>> + Send + 'static,
        RhFn: for<'b> FnOnce(&'b mut ConnectionReadHalf<Self::R<'b>>) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + 'b>> + Send + 'static,
    {
        tokio::spawn(async move {
            {
                let (r, w) = self.split();
                let (mut r, mut w) = (ConnectionReadHalf(r), ConnectionWriteHalf(w));
                let r = read_half_future(&mut r);
                let w = write_half_future(&mut w);
                tokio::select! {
                    _ = r => (),
                    _ = w => (),
                }
            }
        })
    }
}

#[cfg(unix)]
impl DuplexConnection for tokio::net::UnixStream {
    type W<'a> = tokio::net::unix::WriteHalf<'a>;
    type R<'a> = tokio::net::unix::ReadHalf<'a>;

    fn run_split<'a, WhFn, RhFn>(mut self, write_half_future: WhFn, read_half_future: RhFn) -> tokio::task::JoinHandle<()>
    where
        Self: 'a,
        WhFn: for<'b> FnOnce(&'b mut ConnectionWriteHalf<Self::W<'b>>) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + 'b>> + Send + 'static,
        RhFn: for<'b> FnOnce(&'b mut ConnectionReadHalf<Self::R<'b>>) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + 'b>> + Send + 'static,
    {
        tokio::spawn(async move {
            {
                let (r, w) = self.split();
                let (mut r, mut w) = (ConnectionReadHalf(r), ConnectionWriteHalf(w));
                let r = read_half_future(&mut r);
                let w = write_half_future(&mut w);
                tokio::select! {
                    _ = r => (),
                    _ = w => (),
                }
            }
        })
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
            cursor += self.0.read(&mut size).await.ok()?;
        }
        let size = u32::from_be_bytes(size) as usize + HEADER_SIZE;
        let mut buffer = BytesMut::with_capacity(size);
        while buffer.len() < size {
            self.0.read_buf(&mut buffer).await.ok()?;
        }
        Some(buffer.freeze())
    }
    
}