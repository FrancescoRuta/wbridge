use bytes::BytesMut;

use crate::HEADER_SIZE;


pub struct Connection<W, R>(ConnectionWriteHalf<W>, ConnectionReadHalf<R>);

impl<W, R> Connection<W, R> {
    
    pub fn new(w: W, r: R) -> Self {
        Self(ConnectionWriteHalf(w), ConnectionReadHalf(r))
    }
    
    pub fn split(self) -> (ConnectionWriteHalf<W>, ConnectionReadHalf<R>) {
        (self.0, self.1)
    }
    
}


pub struct ConnectionWriteHalf<T>(T);
pub struct ConnectionReadHalf<T>(T);


impl<T> ConnectionWriteHalf<T>
where
    T: tokio::io::AsyncWrite + Unpin
{
    
    pub async fn snd(&mut self, data: bytes::Bytes) -> Result<(), ()> {
        use tokio::io::AsyncWriteExt;
        self.0.write_all(&data).await.map_err(|_| ())?;
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
        let size = self.0.read_u32().await.ok()?;
        let mut data = BytesMut::zeroed(size as usize + HEADER_SIZE);
        {
            let size_bytes = size.to_be_bytes();
            let data = &mut *data;
            data[0] = size_bytes[0];
            data[1] = size_bytes[1];
            data[2] = size_bytes[2];
            data[3] = size_bytes[3];
            self.0.read_exact(&mut data[4..]).await.ok()?;
        }
        Some(data.freeze())
    }
    
}