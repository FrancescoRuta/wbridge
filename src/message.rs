use std::marker::PhantomData;

use crate::HEADER_SIZE;

pub struct FrozenMessage<'a>(pub(crate) u32, pub(crate) bytes::Bytes, pub(crate) PhantomData<&'a ()>);

pub struct PrepardMessage {
    pub(crate) buffer: bytes::BytesMut,
    pub(crate) len: usize,
    pub(crate) max_len: usize,
}

impl PrepardMessage {
    
    pub fn new(size: usize) -> Self {
        Self {
            buffer: bytes::BytesMut::zeroed(HEADER_SIZE + size),
            len: size,
            max_len: size
        }
    }
    
    pub fn get_buffer(&self) -> &[u8] {
        &self.buffer[HEADER_SIZE..(HEADER_SIZE + self.len)]
    }
    
    pub fn get_buffer_mut(&mut self) -> &mut [u8] {
        &mut self.buffer[HEADER_SIZE..(HEADER_SIZE + self.len)]
    }
    
    pub fn resize(&mut self, len: usize) -> Result<(), ()> {
        if len <= self.max_len {
            self.len = len;
            Ok(())
        } else {
            Err(())
        }
    }
    
}

pub struct Message {
    pub from_conn: u32,
    pub from_channel: u32,
    pub to_conn: u32,
    pub to_channel: u32,
    pub data: bytes::Bytes,
}

pub struct BroadcastMessage {
    pub from_conn: u32,
    pub from_channel: u32,
    pub data: bytes::Bytes,
}