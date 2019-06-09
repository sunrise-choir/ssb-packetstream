use byteorder::{BigEndian, ByteOrder};

#[derive(Clone, Debug, PartialEq)]
pub struct Packet {
    pub stream: IsStream,
    pub end: IsEnd,
    pub body_type: BodyType,
    pub id: i32,
    pub body: Vec<u8>,
}
impl Packet {
    pub fn new(
        stream: IsStream,
        end: IsEnd,
        body_type: BodyType,
        id: i32,
        body: Vec<u8>,
    ) -> Packet {
        Packet {
            stream,
            end,
            body_type,
            id,
            body,
        }
    }

    pub fn is_stream(&self) -> bool {
        self.stream == IsStream::Yes
    }

    pub fn is_end(&self) -> bool {
        self.end == IsEnd::Yes
    }

    pub(crate) fn flags(&self) -> u8 {
        self.stream as u8 | self.end as u8 | self.body_type as u8
    }
    pub(crate) fn header(&self) -> [u8; 9] {
        let mut header = [0; 9];
        header[0] = self.flags();
        BigEndian::write_u32(&mut header[1..5], self.body.len() as u32);
        BigEndian::write_i32(&mut header[5..], self.id);
        header
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum IsStream {
    No = 0,
    Yes = 0b0000_1000,
}
impl From<u8> for IsStream {
    fn from(u: u8) -> IsStream {
        match u & IsStream::Yes as u8 {
            0 => IsStream::No,
            _ => IsStream::Yes,
        }
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum IsEnd {
    No = 0,
    Yes = 0b0000_0100,
}
impl From<u8> for IsEnd {
    fn from(u: u8) -> IsEnd {
        match u & IsEnd::Yes as u8 {
            0 => IsEnd::No,
            _ => IsEnd::Yes,
        }
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum BodyType {
    Binary = 0b00,
    Utf8 = 0b01,
    Json = 0b10,
}
impl From<u8> for BodyType {
    fn from(u: u8) -> BodyType {
        match u & 0b11 {
            0b00 => BodyType::Binary,
            0b01 => BodyType::Utf8,
            0b10 => BodyType::Json,
            _ => panic!(),
        }
    }
}
