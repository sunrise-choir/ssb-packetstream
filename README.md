# Packet Stream

[![Documentation](https://docs.rs/packetstream/badge.svg)](https://docs.rs/packetstream)

```rust
#![feature(async_await)]

use futures::prelude::{SinkExt, StreamExt};
use packetstream::*;

let p = Packet {
    is_stream: IsStream::Yes,
    is_end: IsEnd::No,
    body_type: BodyType::Binary,
    id: 12345,
    body: vec![1,2,3,4,5]
};

let (writer, reader) = async_ringbuffer::ring_buffer(64);

let mut sink = PacketSink::new(writer);
let mut stream = PacketStream::new(reader);

async {
    sink.send(p).await;
    let r = stream.next().await.unwrap().unwrap();
    assert_eq!(&r.body, &[1,2,3,4,5]);
    assert_eq!(r.id, 12345);
};
```
