# Packet Stream

```rust
#![feature(async_await, await_macro, futures_api)]

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
    await!(sink.send(p));
    let r = await!(stream.next()).unwrap().unwrap();
    assert_eq!(&r.body, &[1,2,3,4,5]);
    assert_eq!(r.id, 12345);
};
```
