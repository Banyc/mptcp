pub mod message;
pub mod receiver;
pub mod recv_buf;
pub mod send_buf;
pub mod sender;

#[cfg(test)]
mod tests {

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use crate::{receiver::Receiver, sender::Sender};

    #[tokio::test]
    async fn test_sender_receiver() {
        let streams = 4;
        let mut send_streams = vec![];
        let mut recv_streams = vec![];
        for _ in 0..streams {
            let (tx, rx) = tokio::io::duplex(64);
            send_streams.push(tx);
            recv_streams.push(rx);
        }
        let sender = Sender::new(send_streams);
        let receiver = Receiver::new(recv_streams);

        let mut async_write = sender.into_async_write();
        let mut async_read = receiver.into_async_read();

        let msg = b"hello world";
        async_write.write_all(msg).await.unwrap();
        let mut buf = vec![0; msg.len()];
        async_read.read_exact(&mut buf).await.unwrap();

        assert_eq!(&msg[..], &buf);
    }
}
