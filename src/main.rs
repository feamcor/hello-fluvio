use fluvio::{Offset, FluvioError};
use async_std::stream::StreamExt;
use async_std::task::block_on;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let args_slice: Vec<&str> = args.iter().map(|s| &**s).collect();

    let result = match &*args_slice {
        [_, "produce"] => {
            block_on(produce("Hello, Fluvio!"))
        },

        [_, "produce", rest @ ..] => {
            let message = rest.join(" ");
            block_on(produce(&message))
        },

        [_, "consume"] => {
            block_on(consume())
        },

        _ => {
            eprintln!("Usage: hello-fluvio <produce|consume> [message]");
            return;
        },
    };

    if let Err(err) = result {
        eprintln!("Got error: {}", err);
    }
}

async fn produce(message: &str) -> Result<(), FluvioError> {
    let producer = fluvio::producer("hello-fluvio").await?;
    eprintln!("producer created");
    producer.send_record(message, 0).await?;
    eprintln!("message sent");
    Ok(())
}

async fn consume() -> Result<(), FluvioError> {
    let consumer = fluvio::consumer("hello-fluvio", 0).await?;
    eprintln!("consumer created");
    let mut stream = consumer.stream(Offset::beginning()).await?;
    eprintln!("stream created");
    while let Some(Ok(record)) = stream.next().await {
        eprintln!("record fetched");
        if let Some(bytes) = record.try_into_bytes() {
            let string = String::from_utf8_lossy(&bytes);
            println!("Got record: {}", string);
        }
    }

    Ok(())
}
