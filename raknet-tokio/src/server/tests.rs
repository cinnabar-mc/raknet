use super::*;

#[tokio::test]
#[ignore]
async fn rak_server() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_target(true)
        .with_thread_ids(true)
        .with_line_number(true)
        .with_test_writer()
        .compact()
        .try_init();

    let server = RakServer::new("127.0.0.1:19132".parse().unwrap(), |config| {
        config.guid = 123456789;
        config.message = Box::new(*b"MCPE;Chorus;0;1.0.0;0;-1;123456789;Chorus;Survival");
    });

    let mut server = server.start().await.unwrap();

    let mut sessions = Vec::new();

    loop {
        tokio::select! {
            Ok(session) = server.accept() => {
                sessions.push(session);
                debug!("received session")
            }
        }
    }
}
