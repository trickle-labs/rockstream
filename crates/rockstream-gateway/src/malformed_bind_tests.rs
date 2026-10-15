//! #119: real Parse/Bind/Execute/Sync transcripts, storage, and joined server tasks.

use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

type Transcript = Vec<(u8, Vec<u8>)>;

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut bytes = vec![tag];
    bytes.extend_from_slice(&(body.len() as u32 + 4).to_be_bytes());
    bytes.extend_from_slice(body);
    bytes
}

// Unexpected wire I/O or a timeout must fail the test at the read boundary.
#[allow(clippy::unwrap_used, clippy::expect_used)]
async fn read_message(socket: &mut TcpStream) -> (u8, Vec<u8>) {
    tokio::time::timeout(Duration::from_secs(10), async {
        let tag = socket.read_u8().await.unwrap();
        let length = socket.read_u32().await.unwrap();
        assert!((4..=65536).contains(&length));
        let mut body = vec![0; length as usize - 4];
        socket.read_exact(&mut body).await.unwrap();
        (tag, body)
    })
    .await
    .expect("server must respond without disconnecting or hanging")
}

async fn transcript(socket: &mut TcpStream) -> Transcript {
    let mut messages = Vec::new();
    loop {
        let message = read_message(socket).await;
        let ready = message.0 == b'Z';
        messages.push(message);
        if ready {
            return messages;
        }
    }
}

// A failed query write invalidates the protocol fixture.
#[allow(clippy::unwrap_used)]
async fn query(socket: &mut TcpStream, sql: &str) -> Transcript {
    let mut body = sql.as_bytes().to_vec();
    body.push(0);
    socket.write_all(&frame(b'Q', &body)).await.unwrap();
    transcript(socket).await
}

fn command(tag: &str, status: u8) -> Transcript {
    vec![
        (b'C', format!("{tag}\0").into_bytes()),
        (b'Z', vec![status]),
    ]
}

fn table_state() -> Transcript {
    let mut description = 3i16.to_be_bytes().to_vec();
    for (name, oid) in [("id", 20u32), ("tag", 25), ("amount", 20)] {
        description.extend_from_slice(name.as_bytes());
        description.push(0);
        description.extend_from_slice(&0u32.to_be_bytes());
        description.extend_from_slice(&0i16.to_be_bytes());
        description.extend_from_slice(&oid.to_be_bytes());
        description.extend_from_slice(&0i16.to_be_bytes());
        description.extend_from_slice(&0i32.to_be_bytes());
        description.extend_from_slice(&0i16.to_be_bytes());
    }
    let mut messages = vec![(b'T', description)];
    for row in [["1", "first", "10"], ["2", "second", "20"]] {
        let mut body = 3i16.to_be_bytes().to_vec();
        for value in row {
            body.extend_from_slice(&(value.len() as i32).to_be_bytes());
            body.extend_from_slice(value.as_bytes());
        }
        messages.push((b'D', body));
    }
    messages.extend(command("SELECT 2", b'I'));
    messages
}

// Parse/Flush I/O must succeed before testing Bind rejection.
#[allow(clippy::unwrap_used)]
async fn prepare(socket: &mut TcpStream, sql: &str, types: &[Type]) {
    let mut body = vec![0]; // unnamed statement
    body.extend_from_slice(sql.as_bytes());
    body.push(0);
    body.extend_from_slice(&(types.len() as i16).to_be_bytes());
    for ty in types {
        body.extend_from_slice(&ty.oid().to_be_bytes());
    }
    let mut request = frame(b'P', &body);
    request.extend(frame(b'H', &[]));
    socket.write_all(&request).await.unwrap();
    assert_eq!(read_message(socket).await, (b'1', vec![]));
}

fn bind(values: &[Option<&[u8]>], formats: &[i16]) -> Vec<u8> {
    let mut body = vec![0, 0]; // unnamed portal and statement
    body.extend_from_slice(&(formats.len() as i16).to_be_bytes());
    for format in formats {
        body.extend_from_slice(&format.to_be_bytes());
    }
    body.extend_from_slice(&(values.len() as i16).to_be_bytes());
    for value in values {
        if let Some(value) = value {
            body.extend_from_slice(&(value.len() as i32).to_be_bytes());
            body.extend_from_slice(value);
        } else {
            body.extend_from_slice(&(-1i32).to_be_bytes());
        }
    }
    body.extend_from_slice(&0i16.to_be_bytes());
    frame(b'B', &body)
}

fn execute() -> Vec<u8> {
    frame(b'E', &[0, 0, 0, 0, 0])
}

fn array(dimensions: i32, flags: i32, oid: u32, length: i32, elements: &[u8]) -> Vec<u8> {
    let mut value = Vec::new();
    for field in [dimensions, flags, oid as i32, length, 1] {
        value.extend_from_slice(&field.to_be_bytes());
    }
    value.extend_from_slice(elements);
    value
}

fn element(length: i32, value: &[u8]) -> Vec<u8> {
    let mut bytes = length.to_be_bytes().to_vec();
    bytes.extend_from_slice(value);
    bytes
}

struct NoopViewReader;

#[async_trait]
impl ViewReader for NoopViewReader {
    async fn read_view(
        &self,
        _: &str,
        _: Option<usize>,
        _: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        Ok(vec![])
    }

    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

// Accept exactly the connections the test opens and join every process_socket task.
#[allow(clippy::unwrap_used)]
async fn connection(
    listener: &TcpListener,
    handler: Arc<GatewayHandler>,
    id: &str,
) -> (TcpStream, tokio::task::JoinHandle<std::io::Result<()>>) {
    let mut client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (socket, _) = listener.accept().await.unwrap();
    let factory = Arc::new(GatewayHandlerFactory { handler });
    let task = tokio::spawn(CONN_ID.scope(id.to_string(), async move {
        pgwire::tokio::process_socket(socket, None, factory).await
    }));
    let mut startup = 196608u32.to_be_bytes().to_vec();
    startup.extend_from_slice(b"user\0test\0database\0test\0\0");
    let mut bytes = (startup.len() as u32 + 4).to_be_bytes().to_vec();
    bytes.extend(startup);
    client.write_all(&bytes).await.unwrap();
    let startup = transcript(&mut client).await;
    assert_eq!(startup.first(), Some(&(b'R', vec![0, 0, 0, 0])));
    assert_eq!(startup.last(), Some(&(b'Z', vec![b'I'])));
    (client, task)
}

// Shutdown errors and server task panics must fail the test rather than be discarded.
#[allow(clippy::unwrap_used, clippy::expect_used)]
async fn terminate(mut client: TcpStream, task: tokio::task::JoinHandle<std::io::Result<()>>) {
    client.write_all(&frame(b'X', &[])).await.unwrap();
    client.shutdown().await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), client.read_u8())
            .await
            .unwrap()
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::UnexpectedEof
    );
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .expect("server task panicked")
        .expect("server task failed");
}

#[tokio::test]
// Fixture setup and socket writes must succeed for the transcript assertions to be valid.
#[allow(clippy::unwrap_used)]
async fn malformed_binary_bind_rejects_without_mutation_and_recovers() {
    let dir = tempfile::TempDir::new().unwrap();
    let store =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Arc::new(
        rockstream_storage::ShardDb::builder("malformed-bind", store)
            .build()
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
        db,
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (mut client, task) = connection(&listener, server.handler().clone(), "malformed").await;
    assert_eq!(
        query(
            &mut client,
            "CREATE TABLE t (id BIGINT, tag TEXT, amount BIGINT)"
        )
        .await,
        command("CREATE TABLE 0", b'I')
    );
    assert_eq!(
        query(
            &mut client,
            "INSERT INTO t VALUES (1, 'first', 10), (2, 'second', 20)"
        )
        .await,
        command("INSERT 0 2", b'I')
    );
    let select = "SELECT id, tag, amount FROM t ORDER BY id";
    assert_eq!(query(&mut client, select).await, table_state());

    // Valid empty, nullable, numeric, and mixed-format values must retain their results.
    let mut two_elements = element(8, &1i64.to_be_bytes());
    two_elements.extend(element(8, &2i64.to_be_bytes()));
    let mut nullable_elements = element(8, &1i64.to_be_bytes());
    nullable_elements.extend(element(-1, &[]));
    nullable_elements.extend(element(8, &2i64.to_be_bytes()));
    for (value, empty, format) in [
        (array(1, 0, 20, 2, &two_elements), false, 1),
        (array(1, 1, 20, 3, &nullable_elements), false, 1),
        (array(0, 0, 20, 0, &[])[..12].to_vec(), true, 1),
        (array(1, 0, 20, 0, &[]), true, 1),
        (b"{1,2}".to_vec(), false, 0),
    ] {
        prepare(
            &mut client,
            "SELECT id, tag, amount FROM t WHERE id = ANY($1) ORDER BY id",
            &[Type::INT8_ARRAY],
        )
        .await;
        let mut request = bind(&[Some(&value)], &[format]);
        request.extend(execute());
        request.extend(frame(b'S', &[]));
        client.write_all(&request).await.unwrap();
        // Extended Execute emits rows and CommandComplete without a RowDescription.
        let mut expected = vec![(b'2', vec![])];
        if empty {
            expected.extend(command("SELECT 0", b'I'));
        } else {
            expected.extend(table_state().into_iter().skip(1));
        }
        assert_eq!(transcript(&mut client).await, expected);
    }
    prepare(
        &mut client,
        "SELECT id, tag, amount FROM t WHERE tag != $1 AND amount >= $2 ORDER BY id",
        &[Type::TEXT, Type::INT8],
    )
    .await;
    for request in [
        bind(&[Some(b"absent"), Some(&10i64.to_be_bytes())], &[0, 1]),
        bind(&[Some(b"absent"), Some(b"10")], &[0]),
        bind(&[Some(b"absent"), Some(b"10")], &[]),
    ] {
        let mut request = request;
        request.extend(execute());
        request.extend(frame(b'S', &[]));
        client.write_all(&request).await.unwrap();
        let mut expected = vec![(b'2', vec![])];
        expected.extend(table_state().into_iter().skip(1));
        assert_eq!(transcript(&mut client).await, expected);
    }

    let valid = array(1, 0, 20, 1, &element(8, &1i64.to_be_bytes()));
    let mut cases = Vec::new();
    let mut overflowing_bounds = array(1, 0, 20, 2, &two_elements);
    overflowing_bounds[16..20].copy_from_slice(&i32::MAX.to_be_bytes());
    cases.push((
        "overflowing array bounds".to_string(),
        Type::INT8_ARRAY,
        overflowing_bounds,
    ));
    for length in 0..20 {
        cases.push((
            format!("truncated header {length}"),
            Type::INT8_ARRAY,
            valid[..length].to_vec(),
        ));
    }
    for (name, value) in [
        ("short length word", array(1, 0, 20, 1, &[0, 0, 0])),
        (
            "short element payload",
            array(1, 0, 20, 1, &element(8, &[0; 7])),
        ),
        (
            "negative element length",
            array(1, 1, 20, 1, &element(-2, &[])),
        ),
        (
            "huge element length",
            array(1, 0, 20, 1, &element(i32::MAX, &[])),
        ),
        (
            "missing second element",
            array(1, 0, 20, 2, &element(8, &1i64.to_be_bytes())),
        ),
        (
            "trailing element",
            array(1, 0, 20, 0, &element(8, &1i64.to_be_bytes())),
        ),
        ("negative dimension count", array(-1, 0, 20, 1, &[])),
        ("multiple dimensions", array(2, 0, 20, 1, &[])),
        ("huge dimension count", array(i32::MAX, 0, 20, 1, &[])),
        ("negative dimension length", array(1, 0, 20, -1, &[])),
        ("huge dimension length", array(1, 0, 20, i32::MAX, &[])),
        (
            "zero dimensions with trailing bytes",
            array(0, 0, 20, 0, &[]),
        ),
        (
            "wrong element oid",
            array(1, 0, 23, 1, &element(4, &[0; 4])),
        ),
        ("invalid flags", array(1, 2, 20, 0, &[])),
        ("null without flag", array(1, 0, 20, 1, &element(-1, &[]))),
    ] {
        cases.push((name.to_string(), Type::INT8_ARRAY, value));
    }
    for (scalar, array_type, width) in [
        (Type::INT2, Type::INT2_ARRAY, 2),
        (Type::INT4, Type::INT4_ARRAY, 4),
        (Type::INT8, Type::INT8_ARRAY, 8),
        (Type::FLOAT4, Type::FLOAT4_ARRAY, 4),
        (Type::FLOAT8, Type::FLOAT8_ARRAY, 8),
    ] {
        for length in [0, width - 1, width + 1] {
            cases.push((
                format!("{scalar} scalar width {length}"),
                scalar.clone(),
                vec![b'1'; length],
            ));
            cases.push((
                format!("{scalar} element width {length}"),
                array_type.clone(),
                array(
                    1,
                    0,
                    scalar.oid(),
                    1,
                    &element(length as i32, &vec![0; length]),
                ),
            ));
        }
    }

    // R1-R5: each payload is rejected at Bind in idle and explicit transaction states.
    for (name, ty, bytes) in cases {
        for in_transaction in [false, true] {
            if in_transaction {
                assert_eq!(query(&mut client, "BEGIN").await, command("BEGIN", b'T'));
            }
            let sql = if matches!(ty.kind(), postgres_types::Kind::Array(_)) {
                "UPDATE t SET amount = 99 WHERE id = ANY($1)"
            } else {
                "INSERT INTO t VALUES (3, 'partial', 30), (4, 'invalid', $1)"
            };
            prepare(&mut client, sql, std::slice::from_ref(&ty)).await;
            let mut request = bind(&[Some(&bytes)], &[1]);
            request.extend(execute());
            // Even a mutating simple Query must be ignored until Sync after a Bind error.
            request.extend(frame(b'Q', b"DELETE FROM t\0"));
            request.extend(frame(b'S', &[]));
            client.write_all(&request).await.unwrap();
            assert_eq!(
                transcript(&mut client).await,
                vec![
                    (
                        b'E',
                        format!(
                            "SERROR\0C22P03\0Minvalid binary representation for type {}\0\0",
                            ty.name()
                        )
                        .into_bytes()
                    ),
                    (b'Z', vec![if in_transaction { b'E' } else { b'I' }]),
                ],
                "{name}, transaction={in_transaction}"
            );
            if let Some(buffer) = server.handler().write_buffers.get("malformed") {
                assert!(matches!(buffer.ops(), []), "{name}: {:?}", buffer.ops());
            }
            if in_transaction {
                assert_eq!(
                    query(&mut client, "ROLLBACK").await,
                    command("ROLLBACK", b'I')
                );
            }
            assert_eq!(query(&mut client, select).await, table_state(), "{name}");
        }
    }

    for request in [
        bind(&[Some(&1i64.to_be_bytes())], &[0, 1]),
        bind(&[Some(&1i64.to_be_bytes())], &[2]),
        bind(&[Some(&1i64.to_be_bytes())], &[-1]),
        bind(&[], &[]),
        bind(&[Some(&1i64.to_be_bytes()), None], &[1]),
    ] {
        prepare(
            &mut client,
            "UPDATE t SET amount = $1 WHERE id = 1",
            &[Type::INT8],
        )
        .await;
        let mut request = request;
        request.extend(execute());
        request.extend(frame(b'S', &[]));
        client.write_all(&request).await.unwrap();
        assert_eq!(
            transcript(&mut client).await,
            vec![
                (
                    b'E',
                    b"SERROR\0C08P01\0Minvalid Bind parameter count or format codes\0\0".to_vec()
                ),
                (b'Z', vec![b'I']),
            ]
        );
        assert_eq!(query(&mut client, select).await, table_state());
    }

    // A second live connection checks that rejection cannot affect other sessions.
    let (mut independent, independent_task) =
        connection(&listener, server.handler().clone(), "independent").await;
    assert_eq!(query(&mut independent, select).await, table_state());
    terminate(independent, independent_task).await;
    terminate(client, task).await;
}
