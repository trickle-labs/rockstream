#[cfg(test)]
mod tests {
    use bytes::{Buf, BytesMut};
    use pgwire::messages::{data::FieldDescription, startup::Authentication, PgWireBackendMessage};
    use std::time::Duration;
    use testcontainers::{core::WaitFor, runners::AsyncRunner, GenericImage, ImageExt};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    #[derive(Debug, PartialEq, Eq)]
    enum Event {
        Columns(Vec<FieldDescription>),
        Row(Vec<Option<String>>),
        Command(String),
        SqlState(String),
        Ready(u8),
        ParseComplete,
        BindComplete,
    }

    async fn read_ready(stream: &mut TcpStream) -> Vec<PgWireBackendMessage> {
        tokio::time::timeout(Duration::from_secs(15), async {
            let mut messages = Vec::new();
            loop {
                let tag = stream.read_u8().await.expect("read backend message tag");
                let length = stream.read_i32().await.unwrap();
                assert!(
                    (4..=1_048_576).contains(&length),
                    "invalid backend frame length {length}"
                );
                let mut frame = BytesMut::from(&[tag][..]);
                frame.extend_from_slice(&length.to_be_bytes());
                frame.resize(length as usize + 1, 0);
                stream.read_exact(&mut frame[5..]).await.unwrap();
                messages.push(PgWireBackendMessage::decode(&mut frame).unwrap().unwrap());
                assert!(frame.is_empty());
                if tag == b'Z' {
                    return messages;
                }
            }
        })
        .await
        .expect("backend did not reach ReadyForQuery within 15s")
    }

    async fn connect(address: impl tokio::net::ToSocketAddrs) -> TcpStream {
        let mut stream = TcpStream::connect(address)
            .await
            .expect("required server unavailable");
        let startup = b"\0\x03\0\0user\0test\0database\0test\0\0";
        stream.write_i32(startup.len() as i32 + 4).await.unwrap();
        stream.write_all(startup).await.unwrap();
        let messages = read_ready(&mut stream).await;
        assert!(messages
            .iter()
            .any(|m| matches!(m, PgWireBackendMessage::Authentication(Authentication::Ok))));
        assert!(
            !messages
                .iter()
                .any(|m| matches!(m, PgWireBackendMessage::ErrorResponse(_))),
            "startup failed: {messages:?}"
        );
        assert!(
            matches!(messages.last(), Some(PgWireBackendMessage::ReadyForQuery(r)) if r.status as u8 == b'I')
        );
        stream
    }

    async fn send(stream: &mut TcpStream, tag: u8, body: &[u8]) {
        stream.write_u8(tag).await.unwrap();
        stream.write_i32(body.len() as i32 + 4).await.unwrap();
        stream.write_all(body).await.unwrap();
    }

    // Both servers receive byte-identical simple queries or Parse/Bind/Describe/Execute/Sync.
    // Arrays use the same binary parameter encoding as tokio-postgres; results use text.
    async fn query(
        stream: &mut TcpStream,
        sql: &str,
        parameter: Option<(u32, &[u8])>,
    ) -> Vec<Event> {
        if let Some((oid, value)) = parameter {
            let mut parse = vec![0];
            parse.extend_from_slice(sql.as_bytes());
            parse.extend_from_slice(&[0, 0, 1]);
            parse.extend_from_slice(&oid.to_be_bytes());
            send(stream, b'P', &parse).await;
            let mut bind = vec![0, 0, 0, 1, 0, 1, 0, 1];
            bind.extend_from_slice(&(value.len() as i32).to_be_bytes());
            bind.extend_from_slice(value);
            bind.extend_from_slice(&[0, 0]);
            send(stream, b'B', &bind).await;
            send(stream, b'D', b"P\0").await;
            send(stream, b'E', &[0, 0, 0, 0, 0]).await;
            send(stream, b'S', &[]).await;
        } else {
            send(stream, b'Q', &[sql.as_bytes(), &[0]].concat()).await;
        }
        read_ready(stream)
            .await
            .into_iter()
            .map(|message| match message {
                PgWireBackendMessage::RowDescription(r) => Event::Columns(r.fields),
                PgWireBackendMessage::DataRow(mut r) => {
                    let values = (0..r.field_count)
                        .map(|_| {
                            let length = r.data.get_i32();
                            if length == -1 {
                                None
                            } else {
                                assert!(length >= 0);
                                Some(
                                    String::from_utf8(r.data.split_to(length as usize).to_vec())
                                        .unwrap(),
                                )
                            }
                        })
                        .collect();
                    assert!(r.data.is_empty());
                    Event::Row(values)
                }
                PgWireBackendMessage::CommandComplete(c) => Event::Command(c.tag),
                PgWireBackendMessage::ErrorResponse(e) => Event::SqlState(
                    e.fields
                        .into_iter()
                        .find(|(tag, _)| *tag == b'C')
                        .expect("SQLSTATE missing")
                        .1,
                ),
                PgWireBackendMessage::ReadyForQuery(r) => Event::Ready(r.status as u8),
                PgWireBackendMessage::ParseComplete(_) => Event::ParseComplete,
                PgWireBackendMessage::BindComplete(_) => Event::BindComplete,
                other => panic!("unexpected corpus response: {other:?}"),
            })
            .collect()
    }

    fn field_matches(actual: &FieldDescription, reference: &FieldDescription) -> bool {
        if actual == reference {
            return true;
        }
        // Bound the gateway's missing origin/size/typmod metadata to these corpus columns.
        let (column, size, modifier) = match (reference.name.as_str(), reference.type_id) {
            ("id", 23) => (1, 4, -1),
            ("id", 20) => (1, 8, -1),
            ("amount", 1700) => (2, -1, (18 << 16) + 4 + 4),
            ("category", 25) => (2, -1, -1),
            _ => return false,
        };
        reference.table_id > 0
            && reference.column_id == column
            && reference.type_size == size
            && reference.type_modifier == modifier
            && *actual
                == FieldDescription::new(
                    reference.name.clone(),
                    0,
                    0,
                    reference.type_id,
                    0,
                    0,
                    reference.format_code,
                )
    }

    fn compare(sql: &str, actual: &[Event], reference: &[Event]) -> Result<(), String> {
        for index in 0..actual.len().max(reference.len()) {
            let same = match (actual.get(index), reference.get(index)) {
                (Some(Event::Command(a)), Some(Event::Command(r)))
                    if sql.starts_with("CREATE TABLE ")
                        && a == "CREATE TABLE 0"
                        && r == "CREATE TABLE" =>
                {
                    true
                }
                (Some(Event::Columns(a)), Some(Event::Columns(r))) => {
                    a.len() == r.len() && a.iter().zip(r).all(|(a, r)| field_matches(a, r))
                }
                (a, r) => a == r,
            };
            if !same {
                return Err(format!(
                    "statement: {sql}\nmessage {index}\nPostgreSQL: {:?}\nRockstream: {:?}",
                    reference.get(index),
                    actual.get(index)
                ));
            }
        }
        Ok(())
    }

    async fn both(
        rockstream: &mut TcpStream,
        postgres: &mut TcpStream,
        sql: &str,
        parameter: Option<(u32, &[u8])>,
    ) -> Vec<Event> {
        let reference = query(postgres, sql, parameter).await;
        let actual = query(rockstream, sql, parameter).await;
        eprintln!("statement: {sql}\nPostgreSQL: {reference:#?}\nRockstream: {actual:#?}");
        compare(sql, &actual, &reference).unwrap_or_else(|difference| panic!("{difference}"));
        reference
    }

    fn row(values: &[Option<&str>]) -> Event {
        Event::Row(values.iter().map(|v| v.map(str::to_owned)).collect())
    }

    fn assert_rows(events: &[Event], expected: Vec<Event>, tag: &str, status: u8, prepared: bool) {
        assert!(matches!(events.first(), Some(Event::ParseComplete)) == prepared);
        let column_index = if prepared {
            assert_eq!(events.get(1), Some(&Event::BindComplete));
            2
        } else {
            0
        };
        assert!(
            matches!(events.get(column_index), Some(Event::Columns(_))),
            "missing row description: {events:?}"
        );
        let mut tail = expected;
        tail.push(Event::Command(tag.to_owned()));
        tail.push(Event::Ready(status));
        assert_eq!(&events[column_index + 1..], tail);
    }

    #[tokio::test]
    #[ignore = "requires Docker; mandatory CI job runs with --include-ignored"]
    async fn pinned_postgres_numeric_and_array_corpus() {
        use postgres_types::{ToSql, Type};
        let postgres = GenericImage::new("postgres", "18.0")
            .with_wait_for(WaitFor::message_on_stderr(
                "database system is ready to accept connections",
            ))
            .with_env_var("POSTGRES_USER", "test")
            .with_env_var("POSTGRES_DB", "test")
            .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
            .start()
            .await
            .expect("mandatory PostgreSQL 18.0 reference could not start (Docker/image required)");
        let mut reference = connect((
            postgres.get_host().await.unwrap().to_string(),
            postgres.get_host_port_ipv4(5432).await.unwrap(),
        ))
        .await;
        let version = query(&mut reference, "SHOW server_version_num", None).await;
        assert_eq!(
            &version[1..],
            &[
                row(&[Some("180000")]),
                Event::Command("SHOW".into()),
                Event::Ready(b'I')
            ]
        );
        let (address, handle, _dir) = super::super::start_gateway_server().await;
        let mut gateway = connect(address).await;

        for (sql, tag) in [
            (
                "CREATE TABLE t_numeric (id INT, amount DECIMAL(18, 4))",
                "CREATE TABLE",
            ),
            (
                "INSERT INTO t_numeric VALUES (1, 1234.5678), (2, 9999.0000)",
                "INSERT 0 2",
            ),
            ("BEGIN", "BEGIN"),
        ] {
            let events = both(&mut gateway, &mut reference, sql, None).await;
            assert_eq!(
                events,
                vec![
                    Event::Command(tag.into()),
                    Event::Ready(if sql == "BEGIN" { b'T' } else { b'I' })
                ]
            );
        }
        let numeric_sql = "SELECT id, amount FROM t_numeric ORDER BY id";
        let numeric_rows = || {
            vec![
                row(&[Some("1"), Some("1234.5678")]),
                row(&[Some("2"), Some("9999.0000")]),
            ]
        };
        let events = both(&mut gateway, &mut reference, numeric_sql, None).await;
        assert_rows(&events, numeric_rows(), "SELECT 2", b'T', false);
        let events = both(
            &mut gateway,
            &mut reference,
            "INSERT INTO t_numeric VALUES (3, 100000000000000.0000)",
            None,
        )
        .await;
        assert_eq!(
            events,
            vec![Event::SqlState("22003".into()), Event::Ready(b'E')]
        );
        let events = both(&mut gateway, &mut reference, numeric_sql, None).await;
        assert_eq!(
            events,
            vec![Event::SqlState("25P02".into()), Event::Ready(b'E')]
        );
        let events = both(&mut gateway, &mut reference, "ROLLBACK", None).await;
        assert_eq!(
            events,
            vec![Event::Command("ROLLBACK".into()), Event::Ready(b'I')]
        );
        let events = both(&mut gateway, &mut reference, numeric_sql, None).await;
        assert_rows(&events, numeric_rows(), "SELECT 2", b'I', false);
        // Also check statement atomicity when a valid row precedes an overflow in autocommit.
        let events = both(
            &mut gateway,
            &mut reference,
            "INSERT INTO t_numeric VALUES (3, 1.0000), (4, 100000000000000.0000)",
            None,
        )
        .await;
        assert_eq!(
            events,
            vec![Event::SqlState("22003".into()), Event::Ready(b'I')]
        );
        let events = both(&mut gateway, &mut reference, numeric_sql, None).await;
        assert_rows(&events, numeric_rows(), "SELECT 2", b'I', false);

        for (sql, tag) in [
            (
                "CREATE TABLE t_items (id BIGINT, category TEXT)",
                "CREATE TABLE",
            ),
            (
                "INSERT INTO t_items VALUES (10, 'electronics'), (20, 'clothing'), (30, 'books')",
                "INSERT 0 3",
            ),
        ] {
            let events = both(&mut gateway, &mut reference, sql, None).await;
            assert_eq!(events, vec![Event::Command(tag.into()), Event::Ready(b'I')]);
        }
        let mut text_array = BytesMut::new();
        vec!["electronics", "books"]
            .to_sql(&Type::TEXT_ARRAY, &mut text_array)
            .unwrap();
        let events = both(
            &mut gateway,
            &mut reference,
            "SELECT id FROM t_items WHERE category = ANY($1) ORDER BY id",
            Some((1009, &text_array)),
        )
        .await;
        assert_rows(
            &events,
            vec![row(&[Some("10")]), row(&[Some("30")])],
            "SELECT 2",
            b'I',
            true,
        );
        let mut int_array = BytesMut::new();
        vec![20i64, 30i64]
            .to_sql(&Type::INT8_ARRAY, &mut int_array)
            .unwrap();
        let events = both(
            &mut gateway,
            &mut reference,
            "SELECT id FROM t_items WHERE id = ANY($1) ORDER BY id",
            Some((1016, &int_array)),
        )
        .await;
        assert_rows(
            &events,
            vec![row(&[Some("20")]), row(&[Some("30")])],
            "SELECT 2",
            b'I',
            true,
        );
        let mut empty_array = BytesMut::new();
        Vec::<i64>::new()
            .to_sql(&Type::INT8_ARRAY, &mut empty_array)
            .unwrap();
        let events = both(
            &mut gateway,
            &mut reference,
            "SELECT id FROM t_items WHERE id = ANY($1) ORDER BY id",
            Some((1016, &empty_array)),
        )
        .await;
        assert_rows(&events, vec![], "SELECT 0", b'I', true);
        let events = both(
            &mut gateway,
            &mut reference,
            "INSERT INTO t_items VALUES (40, NULL)",
            None,
        )
        .await;
        assert_eq!(
            events,
            vec![Event::Command("INSERT 0 1".into()), Event::Ready(b'I')]
        );
        let events = both(
            &mut gateway,
            &mut reference,
            "SELECT id, category FROM t_items WHERE id = 40",
            None,
        )
        .await;
        assert_rows(
            &events,
            vec![row(&[Some("40"), None])],
            "SELECT 1",
            b'I',
            false,
        );
        handle.abort();
    }

    #[test]
    fn altered_row_value_fails() {
        assert_eq!(compare("SELECT id", &[row(&[Some("11")])], &[row(&[Some("10")])]), Err("statement: SELECT id\nmessage 0\nPostgreSQL: Some(Row([Some(\"10\")]))\nRockstream: Some(Row([Some(\"11\")]))".into()));
    }

    #[test]
    fn altered_type_fails() {
        let columns =
            |oid| Event::Columns(vec![FieldDescription::new("id".into(), 0, 0, oid, 0, 0, 0)]);
        assert_eq!(compare("SELECT id", &[columns(25)], &[Event::Columns(vec![FieldDescription::new("id".into(), 16384, 1, 20, 8, -1, 0)])]), Err("statement: SELECT id\nmessage 0\nPostgreSQL: Some(Columns([FieldDescription { name: \"id\", table_id: 16384, column_id: 1, type_id: 20, type_size: 8, type_modifier: -1, format_code: 0 }]))\nRockstream: Some(Columns([FieldDescription { name: \"id\", table_id: 0, column_id: 0, type_id: 25, type_size: 0, type_modifier: 0, format_code: 0 }]))".into()));
    }

    #[test]
    fn metadata_exceptions_are_bounded() {
        let reference = FieldDescription::new("amount".into(), 16384, 2, 1700, -1, 1179656, 0);
        let gateway = || FieldDescription::new("amount".into(), 0, 0, 1700, 0, 0, 0);
        assert!(field_matches(&gateway(), &reference));
        for field in 0..7 {
            let mut changed = gateway();
            match field {
                0 => changed.name = "other".into(),
                1 => changed.table_id = 1,
                2 => changed.column_id = 1,
                3 => changed.type_id = 25,
                4 => changed.type_size = -1,
                5 => changed.type_modifier = -1,
                6 => changed.format_code = 1,
                _ => unreachable!(),
            }
            assert!(
                !field_matches(&changed, &reference),
                "unexpected allowance for metadata field {field}"
            );
        }
    }

    #[test]
    fn altered_sqlstate_fails() {
        assert_eq!(compare("INSERT overflow", &[Event::SqlState("XX000".into())], &[Event::SqlState("22003".into())]), Err("statement: INSERT overflow\nmessage 0\nPostgreSQL: Some(SqlState(\"22003\"))\nRockstream: Some(SqlState(\"XX000\"))".into()));
    }

    #[test]
    fn command_complete_cannot_replace_rows() {
        assert_eq!(compare("SELECT id", &[Event::Command("SELECT 1".into())], &[row(&[Some("10")]), Event::Command("SELECT 1".into())]), Err("statement: SELECT id\nmessage 0\nPostgreSQL: Some(Row([Some(\"10\")]))\nRockstream: Some(Command(\"SELECT 1\"))".into()));
    }
}
