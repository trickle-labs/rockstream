use pgwire::api::Type;
use rockstream_gateway::catalog_stubs::{
    arrow_type_to_pg_data_type, arrow_type_to_pg_oid, PG_OID_ARRAY_BOOL, PG_OID_ARRAY_FLOAT8,
    PG_OID_ARRAY_INT4, PG_OID_ARRAY_INT8, PG_OID_ARRAY_TEXT, PG_OID_ARRAY_UUID, PG_OID_BOOL,
    PG_OID_BYTEA, PG_OID_CHAR, PG_OID_DATE, PG_OID_FLOAT4, PG_OID_FLOAT8, PG_OID_INT2, PG_OID_INT4,
    PG_OID_INT8, PG_OID_INTERVAL, PG_OID_JSON, PG_OID_JSONB, PG_OID_NUMERIC, PG_OID_TIME,
    PG_OID_TIMESTAMP, PG_OID_TIMESTAMPTZ, PG_OID_UUID, PG_OID_VARCHAR,
};
use rockstream_gateway::protocol::pg_type_from_name;
use rockstream_gateway::{
    catalog_stubs::{CatalogColumn, CatalogStubs, CatalogView},
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use std::sync::Arc;
use tokio_postgres::NoTls;

#[test]
fn test_type_mappings_s1() {
    // Assert OID mapping round-trips for every new type
    let test_cases = vec![
        ("Int16", PG_OID_INT2, "smallint", Type::INT2),
        ("Int32", PG_OID_INT4, "integer", Type::INT4),
        ("Int64", PG_OID_INT8, "bigint", Type::INT8),
        ("Float32", PG_OID_FLOAT4, "real", Type::FLOAT4),
        ("Float64", PG_OID_FLOAT8, "double precision", Type::FLOAT8),
        ("Boolean", PG_OID_BOOL, "boolean", Type::BOOL),
        ("Binary", PG_OID_BYTEA, "bytea", Type::BYTEA),
        (
            "Timestamp",
            PG_OID_TIMESTAMP,
            "timestamp without time zone",
            Type::TIMESTAMP,
        ),
        (
            "TimestampTz",
            PG_OID_TIMESTAMPTZ,
            "timestamp with time zone",
            Type::TIMESTAMPTZ,
        ),
        ("Date32", PG_OID_DATE, "date", Type::DATE),
        ("Time32", PG_OID_TIME, "time without time zone", Type::TIME),
        ("Uuid", PG_OID_UUID, "uuid", Type::UUID),
        ("Decimal", PG_OID_NUMERIC, "numeric", Type::NUMERIC),
        ("Json", PG_OID_JSON, "json", Type::JSON),
        ("Jsonb", PG_OID_JSONB, "jsonb", Type::JSONB),
        (
            "Varchar",
            PG_OID_VARCHAR,
            "character varying",
            Type::VARCHAR,
        ),
        ("Char", PG_OID_CHAR, "character", Type::CHAR),
        ("Interval", PG_OID_INTERVAL, "interval", Type::INTERVAL),
        // Array variants
        ("_int4", PG_OID_ARRAY_INT4, "integer[]", Type::INT4_ARRAY),
        ("_int8", PG_OID_ARRAY_INT8, "bigint[]", Type::INT8_ARRAY),
        ("_text", PG_OID_ARRAY_TEXT, "text[]", Type::TEXT_ARRAY),
        (
            "_float8",
            PG_OID_ARRAY_FLOAT8,
            "double precision[]",
            Type::FLOAT8_ARRAY,
        ),
        ("_bool", PG_OID_ARRAY_BOOL, "boolean[]", Type::BOOL_ARRAY),
        ("_uuid", PG_OID_ARRAY_UUID, "uuid[]", Type::UUID_ARRAY),
    ];

    for (arrow_name, expected_oid, expected_pg_type_name, expected_type) in test_cases {
        assert_eq!(
            arrow_type_to_pg_oid(arrow_name),
            expected_oid,
            "arrow_type_to_pg_oid({})",
            arrow_name
        );
        assert_eq!(
            arrow_type_to_pg_data_type(arrow_name),
            expected_pg_type_name,
            "arrow_type_to_pg_data_type({})",
            arrow_name
        );
        assert_eq!(
            pg_type_from_name(arrow_name),
            expected_type,
            "pg_type_from_name({})",
            arrow_name
        );
    }
}

struct MockViewReader {
    row_data: Vec<u8>,
}

#[async_trait::async_trait]
impl ViewReader for MockViewReader {
    async fn read_view(
        &self,
        _view_name: &str,
        _limit: Option<usize>,
        _strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        Ok(vec![self.row_data.clone()])
    }
    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

#[tokio::test]
async fn test_binary_encoding_unit() {
    let catalog = CatalogStubs::new();
    catalog.add_view(CatalogView {
        name: "numeric_view".to_string(),
        sql: "SELECT * FROM numeric_view".to_string(),
        columns: vec![
            CatalogColumn {
                name: "c_int2".to_string(),
                data_type: "Int16".to_string(),
            },
            CatalogColumn {
                name: "c_int4".to_string(),
                data_type: "Int32".to_string(),
            },
            CatalogColumn {
                name: "c_int8".to_string(),
                data_type: "Int64".to_string(),
            },
            CatalogColumn {
                name: "c_float4".to_string(),
                data_type: "Float32".to_string(),
            },
            CatalogColumn {
                name: "c_float64".to_string(),
                data_type: "Float64".to_string(),
            },
            CatalogColumn {
                name: "c_bool".to_string(),
                data_type: "Boolean".to_string(),
            },
        ],
        namespace: "public".to_string(),
        op_id: None,
    });

    let row_data = b"32767\t2147483647\t9223372036854775807\t123.45\t678.901\ttrue".to_vec();
    let reader = Arc::new(MockViewReader { row_data });

    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(addr, Arc::new(catalog), reader);
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let port = local_addr.port();

    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=test dbname=test"),
        NoTls,
    )
    .await
    .expect("connect failed");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    // query using extended query protocol (prepares and binds, requesting binary format)
    let stmt = client.prepare("SELECT * FROM numeric_view").await.unwrap();
    let rows = client.query(&stmt, &[]).await.unwrap();

    assert_eq!(rows.len(), 1);
    let row = &rows[0];

    // Assert correct types and values
    let val_int2: i16 = row.get(0);
    let val_int4: i32 = row.get(1);
    let val_int8: i64 = row.get(2);
    let val_float4: f32 = row.get(3);
    let val_float64: f64 = row.get(4);
    let val_bool: bool = row.get(5);

    assert_eq!(val_int2, 32767);
    assert_eq!(val_int4, 2147483647);
    assert_eq!(val_int8, 9223372036854775807);
    assert!((val_float4 - 123.45).abs() < 1e-4);
    assert!((val_float64 - 678.901).abs() < 1e-9);
    assert!(val_bool);
}

#[cfg(feature = "testcontainers")]
#[tokio::test]
async fn test_binary_encoding_postgres_comparison() {
    use testcontainers::runners::AsyncRunner;
    use testcontainers::GenericImage;
    use testcontainers::ImageExt;

    let postgres_container = GenericImage::new("postgres", "14-alpine")
        .with_env_var("POSTGRES_DB", "postgres")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .start()
        .await
        .expect("Failed to start Postgres container");

    let host = postgres_container.get_host().await.expect("get host");
    let pg_port = postgres_container
        .get_host_port_ipv4(5432)
        .await
        .expect("get port");

    // Connect to real Postgres container with retry loop
    let pg_client = {
        let mut client_opt = None;
        for i in 0..30 {
            match tokio_postgres::connect(
                &format!(
                    "host={host} port={pg_port} user=postgres password=postgres dbname=postgres"
                ),
                NoTls,
            )
            .await
            {
                Ok((client, conn)) => {
                    tokio::spawn(async move {
                        let _ = conn.await;
                    });
                    client_opt = Some(client);
                    break;
                }
                Err(e) => {
                    if i == 29 {
                        panic!("Failed to connect to real PG container: {:?}", e);
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                }
            }
        }
        client_opt.unwrap()
    };

    // Create a table with the same types and insert the same values
    pg_client.execute(
        "CREATE TABLE test_numeric (c_int2 int2, c_int4 int4, c_int8 int8, c_float4 float4, c_float64 float8, c_bool bool)",
        &[],
    )
    .await
    .unwrap();

    pg_client.execute(
        "INSERT INTO test_numeric VALUES (32767, 2147483647, 9223372036854775807, 123.45, 678.901, true)",
        &[],
    )
    .await
    .unwrap();

    let pg_stmt = pg_client
        .prepare("SELECT * FROM test_numeric")
        .await
        .unwrap();
    let pg_rows = pg_client.query(&pg_stmt, &[]).await.unwrap();
    assert_eq!(pg_rows.len(), 1);
    let pg_row = &pg_rows[0];

    // Now set up our gateway server
    let catalog = CatalogStubs::new();
    catalog.add_view(CatalogView {
        name: "numeric_view".to_string(),
        sql: "SELECT * FROM numeric_view".to_string(),
        columns: vec![
            CatalogColumn {
                name: "c_int2".to_string(),
                data_type: "Int16".to_string(),
            },
            CatalogColumn {
                name: "c_int4".to_string(),
                data_type: "Int32".to_string(),
            },
            CatalogColumn {
                name: "c_int8".to_string(),
                data_type: "Int64".to_string(),
            },
            CatalogColumn {
                name: "c_float4".to_string(),
                data_type: "Float32".to_string(),
            },
            CatalogColumn {
                name: "c_float64".to_string(),
                data_type: "Float64".to_string(),
            },
            CatalogColumn {
                name: "c_bool".to_string(),
                data_type: "Boolean".to_string(),
            },
        ],
        namespace: "public".to_string(),
        op_id: None,
    });

    let row_data = b"32767\t2147483647\t9223372036854775807\t123.45\t678.901\ttrue".to_vec();
    let reader = Arc::new(MockViewReader { row_data });

    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(addr, Arc::new(catalog), reader);
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let port = local_addr.port();

    // Connect to our gateway server
    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=test dbname=test"),
        NoTls,
    )
    .await
    .expect("connect to gateway failed");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let stmt = client.prepare("SELECT * FROM numeric_view").await.unwrap();
    let rows = client.query(&stmt, &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let row = &rows[0];

    // Let's assert that the binary representation returned for each column is identical to what real PG returned
    assert_eq!(row.get::<_, i16>(0), pg_row.get::<_, i16>(0));
    assert_eq!(row.get::<_, i32>(1), pg_row.get::<_, i32>(1));
    assert_eq!(row.get::<_, i64>(2), pg_row.get::<_, i64>(2));
    assert_eq!(row.get::<_, f32>(3), pg_row.get::<_, f32>(3));
    assert_eq!(row.get::<_, f64>(4), pg_row.get::<_, f64>(4));
    assert_eq!(row.get::<_, bool>(5), pg_row.get::<_, bool>(5));
}

#[cfg(feature = "testcontainers")]
#[tokio::test]
async fn test_orm_conformance() {
    use testcontainers::runners::AsyncRunner;
    use testcontainers::GenericImage;
    use testcontainers::ImageExt;

    async fn run_cmd_checked(
        container: &testcontainers::ContainerAsync<testcontainers::GenericImage>,
        cmd: Vec<&str>,
        name: &str,
    ) {
        use tokio::io::AsyncReadExt;
        let exec_cmd = testcontainers::core::ExecCommand::new(cmd);
        let mut exec_res = container.exec(exec_cmd).await.expect("Failed to call exec");
        let mut stdout_buf = Vec::new();
        let mut stderr_buf = Vec::new();
        let _ = exec_res.stdout().read_to_end(&mut stdout_buf).await;
        let _ = exec_res.stderr().read_to_end(&mut stderr_buf).await;
        let exit_code = exec_res.exit_code().await.expect("Failed to get exit code");
        let stdout = String::from_utf8_lossy(&stdout_buf);
        let stderr = String::from_utf8_lossy(&stderr_buf);
        println!("CMD STDOUT:\n{}", stdout);
        println!("CMD STDERR:\n{}", stderr);
        assert_eq!(
            exit_code,
            Some(0),
            "{} failed (exit code: {:?}):\nstdout: {}\nstderr: {}",
            name,
            exit_code,
            stdout,
            stderr
        );
    }

    // Set up gateway server
    let catalog = CatalogStubs::new();
    catalog.add_view(CatalogView {
        name: "numeric_view".to_string(),
        sql: "SELECT * FROM numeric_view".to_string(),
        columns: vec![
            CatalogColumn {
                name: "c_int2".to_string(),
                data_type: "Int16".to_string(),
            },
            CatalogColumn {
                name: "c_int4".to_string(),
                data_type: "Int32".to_string(),
            },
            CatalogColumn {
                name: "c_int8".to_string(),
                data_type: "Int64".to_string(),
            },
            CatalogColumn {
                name: "c_float4".to_string(),
                data_type: "Float32".to_string(),
            },
            CatalogColumn {
                name: "c_float64".to_string(),
                data_type: "Float64".to_string(),
            },
            CatalogColumn {
                name: "c_bool".to_string(),
                data_type: "Boolean".to_string(),
            },
        ],
        namespace: "public".to_string(),
        op_id: None,
    });

    let row_data = b"32767\t2147483647\t9223372036854775807\t123.45\t678.901\ttrue".to_vec();
    let reader = Arc::new(MockViewReader { row_data });

    // Listen on 0.0.0.0 so that container can connect to it!
    let addr: std::net::SocketAddr = "0.0.0.0:0".parse().unwrap();
    let server = GatewayServer::with_catalog(addr, Arc::new(catalog), reader);
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let port = local_addr.port();

    // Determine host IP address reachable from container
    fn get_host_ip() -> String {
        if let Some(socket) = std::net::UdpSocket::bind("0.0.0.0:0").ok() {
            if socket.connect("8.8.8.8:80").is_ok() {
                if let Some(addr) = socket.local_addr().ok() {
                    let ip = addr.ip().to_string();
                    if ip != "127.0.0.1" && ip != "0.0.0.0" {
                        return ip;
                    }
                }
            }
        }
        "host.docker.internal".to_string()
    }
    let host_ip = get_host_ip();

    // 1. SQLAlchemy E2E Test
    let python_container = GenericImage::new("python", "3.11-slim")
        .with_cmd(["sleep", "3600"])
        .start()
        .await
        .expect("Failed to start Python container");

    run_cmd_checked(
        &python_container,
        vec!["pip", "install", "-q", "sqlalchemy", "psycopg2-binary"],
        "pip install",
    )
    .await;

    let python_script = format!(
        "import sqlalchemy as sa
engine = sa.create_engine('postgresql+psycopg2://test:test@{}:{}/test')
insp = sa.inspect(engine)
cols = insp.get_columns('numeric_view')
print('COLS:', cols)
assert len(cols) == 6, f'Expected 6 columns, got {{len(cols)}}'
names = [c['name'] for c in cols]
assert 'c_int2' in names
assert 'c_bool' in names",
        host_ip, port
    );
    run_cmd_checked(
        &python_container,
        vec![
            "sh",
            "-c",
            &format!("cat << 'EOF' > /tmp/diagnostic.py\n{}\nEOF", python_script),
        ],
        "write diagnostic.py",
    )
    .await;
    run_cmd_checked(
        &python_container,
        vec!["python", "/tmp/diagnostic.py"],
        "SQLAlchemy inspect",
    )
    .await;

    // 2. Prisma E2E Test
    let node_container = GenericImage::new("node", "20-slim")
        .with_cmd(["sleep", "3600"])
        .start()
        .await
        .expect("Failed to start Node container");

    run_cmd_checked(
        &node_container,
        vec![
            "sh",
            "-c",
            "mkdir -p /app && cd /app && npm init -y && npm install -q prisma @prisma/client",
        ],
        "npm install prisma",
    )
    .await;

    run_cmd_checked(
        &node_container,
        vec!["sh", "-c", "cd /app && npx prisma init"],
        "prisma init",
    )
    .await;

    let pull_cmd = format!(
        "cd /app && DATABASE_URL=postgresql://test:test@{}:{}/test npx prisma db pull",
        host_ip, port
    );
    run_cmd_checked(
        &node_container,
        vec!["sh", "-c", &pull_cmd],
        "prisma db pull",
    )
    .await;

    // 3. Hibernate / JDBC E2E Test
    let openjdk_container = GenericImage::new("eclipse-temurin", "17-jdk")
        .with_cmd(["sleep", "3600"])
        .start()
        .await
        .expect("Failed to start eclipse-temurin container");

    // Install curl
    run_cmd_checked(
        &openjdk_container,
        vec!["sh", "-c", "apt-get update -qy && apt-get install -qy curl"],
        "apt-get install curl",
    )
    .await;

    // Download postgresql JDBC driver
    run_cmd_checked(
        &openjdk_container,
        vec![
            "curl",
            "-LOs",
            "https://jdbc.postgresql.org/download/postgresql-42.7.2.jar",
        ],
        "download JDBC driver",
    )
    .await;

    // Write Java test program
    let java_code = format!(
        "import java.sql.*;\n\
         public class TestJDBC {{\n\
             public static void main(String[] args) throws Exception {{\n\
                 String url = \"jdbc:postgresql://{}:{}/test\";\n\
                 Connection conn = DriverManager.getConnection(url, \"test\", \"test\");\n\
                 DatabaseMetaData meta = conn.getMetaData();\n\
                 ResultSet rs = meta.getColumns(null, \"public\", \"numeric_view\", \"%\");\n\
                 int count = 0;\n\
                 while (rs.next()) {{\n\
                     count++;\n\
                     System.out.println(\"COLUMN: \" + rs.getString(\"COLUMN_NAME\") + \" - \" + rs.getString(\"TYPE_NAME\"));\n\
                 }}\n\
                 if (count != 6) {{\n\
                     throw new RuntimeException(\"Expected 6 columns, got \" + count);\n\
                 }}\n\
                 conn.close();\n\
             }}\n\
         }}",
        host_ip, port
    );
    run_cmd_checked(
        &openjdk_container,
        vec![
            "sh",
            "-c",
            &format!("cat << 'EOF' > TestJDBC.java\n{}\nEOF", java_code),
        ],
        "write TestJDBC.java",
    )
    .await;

    run_cmd_checked(
        &openjdk_container,
        vec!["javac", "TestJDBC.java"],
        "javac compile",
    )
    .await;

    run_cmd_checked(
        &openjdk_container,
        vec!["java", "-cp", ".:postgresql-42.7.2.jar", "TestJDBC"],
        "java run TestJDBC",
    )
    .await;
}
