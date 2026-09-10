# Getting started

## Evaluate the product

Run the deterministic demo:

```console
$ rockstream demo
RockStream Demo: scenario='orders' status=passed in <duration>ms
Mode: demo
Durability: ephemeral
Topology: simulated
Storage: <temporary storage> (retained: false)
--------------------------------------------------------------------------------
[Step 1] create_table_orders (<duration>ms) [ok]
  SQL: CREATE TABLE orders (order_id BIGINT, store_id BIGINT, amount BIGINT);
  Command Tag: rows=0
[Step 2] create_mv_sales_by_store (<duration>ms) [ok]
  SQL: CREATE MATERIALIZED VIEW sales_by_store AS SELECT store_id, SUM(amount) AS total_amount FROM orders GROUP BY store_id;
  Command Tag: rows=0
[Step 3] insert_initial_orders (<duration>ms) [ok]
  SQL: INSERT INTO orders VALUES (1, 100, 50), (2, 100, 70), (3, 200, 40);
  Command Tag: rows=3
[Step 4] query_after_insert (<duration>ms) [ok]
  SQL: SELECT store_id, total_amount FROM sales_by_store ORDER BY store_id;
  Command Tag: rows=2
  Result Rows (2):
    100	120
    200	40
[Step 5] update_order (<duration>ms) [ok]
  SQL: UPDATE orders SET amount = 100 WHERE order_id = 1, store_id = 100, amount = 50;
  Command Tag: rows=1
[Step 6] query_after_update (<duration>ms) [ok]
  SQL: SELECT store_id, total_amount FROM sales_by_store ORDER BY store_id;
  Command Tag: rows=2
  Result Rows (2):
    100	170
    200	40
[Step 7] delete_order (<duration>ms) [ok]
  SQL: DELETE FROM orders WHERE order_id = 3, store_id = 200, amount = 40;
  Command Tag: rows=1
[Step 8] query_after_delete (<duration>ms) [ok]
  SQL: SELECT store_id, total_amount FROM sales_by_store ORDER BY store_id;
  Command Tag: rows=1
  Result Rows (1):
    100	170
```

The demo is temporary. It is the quickest way to inspect the product without
creating a project.

The JSON form is stable apart from duration and temporary-storage fields:

```console
$ rockstream demo --output json
{
  "mode": "demo",
  "durability": "ephemeral",
  "topology": "simulated",
  "scenario": "orders",
  "status": "passed",
  "steps": [
    {
      "step": 1,
      "name": "create_table_orders",
      "sql": "CREATE TABLE orders (order_id BIGINT, store_id BIGINT, amount BIGINT);",
      "status": "ok",
      "command_tag": "rows=0",
      "duration_ms": 0
    },
    {
      "step": 2,
      "name": "create_mv_sales_by_store",
      "sql": "CREATE MATERIALIZED VIEW sales_by_store AS SELECT store_id, SUM(amount) AS total_amount FROM orders GROUP BY store_id;",
      "status": "ok",
      "command_tag": "rows=0",
      "duration_ms": 0
    },
    {
      "step": 3,
      "name": "insert_initial_orders",
      "sql": "INSERT INTO orders VALUES (1, 100, 50), (2, 100, 70), (3, 200, 40);",
      "status": "ok",
      "command_tag": "rows=3",
      "duration_ms": 0
    },
    {
      "step": 4,
      "name": "query_after_insert",
      "sql": "SELECT store_id, total_amount FROM sales_by_store ORDER BY store_id;",
      "status": "ok",
      "command_tag": "rows=2",
      "rows": [
        [
          "100",
          "120"
        ],
        [
          "200",
          "40"
        ]
      ],
      "duration_ms": 0
    },
    {
      "step": 5,
      "name": "update_order",
      "sql": "UPDATE orders SET amount = 100 WHERE order_id = 1, store_id = 100, amount = 50;",
      "status": "ok",
      "command_tag": "rows=1",
      "duration_ms": 0
    },
    {
      "step": 6,
      "name": "query_after_update",
      "sql": "SELECT store_id, total_amount FROM sales_by_store ORDER BY store_id;",
      "status": "ok",
      "command_tag": "rows=2",
      "rows": [
        [
          "100",
          "170"
        ],
        [
          "200",
          "40"
        ]
      ],
      "duration_ms": 0
    },
    {
      "step": 7,
      "name": "delete_order",
      "sql": "DELETE FROM orders WHERE order_id = 3, store_id = 200, amount = 40;",
      "status": "ok",
      "command_tag": "rows=1",
      "duration_ms": 0
    },
    {
      "step": 8,
      "name": "query_after_delete",
      "sql": "SELECT store_id, total_amount FROM sales_by_store ORDER BY store_id;",
      "status": "ok",
      "command_tag": "rows=1",
      "rows": [
        [
          "100",
          "170"
        ]
      ],
      "duration_ms": 0
    }
  ],
  "total_duration_ms": 0,
  "storage_path": "<temporary storage>",
  "retained": false
}
```

## Create a local project

Create a persistent local scaffold:

```console
$ rockstream init my-project --template local
RockStream Project Initialized: name='my-project' template='local'
Target Directory: my-project
Status: created
Generated Files:
  - rockstream.toml
  - project.toml
  - schema.sql
  - data/seed.csv
  - queries/verify.sql
  - README.md

Next steps:
  1. cd my-project
  2. rockstream start --storage ./storage --listen 127.0.0.1:5432
  3. rockstream project apply
  4. rockstream project verify
```

```console
$ cd my-project
```

The generated local project has this layout:

```text
my-project/
├── README.md
├── data/seed.csv
├── project.toml
├── queries/verify.sql
├── rockstream.toml
└── schema.sql
```

Apply the declarative schema and seed data over pgwire:

```console
$ rockstream project apply
Project 'my-project' applied successfully:
  - schema 'schema.sql' (applied)
  - seed table 'orders' from 'data/seed.csv' (ingested)

Next step:
  rockstream project verify
```

Verify maintained views without external psql:

```console
$ rockstream project verify
PASSED verification 'sales_by_store' (2 rows)
```

Read the generated `README.md`, then use its verification command before
connecting a client. The supported template in v0.61 is `local`. Kafka and
PostgreSQL CDC templates are available under `examples/experimental/`.

For the full command surface, see the [CLI reference](reference/cli.md).

