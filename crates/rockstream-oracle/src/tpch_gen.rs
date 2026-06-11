//! Deterministic TPC-H scale factor 0.01 data generator (v0.14).

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use rockstream_ops::zset::ArrowZSet;

// ─── LCG Generator ───────────────────────────────────────────────────────────

pub struct SimpleRng {
    seed: u64,
}

impl SimpleRng {
    pub fn new(seed: u64) -> Self {
        Self { seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.seed = self.seed.wrapping_mul(1664525).wrapping_add(1013904223);
        self.seed
    }

    pub fn next_range(&mut self, min: i64, max: i64) -> i64 {
        if min >= max {
            return min;
        }
        let diff = (max - min + 1) as u64;
        min + (self.next_u64() % diff) as i64
    }
}

// ─── TPC-H Schemas ───────────────────────────────────────────────────────────

pub fn region_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "r_regionkey",
        DataType::Int64,
        false,
    )]))
}

pub fn nation_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("n_nationkey", DataType::Int64, false),
        Field::new("n_regionkey", DataType::Int64, false),
    ]))
}

pub fn supplier_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("s_suppkey", DataType::Int64, false),
        Field::new("s_nationkey", DataType::Int64, false),
        Field::new("s_acctbal", DataType::Int64, false),
    ]))
}

pub fn part_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("p_partkey", DataType::Int64, false),
        Field::new("p_size", DataType::Int64, false),
        Field::new("p_retailprice", DataType::Int64, false),
        Field::new("p_brand", DataType::Int64, false),
        Field::new("p_type", DataType::Int64, false),
        Field::new("p_container", DataType::Int64, false),
    ]))
}

pub fn partsupp_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ps_partkey", DataType::Int64, false),
        Field::new("ps_suppkey", DataType::Int64, false),
        Field::new("ps_availqty", DataType::Int64, false),
        Field::new("ps_supplycost", DataType::Int64, false),
    ]))
}

pub fn customer_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_nationkey", DataType::Int64, false),
        Field::new("c_acctbal", DataType::Int64, false),
        Field::new("c_mktsegment", DataType::Int64, false),
    ]))
}

pub fn orders_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_orderdate", DataType::Int64, false),
        Field::new("o_totalprice", DataType::Int64, false),
        Field::new("o_shippriority", DataType::Int64, false),
    ]))
}

pub fn lineitem_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_partkey", DataType::Int64, false),
        Field::new("l_suppkey", DataType::Int64, false),
        Field::new("l_extendedprice", DataType::Int64, false),
        Field::new("l_discount", DataType::Int64, false),
        Field::new("l_quantity", DataType::Int64, false),
        Field::new("l_returnflag", DataType::Int64, false),
        Field::new("l_linestatus", DataType::Int64, false),
        Field::new("l_shipdate", DataType::Int64, false),
        Field::new("l_commitdate", DataType::Int64, false),
        Field::new("l_receiptdate", DataType::Int64, false),
    ]))
}

// ─── Generator Helper ────────────────────────────────────────────────────────

fn make_zset(schema: SchemaRef, columns: Vec<Vec<i64>>, weight: i64) -> ArrowZSet {
    let num_rows = columns[0].len();
    let arrow_cols: Vec<ArrayRef> = columns
        .into_iter()
        .map(|col| Arc::new(Int64Array::from(col)) as ArrayRef)
        .collect();
    let data = RecordBatch::try_new(schema, arrow_cols).unwrap();
    let weights = vec![weight; num_rows];
    ArrowZSet::new(data, weights)
}

// ─── Main Dataset Generator ──────────────────────────────────────────────────

pub fn generate_tpch_dataset(seed: u64) -> HashMap<String, ArrowZSet> {
    let mut rng = SimpleRng::new(seed);
    let mut tables = HashMap::new();

    // 1. region: 5 rows
    let mut r_regionkey = Vec::new();
    for i in 1..=5 {
        r_regionkey.push(i);
    }
    tables.insert(
        "region".to_string(),
        make_zset(region_schema(), vec![r_regionkey], 1),
    );

    // 2. nation: 25 rows
    let mut n_nationkey = Vec::new();
    let mut n_regionkey = Vec::new();
    for i in 1..=25 {
        n_nationkey.push(i);
        n_regionkey.push(rng.next_range(1, 5));
    }
    tables.insert(
        "nation".to_string(),
        make_zset(nation_schema(), vec![n_nationkey, n_regionkey], 1),
    );

    // 3. supplier: 100 rows
    let mut s_suppkey = Vec::new();
    let mut s_nationkey = Vec::new();
    let mut s_acctbal = Vec::new();
    for i in 1..=100 {
        s_suppkey.push(i);
        s_nationkey.push(rng.next_range(1, 25));
        s_acctbal.push(rng.next_range(-1000, 10000));
    }
    tables.insert(
        "supplier".to_string(),
        make_zset(
            supplier_schema(),
            vec![s_suppkey, s_nationkey, s_acctbal],
            1,
        ),
    );

    // 4. part: 2,000 rows
    let mut p_partkey = Vec::new();
    let mut p_size = Vec::new();
    let mut p_retailprice = Vec::new();
    let mut p_brand = Vec::new();
    let mut p_type = Vec::new();
    let mut p_container = Vec::new();
    for i in 1..=2000 {
        p_partkey.push(i);
        p_size.push(rng.next_range(1, 50));
        p_retailprice.push(rng.next_range(900, 200000));
        p_brand.push(rng.next_range(1, 5));
        p_type.push(rng.next_range(1, 15));
        p_container.push(rng.next_range(1, 10));
    }
    tables.insert(
        "part".to_string(),
        make_zset(
            part_schema(),
            vec![
                p_partkey,
                p_size,
                p_retailprice,
                p_brand,
                p_type,
                p_container,
            ],
            1,
        ),
    );

    // 5. partsupp: 8,000 rows
    let mut ps_partkey = Vec::new();
    let mut ps_suppkey = Vec::new();
    let mut ps_availqty = Vec::new();
    let mut ps_supplycost = Vec::new();
    for i in 1..=2000 {
        ps_partkey.push(i);
        ps_suppkey.push(rng.next_range(1, 100));
        ps_availqty.push(rng.next_range(1, 9999));
        ps_supplycost.push(rng.next_range(1, 1000));
    }
    for _ in 2000..8000 {
        ps_partkey.push(rng.next_range(1, 2000));
        ps_suppkey.push(rng.next_range(1, 100));
        ps_availqty.push(rng.next_range(1, 9999));
        ps_supplycost.push(rng.next_range(1, 1000));
    }
    tables.insert(
        "partsupp".to_string(),
        make_zset(
            partsupp_schema(),
            vec![ps_partkey, ps_suppkey, ps_availqty, ps_supplycost],
            1,
        ),
    );

    // 6. customer: 1,500 rows
    let mut c_custkey = Vec::new();
    let mut c_nationkey = Vec::new();
    let mut c_acctbal = Vec::new();
    let mut c_mktsegment = Vec::new();
    for i in 1..=1500 {
        c_custkey.push(i);
        c_nationkey.push(rng.next_range(1, 25));
        c_acctbal.push(rng.next_range(-1000, 10000));
        c_mktsegment.push(rng.next_range(1, 5));
    }
    tables.insert(
        "customer".to_string(),
        make_zset(
            customer_schema(),
            vec![c_custkey, c_nationkey, c_acctbal, c_mktsegment],
            1,
        ),
    );

    // 7. orders: 15,000 rows
    let mut o_orderkey = Vec::new();
    let mut o_custkey = Vec::new();
    let mut o_orderdate = Vec::new();
    let mut o_totalprice = Vec::new();
    let mut o_shippriority = Vec::new();
    for i in 1..=15000 {
        o_orderkey.push(i);
        o_custkey.push(rng.next_range(1, 1500));
        o_orderdate.push(rng.next_range(1, 2500));
        o_totalprice.push(rng.next_range(900, 500000));
        o_shippriority.push(rng.next_range(1, 3));
    }
    tables.insert(
        "orders".to_string(),
        make_zset(
            orders_schema(),
            vec![
                o_orderkey,
                o_custkey,
                o_orderdate,
                o_totalprice,
                o_shippriority,
            ],
            1,
        ),
    );

    // 8. lineitem: exactly 60,000 rows
    let mut l_orderkey = Vec::new();
    let mut l_partkey = Vec::new();
    let mut l_suppkey = Vec::new();
    let mut l_extendedprice = Vec::new();
    let mut l_discount = Vec::new();
    let mut l_quantity = Vec::new();
    let mut l_returnflag = Vec::new();
    let mut l_linestatus = Vec::new();
    let mut l_shipdate = Vec::new();
    let mut l_commitdate = Vec::new();
    let mut l_receiptdate = Vec::new();
    for i in 1..=15000 {
        l_orderkey.push(i);
        l_partkey.push(rng.next_range(1, 2000));
        l_suppkey.push(rng.next_range(1, 100));
        l_extendedprice.push(rng.next_range(900, 100000));
        l_discount.push(rng.next_range(0, 10));
        l_quantity.push(rng.next_range(1, 50));
        l_returnflag.push(rng.next_range(0, 1));
        l_linestatus.push(rng.next_range(0, 1));
        l_shipdate.push(rng.next_range(1, 2500));
        l_commitdate.push(rng.next_range(1, 2500));
        l_receiptdate.push(rng.next_range(1, 2500));
    }
    for _ in 15000..60000 {
        l_orderkey.push(rng.next_range(1, 15000));
        l_partkey.push(rng.next_range(1, 2000));
        l_suppkey.push(rng.next_range(1, 100));
        l_extendedprice.push(rng.next_range(900, 100000));
        l_discount.push(rng.next_range(0, 10));
        l_quantity.push(rng.next_range(1, 50));
        l_returnflag.push(rng.next_range(0, 1));
        l_linestatus.push(rng.next_range(0, 1));
        l_shipdate.push(rng.next_range(1, 2500));
        l_commitdate.push(rng.next_range(1, 2500));
        l_receiptdate.push(rng.next_range(1, 2500));
    }
    tables.insert(
        "lineitem".to_string(),
        make_zset(
            lineitem_schema(),
            vec![
                l_orderkey,
                l_partkey,
                l_suppkey,
                l_extendedprice,
                l_discount,
                l_quantity,
                l_returnflag,
                l_linestatus,
                l_shipdate,
                l_commitdate,
                l_receiptdate,
            ],
            1,
        ),
    );

    tables
}

// ─── Delta Generator (1% Churn) ──────────────────────────────────────────────

fn concat_zsets(a: &ArrowZSet, b: &ArrowZSet) -> ArrowZSet {
    if a.is_empty() {
        return b.clone();
    }
    if b.is_empty() {
        return a.clone();
    }
    let mut columns = Vec::new();
    for i in 0..a.data.num_columns() {
        let col_a = a.data.column(i);
        let col_b = b.data.column(i);
        let concatenated = arrow::compute::concat(&[col_a.as_ref(), col_b.as_ref()]).unwrap();
        columns.push(concatenated);
    }
    let data = RecordBatch::try_new(a.schema(), columns).unwrap();
    let mut weights = a.weights.clone();
    weights.extend_from_slice(&b.weights);
    ArrowZSet::new(data, weights)
}

fn select_retractions(
    current_dataset: &HashMap<String, ArrowZSet>,
    table_name: &str,
    count: usize,
    rng: &mut SimpleRng,
) -> ArrowZSet {
    let current = current_dataset.get(table_name).unwrap();
    let num_rows = current.num_rows();
    let mut indices = Vec::new();
    for _ in 0..count {
        indices.push(rng.next_range(0, (num_rows - 1) as i64) as usize);
    }
    let mut ret = current.select_rows(&indices).unwrap();
    ret.weights = vec![-1; ret.weights.len()];
    ret
}

pub fn generate_tpch_deltas(
    current_dataset: &HashMap<String, ArrowZSet>,
    seed: u64,
) -> HashMap<String, ArrowZSet> {
    let mut rng = SimpleRng::new(seed);
    let mut deltas = HashMap::new();

    // 1. region: static (0 changes)
    deltas.insert("region".to_string(), ArrowZSet::empty(region_schema()));

    // 2. nation: static (0 changes)
    deltas.insert("nation".to_string(), ArrowZSet::empty(nation_schema()));

    // 3. supplier: 1% of 100 = 1 retraction, 1 insertion
    let s_ret = select_retractions(current_dataset, "supplier", 1, &mut rng);
    let s_suppkey = vec![rng.next_range(101, 200)];
    let s_nationkey = vec![rng.next_range(1, 25)];
    let s_acctbal = vec![rng.next_range(-1000, 10000)];
    let s_ins = make_zset(
        supplier_schema(),
        vec![s_suppkey, s_nationkey, s_acctbal],
        1,
    );
    deltas.insert("supplier".to_string(), concat_zsets(&s_ret, &s_ins));

    // 4. part: 1% of 2000 = 20 retractions, 20 insertions
    let p_ret = select_retractions(current_dataset, "part", 20, &mut rng);
    let mut p_partkey = Vec::new();
    let mut p_size = Vec::new();
    let mut p_retailprice = Vec::new();
    let mut p_brand = Vec::new();
    let mut p_type = Vec::new();
    let mut p_container = Vec::new();
    for i in 1..=20 {
        p_partkey.push(2000 + i);
        p_size.push(rng.next_range(1, 50));
        p_retailprice.push(rng.next_range(900, 200000));
        p_brand.push(rng.next_range(1, 5));
        p_type.push(rng.next_range(1, 15));
        p_container.push(rng.next_range(1, 10));
    }
    let p_ins = make_zset(
        part_schema(),
        vec![
            p_partkey,
            p_size,
            p_retailprice,
            p_brand,
            p_type,
            p_container,
        ],
        1,
    );
    deltas.insert("part".to_string(), concat_zsets(&p_ret, &p_ins));

    // 5. partsupp: 1% of 8000 = 80 retractions, 80 insertions
    let ps_ret = select_retractions(current_dataset, "partsupp", 80, &mut rng);
    let mut ps_partkey = Vec::new();
    let mut ps_suppkey = Vec::new();
    let mut ps_availqty = Vec::new();
    let mut ps_supplycost = Vec::new();
    for _ in 1..=80 {
        ps_partkey.push(rng.next_range(1, 2000));
        ps_suppkey.push(rng.next_range(1, 100));
        ps_availqty.push(rng.next_range(1, 9999));
        ps_supplycost.push(rng.next_range(1, 1000));
    }
    let ps_ins = make_zset(
        partsupp_schema(),
        vec![ps_partkey, ps_suppkey, ps_availqty, ps_supplycost],
        1,
    );
    deltas.insert("partsupp".to_string(), concat_zsets(&ps_ret, &ps_ins));

    // 6. customer: 1% of 1500 = 15 retractions, 15 insertions
    let c_ret = select_retractions(current_dataset, "customer", 15, &mut rng);
    let mut c_custkey = Vec::new();
    let mut c_nationkey = Vec::new();
    let mut c_acctbal = Vec::new();
    let mut c_mktsegment = Vec::new();
    for i in 1..=15 {
        c_custkey.push(1500 + i);
        c_nationkey.push(rng.next_range(1, 25));
        c_acctbal.push(rng.next_range(-1000, 10000));
        c_mktsegment.push(rng.next_range(1, 5));
    }
    let c_ins = make_zset(
        customer_schema(),
        vec![c_custkey, c_nationkey, c_acctbal, c_mktsegment],
        1,
    );
    deltas.insert("customer".to_string(), concat_zsets(&c_ret, &c_ins));

    // 7. orders: 1% of 15000 = 150 retractions, 150 insertions
    let o_ret = select_retractions(current_dataset, "orders", 150, &mut rng);
    let mut o_orderkey = Vec::new();
    let mut o_custkey = Vec::new();
    let mut o_orderdate = Vec::new();
    let mut o_totalprice = Vec::new();
    let mut o_shippriority = Vec::new();
    for i in 1..=150 {
        o_orderkey.push(15000 + i);
        o_custkey.push(rng.next_range(1, 1500));
        o_orderdate.push(rng.next_range(1, 2500));
        o_totalprice.push(rng.next_range(900, 500000));
        o_shippriority.push(rng.next_range(1, 3));
    }
    let o_ins = make_zset(
        orders_schema(),
        vec![
            o_orderkey,
            o_custkey,
            o_orderdate,
            o_totalprice,
            o_shippriority,
        ],
        1,
    );
    deltas.insert("orders".to_string(), concat_zsets(&o_ret, &o_ins));

    // 8. lineitem: 1% of 60000 = 600 retractions, 600 insertions
    let l_ret = select_retractions(current_dataset, "lineitem", 600, &mut rng);
    let mut l_orderkey = Vec::new();
    let mut l_partkey = Vec::new();
    let mut l_suppkey = Vec::new();
    let mut l_extendedprice = Vec::new();
    let mut l_discount = Vec::new();
    let mut l_quantity = Vec::new();
    let mut l_returnflag = Vec::new();
    let mut l_linestatus = Vec::new();
    let mut l_shipdate = Vec::new();
    let mut l_commitdate = Vec::new();
    let mut l_receiptdate = Vec::new();
    for _ in 1..=600 {
        l_orderkey.push(rng.next_range(1, 15000));
        l_partkey.push(rng.next_range(1, 2000));
        l_suppkey.push(rng.next_range(1, 100));
        l_extendedprice.push(rng.next_range(900, 100000));
        l_discount.push(rng.next_range(0, 10));
        l_quantity.push(rng.next_range(1, 50));
        l_returnflag.push(rng.next_range(0, 1));
        l_linestatus.push(rng.next_range(0, 1));
        l_shipdate.push(rng.next_range(1, 2500));
        l_commitdate.push(rng.next_range(1, 2500));
        l_receiptdate.push(rng.next_range(1, 2500));
    }
    let l_ins = make_zset(
        lineitem_schema(),
        vec![
            l_orderkey,
            l_partkey,
            l_suppkey,
            l_extendedprice,
            l_discount,
            l_quantity,
            l_returnflag,
            l_linestatus,
            l_shipdate,
            l_commitdate,
            l_receiptdate,
        ],
        1,
    );
    deltas.insert("lineitem".to_string(), concat_zsets(&l_ret, &l_ins));

    deltas
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generator_counts() {
        let dataset = generate_tpch_dataset(42);
        assert_eq!(dataset.get("region").unwrap().num_rows(), 5);
        assert_eq!(dataset.get("nation").unwrap().num_rows(), 25);
        assert_eq!(dataset.get("supplier").unwrap().num_rows(), 100);
        assert_eq!(dataset.get("part").unwrap().num_rows(), 2000);
        assert_eq!(dataset.get("partsupp").unwrap().num_rows(), 8000);
        assert_eq!(dataset.get("customer").unwrap().num_rows(), 1500);
        assert_eq!(dataset.get("orders").unwrap().num_rows(), 15000);
        assert_eq!(dataset.get("lineitem").unwrap().num_rows(), 60000);
    }
}
