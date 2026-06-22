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

// ─── Delta Generator (50% Churn — retraction-stress variant) ─────────────────

/// Generate 50% churn deltas for `orders` and `lineitem` — for retraction-heavy
/// stress testing of the retraction path through join and aggregate operators.
///
/// Per epoch:
/// - `orders`:   7,500  retractions (50% of 15,000) + 7,500  new insertions
/// - `lineitem`: 30,000 retractions (50% of 60,000) + 30,000 new insertions
/// - All other tables: empty delta (no change)
///
/// New rows use unique high-offset keys (keyed off `seed`) so they never
/// accidentally collide with previously inserted rows.
pub fn generate_tpch_heavy_deltas(
    current_dataset: &HashMap<String, ArrowZSet>,
    seed: u64,
) -> HashMap<String, ArrowZSet> {
    let mut rng = SimpleRng::new(seed);
    let mut deltas = HashMap::new();

    // Static dimension tables — no changes.
    for name in &["region", "nation", "supplier", "part", "partsupp", "customer"] {
        let schema = match *name {
            "region" => region_schema(),
            "nation" => nation_schema(),
            "supplier" => supplier_schema(),
            "part" => part_schema(),
            "partsupp" => partsupp_schema(),
            "customer" => customer_schema(),
            _ => unreachable!(),
        };
        deltas.insert(name.to_string(), ArrowZSet::empty(schema));
    }

    // orders: 50% churn — 7,500 retractions + 7,500 insertions.
    let o_ret = select_retractions(current_dataset, "orders", 7_500, &mut rng);
    let base_o_key = 200_000i64 + (seed as i64 % 1_000) * 10_000;
    let mut o_orderkey = Vec::new();
    let mut o_custkey = Vec::new();
    let mut o_orderdate = Vec::new();
    let mut o_totalprice = Vec::new();
    let mut o_shippriority = Vec::new();
    for i in 0..7_500i64 {
        o_orderkey.push(base_o_key + i);
        o_custkey.push(rng.next_range(1, 1500));
        o_orderdate.push(rng.next_range(1, 2500));
        o_totalprice.push(rng.next_range(900, 500_000));
        o_shippriority.push(rng.next_range(1, 3));
    }
    let o_ins = make_zset(
        orders_schema(),
        vec![o_orderkey, o_custkey, o_orderdate, o_totalprice, o_shippriority],
        1,
    );
    deltas.insert("orders".to_string(), concat_zsets(&o_ret, &o_ins));

    // lineitem: 50% churn — 30,000 retractions + 30,000 insertions.
    // New rows reference l_orderkey values from 1-15000 so they still join
    // with the surviving orders rows (matching the TPC-H generator invariant).
    let l_ret = select_retractions(current_dataset, "lineitem", 30_000, &mut rng);
    let base_l_key = 500_000i64 + (seed as i64 % 1_000) * 100_000;
    let mut l_orderkey_v = Vec::new();
    let mut l_partkey_v = Vec::new();
    let mut l_suppkey_v = Vec::new();
    let mut l_extprice_v = Vec::new();
    let mut l_discount_v = Vec::new();
    let mut l_quantity_v = Vec::new();
    let mut l_returnflag_v = Vec::new();
    let mut l_linestatus_v = Vec::new();
    let mut l_shipdate_v = Vec::new();
    let mut l_commitdate_v = Vec::new();
    let mut l_receiptdate_v = Vec::new();
    for i in 0..30_000i64 {
        l_orderkey_v.push(base_l_key + i);
        l_partkey_v.push(rng.next_range(1, 2000));
        l_suppkey_v.push(rng.next_range(1, 100));
        l_extprice_v.push(rng.next_range(900, 100_000));
        l_discount_v.push(rng.next_range(0, 10));
        l_quantity_v.push(rng.next_range(1, 50));
        l_returnflag_v.push(rng.next_range(0, 1));
        l_linestatus_v.push(rng.next_range(0, 1));
        l_shipdate_v.push(rng.next_range(1, 2500));
        l_commitdate_v.push(rng.next_range(1, 2500));
        l_receiptdate_v.push(rng.next_range(1, 2500));
    }
    let l_ins = make_zset(
        lineitem_schema(),
        vec![
            l_orderkey_v, l_partkey_v, l_suppkey_v,
            l_extprice_v, l_discount_v, l_quantity_v,
            l_returnflag_v, l_linestatus_v,
            l_shipdate_v, l_commitdate_v, l_receiptdate_v,
        ],
        1,
    );
    deltas.insert("lineitem".to_string(), concat_zsets(&l_ret, &l_ins));

    deltas
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use rockstream_ops::aggregate::AggregateOp;
    use rockstream_ops::op::Operator;
    use rockstream_ops::zset::ArrowZSet;
    use rockstream_types::ids::OperatorId;

    use super::*;

    // ─── Helpers ─────────────────────────────────────────────────────────────

    /// Project lineitem Z-set to (k = l_returnflag, v = l_extendedprice).
    ///
    /// Column indices (0-based) for lineitem_schema():
    ///   0  l_orderkey
    ///   1  l_partkey
    ///   2  l_suppkey
    ///   3  l_extendedprice
    ///   4  l_discount
    ///   5  l_quantity
    ///   6  l_returnflag
    ///   7  l_linestatus
    ///   8  l_shipdate
    ///   9  l_commitdate
    ///  10  l_receiptdate
    fn project_lineitem_kv(lineitem: &ArrowZSet) -> ArrowZSet {
        if lineitem.is_empty() {
            let kv_schema = Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("v", DataType::Int64, false),
            ]));
            let empty = RecordBatch::new_empty(kv_schema);
            return ArrowZSet::new(empty, vec![]);
        }
        let returnflag = lineitem
            .data
            .column(6)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        let extprice = lineitem
            .data
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        let kv_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let data = RecordBatch::try_new(
            kv_schema,
            vec![
                Arc::new(Int64Array::from(returnflag)) as _,
                Arc::new(Int64Array::from(extprice)) as _,
            ],
        )
        .unwrap();
        ArrowZSet::new(data, lineitem.weights.clone())
    }

    /// Apply a Z-set delta to a base dataset (physical append + retract).
    ///
    /// Returns the new physical dataset as a positive-weight-only Z-set,
    /// i.e. `accumulated_state` after applying `delta`.
    fn apply_delta_to_dataset(base: &ArrowZSet, delta: &ArrowZSet) -> ArrowZSet {
        // Build a weight map: row → net weight.
        let mut row_weights: BTreeMap<Vec<i64>, i64> = BTreeMap::new();

        let add_rows = |zset: &ArrowZSet, map: &mut BTreeMap<Vec<i64>, i64>| {
            if zset.is_empty() {
                return;
            }
            let ncols = zset.data.num_columns();
            for i in 0..zset.num_rows() {
                let row: Vec<i64> = (0..ncols)
                    .map(|c| {
                        zset.data
                            .column(c)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(i)
                    })
                    .collect();
                *map.entry(row).or_insert(0) += zset.weights[i];
            }
        };

        add_rows(base, &mut row_weights);
        add_rows(delta, &mut row_weights);

        // Collect rows with positive net weight.
        let live_rows: Vec<Vec<i64>> = row_weights
            .into_iter()
            .filter(|(_, w)| *w > 0)
            .map(|(r, _)| r)
            .collect();

        if live_rows.is_empty() {
            return ArrowZSet::new(RecordBatch::new_empty(base.data.schema()), vec![]);
        }

        let ncols = base.data.num_columns();
        let mut cols: Vec<Vec<i64>> = vec![Vec::new(); ncols];
        for row in &live_rows {
            for (c, &v) in row.iter().enumerate() {
                cols[c].push(v);
            }
        }
        let arrow_cols: Vec<_> = cols
            .into_iter()
            .map(|c| Arc::new(Int64Array::from(c)) as _)
            .collect();
        let data = RecordBatch::try_new(base.data.schema(), arrow_cols).unwrap();
        let n = data.num_rows();
        ArrowZSet::new(data, vec![1; n])
    }

    /// Batch reference: compute GROUP BY k → (sum_v, count) from a physical
    /// (positive-weight-only) kv Z-set.
    fn batch_agg_reference(kv: &ArrowZSet) -> BTreeMap<i64, (i64, i64)> {
        let mut result: BTreeMap<i64, (i64, i64)> = BTreeMap::new();
        if kv.is_empty() {
            return result;
        }
        let k_col = kv
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let v_col = kv
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..kv.num_rows() {
            let w = kv.weights[i];
            if w <= 0 {
                continue;
            }
            let entry = result.entry(k_col.value(i)).or_insert((0, 0));
            entry.0 += v_col.value(i) * w;
            entry.1 += w;
        }
        result
    }

    /// Apply AggregateOp output Z-set deltas to an in-memory state map.
    fn apply_agg_output_deltas(
        state: &mut BTreeMap<i64, (i64, i64)>,
        output: &ArrowZSet,
    ) {
        if output.is_empty() {
            return;
        }
        let k_col = output
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let s_col = output
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let c_col = output
            .data
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..output.num_rows() {
            let k = k_col.value(i);
            let s = s_col.value(i);
            let c = c_col.value(i);
            let w = output.weights[i];
            if w > 0 {
                state.insert(k, (s, c));
            } else {
                state.remove(&k);
            }
        }
    }

    // ─── Tests ───────────────────────────────────────────────────────────────

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

    /// TPC-H Q1-style incremental oracle (SF 0.01).
    ///
    /// Query: `SELECT l_returnflag, SUM(l_extendedprice), COUNT(*) FROM lineitem GROUP BY l_returnflag`
    ///
    /// Oracle property: after applying 1% deltas, the incremental aggregate
    /// output accumulated via `AggregateOp` must equal the batch reference
    /// computed from the physically updated dataset.
    ///
    /// This test verifies:
    /// 1. Initial load (60 000 rows) is correctly aggregated incrementally.
    /// 2. A 1% delta (600 retractions + 600 insertions) updates the aggregate correctly.
    /// 3. Final incremental state == batch-from-scratch on the updated dataset.
    #[test]
    fn tpch_q1_style_incremental_oracle() {
        let seed = 42u64;
        let dataset = generate_tpch_dataset(seed);
        let lineitem = dataset.get("lineitem").unwrap().clone();

        // ── Phase 1: initial incremental load ──────────────────────────────
        let op = AggregateOp::new(OperatorId(99));
        let kv_initial = project_lineitem_kv(&lineitem);
        let out_initial = op.process_delta(kv_initial).expect("initial pass");

        let mut incr_state: BTreeMap<i64, (i64, i64)> = BTreeMap::new();
        apply_agg_output_deltas(&mut incr_state, &out_initial);

        // Verify initial state against batch reference.
        let batch_initial = batch_agg_reference(&project_lineitem_kv(&lineitem));
        assert_eq!(
            incr_state, batch_initial,
            "Initial TPC-H Q1 aggregate: incremental != batch\n\
             incremental: {incr_state:?}\n\
             batch:       {batch_initial:?}"
        );

        // ── Phase 2: apply 1% delta (600 retractions + 600 insertions) ─────
        let deltas = generate_tpch_deltas(&dataset, seed + 1);
        let lineitem_delta = deltas.get("lineitem").unwrap();

        let kv_delta = project_lineitem_kv(lineitem_delta);
        let out_delta = op.process_delta(kv_delta).expect("delta pass");
        apply_agg_output_deltas(&mut incr_state, &out_delta);

        // ── Phase 3: batch reference on the updated dataset ─────────────────
        let lineitem_after = apply_delta_to_dataset(&lineitem, lineitem_delta);
        let kv_after = project_lineitem_kv(&lineitem_after);
        let batch_after = batch_agg_reference(&kv_after);

        assert_eq!(
            incr_state, batch_after,
            "TPC-H Q1 aggregate after 1% delta: incremental != batch\n\
             incremental ({} groups): {incr_state:?}\n\
             batch       ({} groups): {batch_after:?}",
            incr_state.len(),
            batch_after.len()
        );
    }

    /// TPC-H Q1-style with three consecutive delta epochs (SF 0.01).
    ///
    /// Extends the single-delta test by applying 3 successive 1% deltas
    /// and verifying the oracle property holds after every epoch.
    ///
    /// This specifically stresses cross-epoch retraction: rows inserted in
    /// epoch N become candidates for retraction in epoch N+1.
    #[test]
    fn tpch_q1_style_three_epoch_oracle() {
        let base_seed = 100u64;
        let dataset0 = generate_tpch_dataset(base_seed);
        let lineitem0 = dataset0.get("lineitem").unwrap().clone();

        let op = AggregateOp::new(OperatorId(100));
        let mut incr_state: BTreeMap<i64, (i64, i64)> = BTreeMap::new();

        // Initial load.
        let out0 = op
            .process_delta(project_lineitem_kv(&lineitem0))
            .expect("epoch 0");
        apply_agg_output_deltas(&mut incr_state, &out0);

        let mut current_lineitem = lineitem0;
        let mut current_dataset = dataset0;

        for epoch in 1u64..=3 {
            let deltas = generate_tpch_deltas(&current_dataset, base_seed + epoch * 7);
            let lineitem_delta = deltas.get("lineitem").unwrap();

            // Incremental update.
            let out = op
                .process_delta(project_lineitem_kv(lineitem_delta))
                .expect("epoch delta");
            apply_agg_output_deltas(&mut incr_state, &out);

            // Batch reference on the physically updated dataset.
            let updated = apply_delta_to_dataset(&current_lineitem, lineitem_delta);
            let batch = batch_agg_reference(&project_lineitem_kv(&updated));

            assert_eq!(
                incr_state, batch,
                "TPC-H Q1 epoch {epoch}: incremental != batch\n\
                 incremental ({} groups): {incr_state:?}\n\
                 batch       ({} groups): {batch:?}",
                incr_state.len(),
                batch.len()
            );

            // Advance dataset.
            current_lineitem = updated;
            // Rebuild a minimal dataset map for the next delta call.
            current_dataset = {
                let mut m = std::collections::HashMap::new();
                m.insert("lineitem".to_string(), current_lineitem.clone());
                // Carry over the other tables unchanged.
                for table in &[
                    "region", "nation", "supplier", "part", "partsupp",
                    "customer", "orders",
                ] {
                    m.insert(
                        table.to_string(),
                        generate_tpch_dataset(base_seed)
                            .remove(*table)
                            .unwrap(),
                    );
                }
                m
            };
        }
    }

    /// Project lineitem to (k=l_returnflag, v=l_extendedprice) only for rows
    /// matching the Q6 filter: `l_discount BETWEEN 5 AND 7 AND l_quantity < 25`.
    ///
    /// This allows counting filtered lineitems via `AggregateOp` SUM(1) = COUNT(*).
    /// Lineitem column indices: 0=l_orderkey, 3=l_extendedprice, 4=l_discount, 5=l_quantity.
    fn project_lineitem_q6(lineitem: &ArrowZSet) -> ArrowZSet {
        if lineitem.is_empty() {
            let schema = Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("v", DataType::Int64, false),
            ]));
            return ArrowZSet::new(RecordBatch::new_empty(schema), vec![]);
        }
        let discount_col = lineitem
            .data
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let quantity_col = lineitem
            .data
            .column(5)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let ep_col = lineitem
            .data
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let mut k_vals = Vec::new();
        let mut v_vals = Vec::new();
        let mut weights = Vec::new();
        for i in 0..lineitem.num_rows() {
            let discount = discount_col.value(i);
            let quantity = quantity_col.value(i);
            // Q6 filter: discount in [5, 7] and quantity < 25
            if discount >= 5 && discount <= 7 && quantity < 25 {
                k_vals.push(0i64); // single group (no GROUP BY in Q6)
                v_vals.push(ep_col.value(i));
                weights.push(lineitem.weights[i]);
            }
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        if k_vals.is_empty() {
            return ArrowZSet::new(RecordBatch::new_empty(schema), vec![]);
        }
        let data = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(k_vals)) as _,
                Arc::new(Int64Array::from(v_vals)) as _,
            ],
        )
        .unwrap();
        ArrowZSet::new(data, weights)
    }

    /// Compute the Q6 batch reference: SUM(l_extendedprice) for rows matching
    /// `l_discount BETWEEN 5 AND 7 AND l_quantity < 25` from a physical lineitem Z-set.
    fn batch_q6_sum(lineitem: &ArrowZSet) -> i64 {
        if lineitem.is_empty() {
            return 0;
        }
        let ep_col = lineitem
            .data
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let discount_col = lineitem
            .data
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let quantity_col = lineitem
            .data
            .column(5)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut total = 0i64;
        for i in 0..lineitem.num_rows() {
            let w = lineitem.weights[i];
            if w <= 0 {
                continue;
            }
            let discount = discount_col.value(i);
            let quantity = quantity_col.value(i);
            if discount >= 5 && discount <= 7 && quantity < 25 {
                total += ep_col.value(i);
            }
        }
        total
    }

    /// TPC-H Q6-style incremental oracle: SUM(l_extendedprice) for filtered lineitems.
    ///
    /// Query (simplified Q6):
    /// `SELECT SUM(l_extendedprice) FROM lineitem
    ///   WHERE l_discount BETWEEN 5 AND 7 AND l_quantity < 25`
    ///
    /// Oracle property: after initial load + 1% delta, the incremental aggregate
    /// output from `AggregateOp` must equal the batch SUM on the updated dataset.
    ///
    /// This tests incremental maintenance of a filter-aggregate combination:
    /// rows that enter or leave the filter predicate must update the aggregate
    /// correctly via retraction + insertion.
    #[test]
    fn tpch_q6_style_incremental_oracle() {
        let seed = 200u64;
        let dataset = generate_tpch_dataset(seed);
        let lineitem = dataset.get("lineitem").unwrap().clone();

        let op = AggregateOp::new(OperatorId(200));
        let mut incr_state: BTreeMap<i64, (i64, i64)> = BTreeMap::new();

        // Phase 1: initial load — project + filter, then aggregate.
        let kv_initial = project_lineitem_q6(&lineitem);
        let out_initial = op.process_delta(kv_initial).expect("Q6 initial pass");
        apply_agg_output_deltas(&mut incr_state, &out_initial);

        // Verify initial state vs batch.
        let expected_initial = batch_q6_sum(&lineitem);
        let incr_initial_sum: i64 = incr_state.values().map(|(sum, _)| *sum).sum();
        assert_eq!(
            incr_initial_sum,
            expected_initial,
            "Q6 initial load: incremental SUM != batch SUM\n\
             incremental sum: {incr_initial_sum}\n\
             batch sum:       {expected_initial}"
        );

        // Phase 2: 1% delta.
        let deltas = generate_tpch_deltas(&dataset, seed + 1);
        let lineitem_delta = deltas.get("lineitem").unwrap();

        let kv_delta = project_lineitem_q6(lineitem_delta);
        let out_delta = op.process_delta(kv_delta).expect("Q6 delta pass");
        apply_agg_output_deltas(&mut incr_state, &out_delta);

        // Phase 3: batch reference on updated dataset.
        let lineitem_after = apply_delta_to_dataset(&lineitem, lineitem_delta);
        let expected_after = batch_q6_sum(&lineitem_after);
        let incr_after_sum: i64 = incr_state.values().map(|(sum, _)| *sum).sum();

        assert_eq!(
            incr_after_sum,
            expected_after,
            "Q6 after 1% delta: incremental SUM != batch SUM\n\
             incremental sum: {incr_after_sum}\n\
             batch sum:       {expected_after}"
        );
    }

    /// TPC-H Q1-style oracle with 10 consecutive delta epochs (SF 0.01).
    ///
    /// Extends the 3-epoch test to prove the oracle property holds across 10
    /// successive 1% churn epochs — total 10% of the dataset replaced.
    ///
    /// Cross-epoch retraction: rows inserted in epoch N are candidates for
    /// deletion in any subsequent epoch, validating the long-running IVM
    /// accumulation correctness.
    #[test]
    fn tpch_q1_style_ten_epoch_oracle() {
        let base_seed = 500u64;
        let dataset0 = generate_tpch_dataset(base_seed);
        let lineitem0 = dataset0.get("lineitem").unwrap().clone();

        let op = AggregateOp::new(OperatorId(500));
        let mut incr_state: BTreeMap<i64, (i64, i64)> = BTreeMap::new();

        // Initial load.
        let out0 = op
            .process_delta(project_lineitem_kv(&lineitem0))
            .expect("epoch 0");
        apply_agg_output_deltas(&mut incr_state, &out0);

        let mut current_lineitem = lineitem0;
        let mut current_dataset = dataset0;

        for epoch in 1u64..=10 {
            let deltas = generate_tpch_deltas(&current_dataset, base_seed + epoch * 13);
            let lineitem_delta = deltas.get("lineitem").unwrap();

            let out = op
                .process_delta(project_lineitem_kv(lineitem_delta))
                .expect("epoch delta");
            apply_agg_output_deltas(&mut incr_state, &out);

            let updated = apply_delta_to_dataset(&current_lineitem, lineitem_delta);
            let batch = batch_agg_reference(&project_lineitem_kv(&updated));

            assert_eq!(
                incr_state, batch,
                "TPC-H Q1 10-epoch test, epoch {epoch}: incremental != batch\n\
                 incremental ({} groups): {incr_state:?}\n\
                 batch       ({} groups): {batch:?}",
                incr_state.len(),
                batch.len()
            );

            current_lineitem = updated;
            current_dataset = {
                let mut m = std::collections::HashMap::new();
                m.insert("lineitem".to_string(), current_lineitem.clone());
                for table in &[
                    "region", "nation", "supplier", "part", "partsupp",
                    "customer", "orders",
                ] {
                    m.insert(
                        table.to_string(),
                        generate_tpch_dataset(base_seed).remove(*table).unwrap(),
                    );
                }
                m
            };
        }
    }
}
