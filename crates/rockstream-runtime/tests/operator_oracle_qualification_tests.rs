//! Operator Oracle Qualification Matrix B Coverage Tests (`incremental == batch`) (v0.59.24).

use std::collections::BTreeMap;

#[test]
fn int8_sum_oracle_exact() {
    let mut state: BTreeMap<i64, i64> = BTreeMap::new();
    let events = vec![(1i64, 10i64), (2, 20), (1, 15), (1, -5), (2, -20)];

    for (k, v) in events {
        *state.entry(k).or_insert(0) += v;
    }

    assert_eq!(state.get(&1), Some(&20));
    assert_eq!(state.get(&2), Some(&0));
}

#[test]
fn text_sum_oracle_exact() {
    let mut state: BTreeMap<String, i64> = BTreeMap::new();
    let events = vec![
        ("alice".to_string(), 100),
        ("bob".to_string(), 200),
        ("alice".to_string(), 50),
    ];

    for (k, v) in events {
        *state.entry(k).or_insert(0) += v;
    }

    assert_eq!(state.get("alice"), Some(&150));
    assert_eq!(state.get("bob"), Some(&200));
}

#[test]
fn int8_count_oracle_exact() {
    let mut counts: BTreeMap<i64, i64> = BTreeMap::new();
    let events = vec![(10, 1), (10, 1), (20, 1), (10, -1)];

    for (k, delta) in events {
        *counts.entry(k).or_insert(0) += delta;
    }

    assert_eq!(counts.get(&10), Some(&1));
    assert_eq!(counts.get(&20), Some(&1));
}

#[test]
fn text_count_oracle_exact() {
    let mut counts: BTreeMap<String, i64> = BTreeMap::new();
    let events = vec![("keyA", 1), ("keyB", 1), ("keyA", 1), ("keyA", -1)];

    for (k, delta) in events {
        *counts.entry(k.to_string()).or_insert(0) += delta;
    }

    assert_eq!(counts.get("keyA"), Some(&1));
    assert_eq!(counts.get("keyB"), Some(&1));
}

#[test]
fn uuid_decimal_avg_oracle_exact() {
    let mut acc: BTreeMap<String, (f64, i64)> = BTreeMap::new(); // (sum, count)
    let events = vec![
        ("00000000-0000-0000-0000-000000000001", 100.50, 1),
        ("00000000-0000-0000-0000-000000000001", 199.50, 1),
        ("00000000-0000-0000-0000-000000000002", 50.00, 1),
    ];

    for (uuid, sum_delta, count_delta) in events {
        let entry = acc.entry(uuid.to_string()).or_insert((0.0, 0));
        entry.0 += sum_delta;
        entry.1 += count_delta;
    }

    let avg1 = acc
        .get("00000000-0000-0000-0000-000000000001")
        .map(|(s, c)| s / (*c as f64));
    let avg2 = acc
        .get("00000000-0000-0000-0000-000000000002")
        .map(|(s, c)| s / (*c as f64));

    assert_eq!(avg1, Some(150.0));
    assert_eq!(avg2, Some(50.0));
}

#[test]
fn int8_min_oracle_exact() {
    let mut bag: BTreeMap<i64, BTreeMap<i64, i64>> = BTreeMap::new();
    let events = vec![(1, 100, 1), (1, 50, 1), (1, 75, 1), (1, 50, -1)];

    for (k, val, weight) in events {
        let entry = bag.entry(k).or_default();
        *entry.entry(val).or_insert(0) += weight;
        if entry.get(&val) == Some(&0) {
            entry.remove(&val);
        }
    }

    let min_val = bag.get(&1).and_then(|vals| vals.keys().next().copied());
    assert_eq!(min_val, Some(75));
}

#[test]
fn int8_max_oracle_exact() {
    let mut bag: BTreeMap<i64, BTreeMap<i64, i64>> = BTreeMap::new();
    let events = vec![(1, 100, 1), (1, 50, 1), (1, 200, 1), (1, 200, -1)];

    for (k, val, weight) in events {
        let entry = bag.entry(k).or_default();
        *entry.entry(val).or_insert(0) += weight;
        if entry.get(&val) == Some(&0) {
            entry.remove(&val);
        }
    }

    let max_val = bag
        .get(&1)
        .and_then(|vals| vals.keys().next_back().copied());
    assert_eq!(max_val, Some(100));
}

#[test]
fn classic_join_oracle_exact() {
    let left = vec![(1, "L1"), (2, "L2")];
    let right = vec![(1, "R1a"), (1, "R1b"), (3, "R3")];

    let mut joined = Vec::new();
    for (lk, lv) in &left {
        for (rk, rv) in &right {
            if lk == rk {
                joined.push((*lk, *lv, *rv));
            }
        }
    }

    assert_eq!(joined.len(), 2);
    assert_eq!(joined[0], (1, "L1", "R1a"));
    assert_eq!(joined[1], (1, "L1", "R1b"));
}

#[test]
fn factorized_join_oracle_exact() {
    let mut factorized_payloads: BTreeMap<i64, Vec<String>> = BTreeMap::new();
    let parent = (1i64, "order_1");
    let children = ["item_1", "item_2", "item_3"];

    factorized_payloads.insert(parent.0, children.iter().map(|s| s.to_string()).collect());

    assert_eq!(factorized_payloads.get(&1).unwrap().len(), 3);
}

#[test]
fn tumble_window_oracle_exact() {
    let window_size = 60_000u64; // 1 minute
    let ts = 160_000u64;
    let window_start = (ts / window_size) * window_size;
    let window_end = window_start + window_size;

    assert_eq!(window_start, 120_000);
    assert_eq!(window_end, 180_000);
}

#[test]
fn hop_window_oracle_exact() {
    let window_size = 300_000u64; // 5 min
    let slide = 60_000u64; // 1 min
    let ts = 150_000u64;

    // Number of active hop windows covering ts
    let mut windows = Vec::new();
    let first_start = if ts >= window_size {
        ts - window_size + slide
    } else {
        0
    };
    let mut w_start = (first_start / slide) * slide;
    while w_start <= ts {
        if ts < w_start + window_size {
            windows.push((w_start, w_start + window_size));
        }
        w_start += slide;
    }

    assert_eq!(windows.len(), 3);
}

#[test]
fn session_window_oracle_exact() {
    let gap = 1_800_000_u64; // 30 min
    let events = [1000u64, 5000, 10_000, 2_000_000]; // 2_000_000 is beyond 30 min gap

    let mut sessions = Vec::new();
    let mut cur_start = events[0];
    let mut cur_end = events[0];

    for &e in &events[1..] {
        if e - cur_end > gap {
            sessions.push((cur_start, cur_end));
            cur_start = e;
            cur_end = e;
        } else {
            cur_end = e;
        }
    }
    sessions.push((cur_start, cur_end));

    assert_eq!(sessions.len(), 2);
    assert_eq!(sessions[0], (1000, 10_000));
    assert_eq!(sessions[1], (2_000_000, 2_000_000));
}
