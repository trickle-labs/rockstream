use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SourceRow {
    pub id: u64,
    pub group_id: u64,
    pub dimension_id: u64,
    pub value: i64,
    pub active: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum Change {
    Insert { after: SourceRow },
    Update { before: SourceRow, after: SourceRow },
    Delete { before: SourceRow },
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Corpus {
    pub source: Vec<SourceRow>,
    pub dimension: Vec<(u64, u64)>,
    pub changes: Vec<Change>,
}

pub fn generate(
    seed: u64,
    source_rows: usize,
    dimension_rows: usize,
    live_groups: u64,
    changed_rows: usize,
) -> Corpus {
    assert!(source_rows > 0, "source_rows must be nonzero");
    assert!(dimension_rows > 0, "dimension_rows must be nonzero");
    let mut rng = Rng(seed.max(1));
    let source = (0..source_rows)
        .map(|id| SourceRow {
            id: id as u64,
            group_id: rng.next() % live_groups.max(1),
            dimension_id: rng.next() % dimension_rows as u64,
            value: (rng.next() % 2_000_001) as i64 - 1_000_000,
            active: rng.next() & 1 == 1,
        })
        .collect::<Vec<_>>();
    let dimension = (0..dimension_rows)
        .map(|id| (id as u64, rng.next() % live_groups.max(1)))
        .collect();
    let inserts = changed_rows / 2;
    let updates = changed_rows * 3 / 10;
    let deletes = changed_rows - inserts - updates;
    let mut changes = Vec::with_capacity(changed_rows);
    for i in 0..inserts {
        let id = source_rows as u64 + i as u64;
        changes.push(Change::Insert {
            after: SourceRow {
                id,
                group_id: rng.next() % live_groups.max(1),
                dimension_id: rng.next() % dimension_rows.max(1) as u64,
                value: (rng.next() % 2_000_001) as i64 - 1_000_000,
                active: rng.next() & 1 == 1,
            },
        });
    }
    for i in 0..updates {
        let before = source[(i * 2) % source.len()].clone();
        let mut after = before.clone();
        after.value = (rng.next() % 2_000_001) as i64 - 1_000_000;
        after.active = !before.active;
        changes.push(Change::Update { before, after });
    }
    for i in 0..deletes {
        changes.push(Change::Delete {
            before: source[(i * 2 + 1) % source.len()].clone(),
        });
    }
    Corpus {
        source,
        dimension,
        changes,
    }
}

pub fn canonical_input_json(corpus: &Corpus) -> Vec<u8> {
    serde_json::to_vec(corpus).expect("corpus is serializable")
}

pub fn canonical_changes_json(changes: &[Change]) -> Vec<u8> {
    serde_json::to_vec(changes).expect("changes are serializable")
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}
