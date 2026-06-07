//! View-on-view DAG utilities for RockStream (v0.24).
//!
//! Provides compile-time cycle detection for view dependency graphs.
//! A view dependency graph (DAG) maps view names to their `PlanNode`
//! definitions.  The `detect_cycle` function traverses the graph and returns
//! `Err` with the cycle path when a cycle is found (`RS-1011`), or `Ok(())`
//! when the graph is acyclic.
//!
//! # Diamond consistency
//!
//! Diamond patterns (view D depends on views B and C, both of which depend on
//! view A) are legal and produce consistent results via the frontier meet.
//! The scheduler advances D's epoch only after both B and C have completed
//! the same epoch — no explicit group API is needed.
//!
//! # Cadence inheritance
//!
//! A downstream view inherits the epoch cadence of its upstream views
//! structurally: the topological execution order returned by
//! `topological_order` is exactly the scheduling order the runtime uses to
//! drive epochs through the DAG.

use crate::PlanNode;
use std::collections::{HashMap, HashSet};

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Collect all `ViewRef` view-names directly referenced in `plan`.
///
/// Does not recurse into the *definitions* of referenced views — it only
/// collects the names that appear as `PlanNode::ViewRef` nodes in this plan.
pub fn direct_view_refs(plan: &PlanNode) -> Vec<String> {
    let mut refs = Vec::new();
    collect_refs(plan, &mut refs);
    refs
}

fn collect_refs(plan: &PlanNode, out: &mut Vec<String>) {
    match plan {
        PlanNode::ViewRef { view_name } => out.push(view_name.clone()),
        PlanNode::Source { .. } | PlanNode::Snapshot { .. } => {}
        PlanNode::Filter { input, .. } => collect_refs(input, out),
        PlanNode::Project { input, .. } => collect_refs(input, out),
        PlanNode::Map { input, .. } => collect_refs(input, out),
        PlanNode::Aggregate { input, .. } => collect_refs(input, out),
        PlanNode::Window { input, .. } => collect_refs(input, out),
        PlanNode::TumbleWindow { input, .. } => collect_refs(input, out),
        PlanNode::TopK { input, .. } => collect_refs(input, out),
        PlanNode::Join { left, right, .. } => {
            collect_refs(left, out);
            collect_refs(right, out);
        }
        PlanNode::Union { left, right } => {
            collect_refs(left, out);
            collect_refs(right, out);
        }
        PlanNode::Recursion { base, step, .. } => {
            collect_refs(base, out);
            collect_refs(step, out);
        }
        PlanNode::Lateral { input, .. } => collect_refs(input, out),
        // v0.4 additions
        PlanNode::ViewSink { child, .. } => collect_refs(child, out),
        PlanNode::Exchange { child, .. } => collect_refs(child, out),
    }
}

// ─── Cycle detection ─────────────────────────────────────────────────────────

/// Detect cycles in a view dependency DAG.
///
/// `views` maps each view name to its `PlanNode` definition.  A view may
/// reference other views via `PlanNode::ViewRef`.
///
/// Returns `Ok(())` if the graph is acyclic.
/// Returns `Err(cycle_path)` where `cycle_path` is the sequence of view names
/// that form the cycle (the first and last entry are the same view name).
///
/// # Error code
///
/// A cycle triggers `RS-1011` at the call site.  This function itself returns
/// a plain `Err` containing the path for diagnostics; the caller is responsible
/// for wrapping it in the appropriate error type.
pub fn detect_cycle(views: &HashMap<String, PlanNode>) -> Result<(), Vec<String>> {
    let mut visited: HashSet<String> = HashSet::new();
    let mut in_stack: HashSet<String> = HashSet::new();

    for name in views.keys() {
        if !visited.contains(name) {
            let mut path: Vec<String> = Vec::new();
            if dfs(name, views, &mut visited, &mut in_stack, &mut path) {
                return Err(path);
            }
        }
    }
    Ok(())
}

/// Returns the topological order of views (sources first, dependents last).
///
/// Returns `Err(cycle_path)` if the graph contains a cycle.
pub fn topological_order(views: &HashMap<String, PlanNode>) -> Result<Vec<String>, Vec<String>> {
    // First check for cycles.
    detect_cycle(views)?;

    // Kahn's algorithm (BFS).
    let mut in_degree: HashMap<String, usize> = views.keys().map(|n| (n.clone(), 0)).collect();

    // Build adjacency: name → names that depend on name (reverse edges for Kahn).
    let mut dependents: HashMap<String, Vec<String>> = HashMap::new();
    for (name, plan) in views {
        for dep in direct_view_refs(plan) {
            if views.contains_key(&dep) {
                dependents.entry(dep).or_default().push(name.clone());
                *in_degree.entry(name.clone()).or_insert(0) += 1;
            }
        }
    }

    let mut queue: Vec<String> = in_degree
        .iter()
        .filter(|(_, &d)| d == 0)
        .map(|(n, _)| n.clone())
        .collect();
    queue.sort(); // deterministic order within the same level

    let mut order = Vec::new();
    while !queue.is_empty() {
        queue.sort();
        let node = queue.remove(0);
        order.push(node.clone());
        if let Some(deps) = dependents.get(&node) {
            for dep in deps {
                let d = in_degree.get_mut(dep).unwrap();
                *d -= 1;
                if *d == 0 {
                    queue.push(dep.clone());
                }
            }
        }
    }

    Ok(order)
}

/// DFS helper — returns `true` if a cycle is found.
///
/// When a cycle is found, `path` contains the cycle sequence (first == last).
fn dfs(
    name: &str,
    views: &HashMap<String, PlanNode>,
    visited: &mut HashSet<String>,
    in_stack: &mut HashSet<String>,
    path: &mut Vec<String>,
) -> bool {
    visited.insert(name.to_string());
    in_stack.insert(name.to_string());
    path.push(name.to_string());

    if let Some(plan) = views.get(name) {
        for dep in direct_view_refs(plan) {
            if !views.contains_key(&dep) {
                // Reference to an external source; not part of the view DAG.
                continue;
            }
            if !visited.contains(&dep) {
                if dfs(&dep, views, visited, in_stack, path) {
                    return true;
                }
            } else if in_stack.contains(&dep) {
                // Cycle found: close the path.
                path.push(dep);
                return true;
            }
        }
    }

    in_stack.remove(name);
    path.pop();
    false
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Expr, PlanNode};

    fn view_ref(name: &str) -> PlanNode {
        PlanNode::ViewRef {
            view_name: name.to_string(),
        }
    }

    fn source(name: &str) -> PlanNode {
        PlanNode::Source {
            name: name.to_string(),
        }
    }

    #[test]
    fn empty_dag_is_acyclic() {
        let views: HashMap<String, PlanNode> = HashMap::new();
        assert!(detect_cycle(&views).is_ok());
    }

    #[test]
    fn single_source_view_is_acyclic() {
        let mut views = HashMap::new();
        views.insert("v1".to_string(), source("s1"));
        assert!(detect_cycle(&views).is_ok());
    }

    #[test]
    fn linear_chain_is_acyclic() {
        // v1 → v2 → v3 (v1 reads from v2, v2 reads from v3/source)
        let mut views = HashMap::new();
        views.insert("v1".to_string(), view_ref("v2"));
        views.insert("v2".to_string(), view_ref("v3"));
        views.insert("v3".to_string(), source("s1"));
        assert!(detect_cycle(&views).is_ok());
    }

    #[test]
    fn self_loop_is_detected() {
        let mut views = HashMap::new();
        views.insert("v1".to_string(), view_ref("v1"));
        let result = detect_cycle(&views);
        assert!(result.is_err());
        let path = result.unwrap_err();
        assert!(path.contains(&"v1".to_string()));
    }

    #[test]
    fn two_node_cycle_is_detected() {
        let mut views = HashMap::new();
        views.insert("v1".to_string(), view_ref("v2"));
        views.insert("v2".to_string(), view_ref("v1"));
        let result = detect_cycle(&views);
        assert!(result.is_err());
    }

    #[test]
    fn diamond_is_acyclic() {
        // A → B, A → C, B → D, C → D (diamond)
        let mut views = HashMap::new();
        views.insert(
            "d".to_string(),
            PlanNode::Union {
                left: Box::new(view_ref("b")),
                right: Box::new(view_ref("c")),
            },
        );
        views.insert("b".to_string(), view_ref("a"));
        views.insert("c".to_string(), view_ref("a"));
        views.insert("a".to_string(), source("s1"));
        assert!(detect_cycle(&views).is_ok());
    }

    #[test]
    fn direct_view_refs_collects_refs() {
        let plan = PlanNode::Union {
            left: Box::new(view_ref("v1")),
            right: Box::new(PlanNode::Filter {
                input: Box::new(view_ref("v2")),
                predicate: Expr::Column(0),
            }),
        };
        let refs = direct_view_refs(&plan);
        assert!(refs.contains(&"v1".to_string()));
        assert!(refs.contains(&"v2".to_string()));
    }

    #[test]
    fn topological_order_linear_chain() {
        // v3 (source) ← v2 ← v1
        let mut views = HashMap::new();
        views.insert("v1".to_string(), view_ref("v2"));
        views.insert("v2".to_string(), view_ref("v3"));
        views.insert("v3".to_string(), source("s1"));
        let order = topological_order(&views).unwrap();
        let pos = |name: &str| order.iter().position(|n| n == name).unwrap();
        // v3 must come before v2, v2 before v1
        assert!(pos("v3") < pos("v2"));
        assert!(pos("v2") < pos("v1"));
    }
}
