use alloy_primitives::B256;

use crate::ast::{BinOp, Expr};

/// A query plan: filters extracted from the WHERE clause for index lookups.
#[derive(Debug, Clone, Default)]
pub struct QueryPlan {
    /// Exact address filter (WHERE address = '0x...')
    pub address: Option<Vec<u8>>,
    /// Exact topic0 filter (WHERE topic0 = event'...' or topic0 = '0x...')
    pub topic0: Option<B256>,
    /// Exact topic1 filter
    pub topic1: Option<B256>,
    /// Block number range [from, to). None means unbounded.
    pub block_from: Option<u64>,
    pub block_to: Option<u64>,
    /// The latest keyword was used — needs resolution at execution time.
    pub uses_latest: bool,
    /// Remaining filters that couldn't be pushed to indexes.
    pub residual_filters: Vec<Expr>,
}

/// Extract a query plan from a WHERE clause expression.
pub fn plan_where(expr: &Expr) -> QueryPlan {
    let mut plan = QueryPlan::default();
    extract_filters(expr, &mut plan);
    plan
}

fn extract_filters(expr: &Expr, plan: &mut QueryPlan) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            extract_filters(left, plan);
            extract_filters(right, plan);
        }
        Expr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => {
            if !try_extract_eq(left, right, plan) {
                plan.residual_filters.push(expr.clone());
            }
        }
        Expr::BinaryOp {
            left,
            op: BinOp::Ge,
            right,
        } => {
            // column >= value  →  block_from
            if let Expr::Column(name) = left.as_ref()
                && name == "block_number"
            {
                if contains_latest(right) {
                    plan.uses_latest = true;
                    plan.residual_filters.push(expr.clone());
                    return;
                }
                if let Some(n) = resolve_number(right) {
                    plan.block_from = Some(n as u64);
                    return;
                }
            }
            plan.residual_filters.push(expr.clone());
        }
        Expr::BinaryOp {
            left,
            op: BinOp::Le,
            right,
        } => {
            if let Expr::Column(name) = left.as_ref()
                && name == "block_number"
            {
                if contains_latest(right) {
                    plan.uses_latest = true;
                    plan.residual_filters.push(expr.clone());
                    return;
                }
                if let Some(n) = resolve_number(right) {
                    plan.block_to = Some(n as u64 + 1); // inclusive → exclusive
                    return;
                }
            }
            plan.residual_filters.push(expr.clone());
        }
        Expr::BinaryOp {
            left,
            op: BinOp::Lt,
            right,
        } => {
            if let Expr::Column(name) = left.as_ref()
                && name == "block_number"
            {
                if contains_latest(right) {
                    plan.uses_latest = true;
                    plan.residual_filters.push(expr.clone());
                    return;
                }
                if let Some(n) = resolve_number(right) {
                    plan.block_to = Some(n as u64);
                    return;
                }
            }
            plan.residual_filters.push(expr.clone());
        }
        Expr::BinaryOp {
            left,
            op: BinOp::Gt,
            right,
        } => {
            if let Expr::Column(name) = left.as_ref()
                && name == "block_number"
            {
                if contains_latest(right) {
                    plan.uses_latest = true;
                    plan.residual_filters.push(expr.clone());
                    return;
                }
                if let Some(n) = resolve_number(right) {
                    plan.block_from = Some(n as u64 + 1);
                    return;
                }
            }
            plan.residual_filters.push(expr.clone());
        }
        Expr::Between {
            expr: between_expr,
            low,
            high,
        } => {
            if let Expr::Column(name) = between_expr.as_ref()
                && name == "block_number"
                    && let (Some(lo), Some(hi)) = (resolve_number(low), resolve_number(high)) {
                        plan.block_from = Some(lo as u64);
                        plan.block_to = Some(hi as u64 + 1); // BETWEEN is inclusive
                        return;
                    }
            plan.residual_filters.push(expr.clone());
        }
        _ => {
            plan.residual_filters.push(expr.clone());
        }
    }
}

/// Try to extract an equality filter from `left = right`.
fn try_extract_eq(left: &Expr, right: &Expr, plan: &mut QueryPlan) -> bool {
    if let Expr::Column(name) = left {
        match name.as_str() {
            "address" => {
                if let Some(bytes) = expr_to_bytes(right) {
                    plan.address = Some(bytes);
                    return true;
                }
            }
            "topic0" => {
                if let Some(hash) = expr_to_b256(right) {
                    plan.topic0 = Some(hash);
                    return true;
                }
            }
            "topic1" => {
                if let Some(hash) = expr_to_b256(right) {
                    plan.topic1 = Some(hash);
                    return true;
                }
            }
            "block_number" => {
                if let Some(n) = resolve_number(right) {
                    plan.block_from = Some(n as u64);
                    plan.block_to = Some(n as u64 + 1);
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}

/// Convert an expression to raw bytes (for address matching).
fn expr_to_bytes(expr: &Expr) -> Option<Vec<u8>> {
    match expr {
        Expr::StringLit(s) => {
            let hex = s.strip_prefix("0x").unwrap_or(s);
            hex::decode(hex).ok()
        }
        Expr::AddressPadded(b) => Some(b.as_slice().to_vec()),
        _ => None,
    }
}

/// Convert an expression to B256 (for topic matching).
fn expr_to_b256(expr: &Expr) -> Option<B256> {
    match expr {
        Expr::EventHash(h) => Some(*h),
        Expr::AddressPadded(b) => Some(*b),
        Expr::StringLit(s) => {
            let hex = s.strip_prefix("0x").unwrap_or(s);
            let bytes = hex::decode(hex).ok()?;
            if bytes.len() == 32 {
                Some(B256::from_slice(&bytes))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Try to resolve a numeric value from an expression (handles `latest - N`).
/// Returns None if it contains `latest` (must be resolved at runtime).
fn resolve_number(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Number(n) => Some(*n),
        Expr::Latest => None, // needs runtime resolution
        Expr::BinaryOp {
            left,
            op: BinOp::Sub,
            right,
        } => {
            // latest - N: can't resolve statically
            if contains_latest(left) {
                return None;
            }
            let l = resolve_number(left)?;
            let r = resolve_number(right)?;
            Some(l - r)
        }
        Expr::BinaryOp {
            left,
            op: BinOp::Add,
            right,
        } => {
            if contains_latest(left) || contains_latest(right) {
                return None;
            }
            let l = resolve_number(left)?;
            let r = resolve_number(right)?;
            Some(l + r)
        }
        _ => None,
    }
}

fn contains_latest(expr: &Expr) -> bool {
    match expr {
        Expr::Latest => true,
        Expr::BinaryOp { left, right, .. } => contains_latest(left) || contains_latest(right),
        _ => false,
    }
}

/// Resolve `latest` references in a plan by substituting the current head block.
pub fn resolve_latest(plan: &mut QueryPlan, head_block: u64) {
    // If the plan uses latest but we couldn't resolve block bounds statically,
    // we need to re-evaluate the original WHERE clause. For now, we handle the
    // common case: `block_number >= latest - N`.
    if plan.uses_latest && plan.block_from.is_none() && plan.block_to.is_none() {
        // Try to find and resolve from residual filters
        let mut resolved = Vec::new();
        let mut remaining = Vec::new();

        for filter in plan.residual_filters.drain(..) {
            if let Some((from, to)) = try_resolve_block_range(&filter, head_block) {
                if from.is_some() {
                    plan.block_from = from;
                }
                if to.is_some() {
                    plan.block_to = to;
                }
                resolved.push(filter);
            } else {
                remaining.push(filter);
            }
        }
        plan.residual_filters = remaining;
    }
}

fn try_resolve_block_range(expr: &Expr, head: u64) -> Option<(Option<u64>, Option<u64>)> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinOp::Ge,
            right,
        } => {
            if let Expr::Column(name) = left.as_ref()
                && name == "block_number"
                    && let Some(n) = resolve_with_latest(right, head) {
                        return Some((Some(n), None));
                    }
            None
        }
        _ => None,
    }
}

fn resolve_with_latest(expr: &Expr, head: u64) -> Option<u64> {
    match expr {
        Expr::Number(n) => Some(*n as u64),
        Expr::Latest => Some(head),
        Expr::BinaryOp {
            left,
            op: BinOp::Sub,
            right,
        } => {
            let l = resolve_with_latest(left, head)?;
            let r = resolve_with_latest(right, head)?;
            Some(l.saturating_sub(r))
        }
        Expr::BinaryOp {
            left,
            op: BinOp::Add,
            right,
        } => {
            let l = resolve_with_latest(left, head)?;
            let r = resolve_with_latest(right, head)?;
            Some(l + r)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse;

    #[test]
    fn test_plan_address_eq() {
        let q = parse(
            "SELECT * FROM logs WHERE address = '0xdAC17F958D2ee523a2206206994597C13D831ec7'",
        )
        .unwrap();
        let plan = plan_where(q.where_clause.as_ref().unwrap());
        assert!(plan.address.is_some());
        assert_eq!(plan.address.as_ref().unwrap().len(), 20);
    }

    #[test]
    fn test_plan_topic0_event() {
        let q = parse("SELECT * FROM logs WHERE topic0 = event'Transfer(address,address,uint256)'")
            .unwrap();
        let plan = plan_where(q.where_clause.as_ref().unwrap());
        assert!(plan.topic0.is_some());
    }

    #[test]
    fn test_plan_block_range() {
        let q = parse("SELECT * FROM logs WHERE block_number BETWEEN 100 AND 200").unwrap();
        let plan = plan_where(q.where_clause.as_ref().unwrap());
        assert_eq!(plan.block_from, Some(100));
        assert_eq!(plan.block_to, Some(201)); // inclusive
    }

    #[test]
    fn test_plan_block_ge() {
        let q = parse("SELECT * FROM logs WHERE block_number >= 1000").unwrap();
        let plan = plan_where(q.where_clause.as_ref().unwrap());
        assert_eq!(plan.block_from, Some(1000));
        assert!(plan.block_to.is_none());
    }

    #[test]
    fn test_plan_combined() {
        let q = parse(
            "SELECT * FROM logs WHERE address = '0xdAC17F958D2ee523a2206206994597C13D831ec7' AND topic0 = event'Transfer(address,address,uint256)' AND block_number >= 100",
        ).unwrap();
        let plan = plan_where(q.where_clause.as_ref().unwrap());
        assert!(plan.address.is_some());
        assert!(plan.topic0.is_some());
        assert_eq!(plan.block_from, Some(100));
        assert!(plan.residual_filters.is_empty());
    }

    #[test]
    fn test_plan_latest() {
        let q = parse("SELECT * FROM logs WHERE block_number >= latest - 1000").unwrap();
        let plan = plan_where(q.where_clause.as_ref().unwrap());
        assert!(plan.uses_latest);
        // Can't resolve statically
        assert!(plan.block_from.is_none());
    }

    #[test]
    fn test_resolve_latest() {
        let q = parse("SELECT * FROM logs WHERE block_number >= latest - 1000").unwrap();
        let mut plan = plan_where(q.where_clause.as_ref().unwrap());
        resolve_latest(&mut plan, 20_000_000);
        assert_eq!(plan.block_from, Some(19_999_000));
    }
}
