use crate::ast::*;
use crate::error::ParseError;
use pg_schema_cache::QualifiedName;

// ---------------------------------------------------------------------------
// Select parser
// ---------------------------------------------------------------------------

/// Parses a PostgREST `select` query parameter into a list of select items.
///
/// Supports columns, `*`, type casts (`name::text`), and nested embedding:
///   `id,name::text,author:authors(name,books(title))`
pub fn parse_select(input: &str) -> Result<Vec<SelectItem>, ParseError> {
    if input.is_empty() {
        return Ok(vec![SelectItem::Star]);
    }
    split_top_level(input, ',')
        .into_iter()
        .map(|part| parse_select_item(part.trim()))
        .collect()
}

fn parse_select_item(input: &str) -> Result<SelectItem, ParseError> {
    if input == "*" {
        return Ok(SelectItem::Star);
    }

    // Look for the first top-level `(` to detect embedding.
    if let Some(paren_pos) = input.find('(') {
        if !input.ends_with(')') {
            return Err(ParseError::InvalidSelect(input.to_string()));
        }
        let before = &input[..paren_pos];
        let inner = &input[paren_pos + 1..input.len() - 1];

        // Parse alias:target!inner or alias:target or just target
        let (alias, rest) = if let Some(colon) = before.find(':') {
            (
                Some(before[..colon].to_string()),
                &before[colon + 1..],
            )
        } else {
            (None, before)
        };

        // Check for !inner hint: target!inner
        let (target, is_inner) = if let Some(t) = rest.strip_suffix("!inner") {
            (t.to_string(), true)
        } else {
            (rest.to_string(), false)
        };

        let sub_select = parse_select(inner)?;

        return Ok(SelectItem::Embed {
            alias,
            target,
            inner: is_inner,
            sub_request: Box::new(ReadRequest {
                table: QualifiedName::new("", ""),
                select: sub_select,
                filters: FilterNode::empty(),
                order: Vec::new(),
                limit: None,
                offset: None,
                count: CountOption::None,
            }),
        });
    }

    // Check for JSON path access: column->>key or column->key
    // Also with optional cast: column->>key::type
    if let Some(pos) = input.find("->>") {
        let column = input[..pos].to_string();
        let rest = &input[pos + 3..];
        let (path, cast) = if let Some((p, t)) = rest.split_once("::") {
            (p.to_string(), Some(t.to_string()))
        } else {
            (rest.to_string(), None)
        };
        return Ok(SelectItem::JsonAccess {
            column,
            path,
            as_text: true,
            cast,
        });
    }
    if let Some(pos) = input.find("->") {
        let column = input[..pos].to_string();
        let rest = &input[pos + 2..];
        let (path, cast) = if let Some((p, t)) = rest.split_once("::") {
            (p.to_string(), Some(t.to_string()))
        } else {
            (rest.to_string(), None)
        };
        return Ok(SelectItem::JsonAccess {
            column,
            path,
            as_text: false,
            cast,
        });
    }

    // Check for type cast: `column::type`
    if let Some((name, pg_type)) = input.split_once("::") {
        return Ok(SelectItem::Cast {
            column: name.to_string(),
            pg_type: pg_type.to_string(),
        });
    }

    Ok(SelectItem::Column(input.to_string()))
}

// ---------------------------------------------------------------------------
// Filter parser
// ---------------------------------------------------------------------------

/// Parses a PostgREST filter value like `eq.25`, `not.in.(1,2,3)`,
/// `fts(english).term` into a [`Filter`].
pub fn parse_filter(column: &str, raw_value: &str) -> Result<Filter, ParseError> {
    let (negated, remaining) = if let Some(rest) = raw_value.strip_prefix("not.") {
        (true, rest)
    } else {
        (false, raw_value)
    };

    let (op_str, value_str) = remaining
        .split_once('.')
        .ok_or_else(|| ParseError::InvalidFilter(raw_value.to_string()))?;

    let (operator, value) = parse_op_and_value(op_str, value_str)?;

    Ok(Filter {
        column: column.to_string(),
        operator,
        value,
        negated,
    })
}

// ---------------------------------------------------------------------------
// Logic filter parser (or/and grouping)
// ---------------------------------------------------------------------------

/// Parse a top-level `or` or `and` query parameter value into a [`FilterNode`].
///
/// The value has the form `(expr1,expr2,...)` where each expr is either
/// `column.op.value`, `not.column.op.value`, `or(...)`, or `and(...)`.
pub fn parse_logic_filter(key: &str, value: &str) -> Result<FilterNode, ParseError> {
    let inner = value
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .ok_or_else(|| ParseError::InvalidFilter(format!("{key}.{value}")))?;

    let parts = split_top_level(inner, ',');
    let nodes: Result<Vec<FilterNode>, _> = parts
        .iter()
        .map(|part| parse_filter_expression(part.trim()))
        .collect();

    match key {
        "or" => Ok(FilterNode::Or(nodes?)),
        "and" => Ok(FilterNode::And(nodes?)),
        _ => Err(ParseError::InvalidFilter(key.to_string())),
    }
}

/// Parse a single filter expression inside an `or(...)` or `and(...)` group.
///
/// Handles: `column.op.value`, `not.column.op.value`,
/// `or(...)`, `and(...)`, `not.or(...)`, `not.and(...)`.
pub fn parse_filter_expression(expr: &str) -> Result<FilterNode, ParseError> {
    let (negated, rest) = if let Some(r) = expr.strip_prefix("not.") {
        (true, r)
    } else {
        (false, expr)
    };

    // Nested or(...)
    if let Some(inner) = rest
        .strip_prefix("or(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let parts = split_top_level(inner, ',');
        let nodes: Result<Vec<FilterNode>, _> = parts
            .iter()
            .map(|p| parse_filter_expression(p.trim()))
            .collect();
        let node = FilterNode::Or(nodes?);
        return Ok(if negated {
            FilterNode::Not(Box::new(node))
        } else {
            node
        });
    }

    // Nested and(...)
    if let Some(inner) = rest
        .strip_prefix("and(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let parts = split_top_level(inner, ',');
        let nodes: Result<Vec<FilterNode>, _> = parts
            .iter()
            .map(|p| parse_filter_expression(p.trim()))
            .collect();
        let node = FilterNode::And(nodes?);
        return Ok(if negated {
            FilterNode::Not(Box::new(node))
        } else {
            node
        });
    }

    // Simple filter: column.op.value (split on first dot for column name)
    let (column, filter_value) = rest
        .split_once('.')
        .ok_or_else(|| ParseError::InvalidFilter(expr.to_string()))?;

    let mut filter = parse_filter(column, filter_value)?;
    if negated {
        filter.negated = !filter.negated;
    }
    Ok(FilterNode::Condition(filter))
}

fn parse_op_and_value(
    op_str: &str,
    value_str: &str,
) -> Result<(FilterOp, FilterValue), ParseError> {
    // Handle FTS operators with optional language: fts(english), plfts, etc.
    if let Some(paren_start) = op_str.find('(') {
        let base = &op_str[..paren_start];
        let lang = op_str[paren_start + 1..]
            .strip_suffix(')')
            .ok_or_else(|| ParseError::UnknownOperator(op_str.to_string()))?;
        let lang = Some(lang.to_string());
        let op = match base {
            "fts" => FilterOp::Fts(lang),
            "plfts" => FilterOp::Plfts(lang),
            "phfts" => FilterOp::Phfts(lang),
            "wfts" => FilterOp::Wfts(lang),
            _ => return Err(ParseError::UnknownOperator(op_str.to_string())),
        };
        return Ok((op, FilterValue::Value(value_str.to_string())));
    }

    match op_str {
        "eq" => Ok((FilterOp::Eq, FilterValue::Value(value_str.to_string()))),
        "neq" => Ok((FilterOp::Neq, FilterValue::Value(value_str.to_string()))),
        "gt" => Ok((FilterOp::Gt, FilterValue::Value(value_str.to_string()))),
        "gte" => Ok((FilterOp::Gte, FilterValue::Value(value_str.to_string()))),
        "lt" => Ok((FilterOp::Lt, FilterValue::Value(value_str.to_string()))),
        "lte" => Ok((FilterOp::Lte, FilterValue::Value(value_str.to_string()))),
        "like" => Ok((
            FilterOp::Like,
            FilterValue::Value(value_str.replace('*', "%")),
        )),
        "ilike" => Ok((
            FilterOp::Ilike,
            FilterValue::Value(value_str.replace('*', "%")),
        )),
        "is" => Ok((FilterOp::Is, FilterValue::Value(value_str.to_string()))),
        "in" => {
            let inner = value_str
                .strip_prefix('(')
                .and_then(|s| s.strip_suffix(')'))
                .ok_or_else(|| ParseError::InvalidFilter(format!("in.{value_str}")))?;
            let values: Vec<String> = inner.split(',').map(|s| s.trim().to_string()).collect();
            Ok((FilterOp::In, FilterValue::List(values)))
        }
        "cs" => Ok((FilterOp::Contains, FilterValue::Value(value_str.to_string()))),
        "cd" => Ok((
            FilterOp::ContainedIn,
            FilterValue::Value(value_str.to_string()),
        )),
        "ov" => Ok((FilterOp::Overlaps, FilterValue::Value(value_str.to_string()))),
        "fts" => Ok((
            FilterOp::Fts(None),
            FilterValue::Value(value_str.to_string()),
        )),
        "plfts" => Ok((
            FilterOp::Plfts(None),
            FilterValue::Value(value_str.to_string()),
        )),
        "phfts" => Ok((
            FilterOp::Phfts(None),
            FilterValue::Value(value_str.to_string()),
        )),
        "wfts" => Ok((
            FilterOp::Wfts(None),
            FilterValue::Value(value_str.to_string()),
        )),
        _ => Err(ParseError::UnknownOperator(op_str.to_string())),
    }
}

// ---------------------------------------------------------------------------
// Order parser
// ---------------------------------------------------------------------------

/// Parses a PostgREST `order` query parameter: `name.asc,age.desc.nullslast`
pub fn parse_order(input: &str) -> Result<Vec<OrderClause>, ParseError> {
    if input.is_empty() {
        return Ok(Vec::new());
    }
    input
        .split(',')
        .map(|item| {
            let parts: Vec<&str> = item.split('.').collect();
            if parts.is_empty() {
                return Err(ParseError::InvalidOrder(item.to_string()));
            }
            let column = parts[0].to_string();
            let mut direction = OrderDirection::Asc;
            let mut nulls = None;

            for &part in &parts[1..] {
                match part {
                    "asc" => direction = OrderDirection::Asc,
                    "desc" => direction = OrderDirection::Desc,
                    "nullsfirst" => nulls = Some(NullsOrder::First),
                    "nullslast" => nulls = Some(NullsOrder::Last),
                    _ => return Err(ParseError::InvalidOrder(item.to_string())),
                }
            }

            Ok(OrderClause {
                column,
                direction,
                nulls,
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

/// Split `input` on `sep`, but only at the top level (not inside parentheses).
fn split_top_level(input: &str, sep: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth: i32 = 0;
    let mut start = 0;
    for (i, c) in input.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            c if c == sep && depth == 0 => {
                parts.push(&input[start..i]);
                start = i + c.len_utf8();
            }
            _ => {}
        }
    }
    parts.push(&input[start..]);
    parts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select_star() {
        let items = parse_select("*").unwrap();
        assert!(matches!(items[0], SelectItem::Star));
    }

    #[test]
    fn test_parse_select_columns() {
        let items = parse_select("id,name,age").unwrap();
        assert_eq!(items.len(), 3);
        assert!(matches!(&items[0], SelectItem::Column(c) if c == "id"));
    }

    #[test]
    fn test_parse_select_cast() {
        let items = parse_select("id::text,name").unwrap();
        assert_eq!(items.len(), 2);
        assert!(matches!(&items[0], SelectItem::Cast { column, pg_type } if column == "id" && pg_type == "text"));
    }

    #[test]
    fn test_parse_select_embed() {
        let items = parse_select("id,author:authors(name)").unwrap();
        assert_eq!(items.len(), 2);
        match &items[1] {
            SelectItem::Embed {
                alias,
                target,
                sub_request,
                ..
            } => {
                assert_eq!(alias.as_deref(), Some("author"));
                assert_eq!(target, "authors");
                assert_eq!(sub_request.select.len(), 1);
            }
            _ => panic!("expected embed"),
        }
    }

    #[test]
    fn test_parse_select_nested_embed() {
        let items = parse_select("id,authors(name,books(title))").unwrap();
        assert_eq!(items.len(), 2);
        match &items[1] {
            SelectItem::Embed {
                target,
                sub_request,
                ..
            } => {
                assert_eq!(target, "authors");
                assert_eq!(sub_request.select.len(), 2);
                assert!(matches!(&sub_request.select[1], SelectItem::Embed { target, .. } if target == "books"));
            }
            _ => panic!("expected embed"),
        }
    }

    #[test]
    fn test_parse_filter_eq() {
        let f = parse_filter("age", "eq.25").unwrap();
        assert_eq!(f.column, "age");
        assert!(matches!(f.operator, FilterOp::Eq));
        assert!(matches!(&f.value, FilterValue::Value(v) if v == "25"));
        assert!(!f.negated);
    }

    #[test]
    fn test_parse_filter_not_in() {
        let f = parse_filter("id", "not.in.(1,2,3)").unwrap();
        assert!(f.negated);
        assert!(matches!(f.operator, FilterOp::In));
        assert!(
            matches!(&f.value, FilterValue::List(v) if v == &["1", "2", "3"])
        );
    }

    #[test]
    fn test_parse_filter_like() {
        let f = parse_filter("name", "like.*smith*").unwrap();
        assert!(matches!(&f.value, FilterValue::Value(v) if v == "%smith%"));
    }

    #[test]
    fn test_parse_filter_fts_with_lang() {
        let f = parse_filter("body", "fts(english).search").unwrap();
        assert!(matches!(&f.operator, FilterOp::Fts(Some(l)) if l == "english"));
    }

    #[test]
    fn test_parse_logic_or() {
        let node = parse_logic_filter("or", "(age.gt.18,name.eq.Alice)").unwrap();
        assert!(matches!(node, FilterNode::Or(ref v) if v.len() == 2));
    }

    #[test]
    fn test_parse_logic_nested() {
        let node =
            parse_logic_filter("or", "(age.gt.18,and(name.eq.Alice,status.eq.active))").unwrap();
        match node {
            FilterNode::Or(v) => {
                assert_eq!(v.len(), 2);
                assert!(matches!(&v[1], FilterNode::And(inner) if inner.len() == 2));
            }
            _ => panic!("expected Or"),
        }
    }

    #[test]
    fn test_parse_logic_not() {
        let node = parse_filter_expression("not.age.gt.18").unwrap();
        match node {
            FilterNode::Condition(f) => assert!(f.negated),
            _ => panic!("expected condition"),
        }
    }

    #[test]
    fn test_parse_order() {
        let clauses = parse_order("name.asc,age.desc.nullslast").unwrap();
        assert_eq!(clauses.len(), 2);
        assert_eq!(clauses[0].column, "name");
        assert_eq!(clauses[0].direction, OrderDirection::Asc);
        assert_eq!(clauses[1].column, "age");
        assert_eq!(clauses[1].direction, OrderDirection::Desc);
        assert_eq!(clauses[1].nulls, Some(NullsOrder::Last));
    }
}
