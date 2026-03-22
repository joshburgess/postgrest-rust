use crate::ast::*;
use crate::error::ParseError;
use pg_schema_cache::QualifiedName;

// ---------------------------------------------------------------------------
// Select parser
// ---------------------------------------------------------------------------

/// Parses a PostgREST `select` query parameter into a list of select items.
///
/// Supports columns, `*`, and nested embedding:
///   `id,name,author:authors(name,books(title))`
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
        // Verify closing paren
        if !input.ends_with(')') {
            return Err(ParseError::InvalidSelect(input.to_string()));
        }
        let before = &input[..paren_pos];
        let inner = &input[paren_pos + 1..input.len() - 1];

        let (alias, target) = if let Some(colon) = before.find(':') {
            (
                Some(before[..colon].to_string()),
                before[colon + 1..].to_string(),
            )
        } else {
            (None, before.to_string())
        };

        let sub_select = parse_select(inner)?;

        Ok(SelectItem::Embed {
            alias,
            target,
            sub_request: Box::new(ReadRequest {
                table: QualifiedName::new("", ""), // resolved during SQL building
                select: sub_select,
                filters: Vec::new(),
                order: Vec::new(),
                limit: None,
                offset: None,
                count: CountOption::None,
            }),
        })
    } else {
        Ok(SelectItem::Column(input.to_string()))
    }
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
        assert!(matches!(&items[1], SelectItem::Column(c) if c == "name"));
        assert!(matches!(&items[2], SelectItem::Column(c) if c == "age"));
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
