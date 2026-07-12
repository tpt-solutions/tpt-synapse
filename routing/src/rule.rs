//! Embedded SQL-like Rule Engine (spec.txt §3.2, TODO.md Phase 1).
//!
//! A lightweight filter/route engine: parse `SELECT <* | fields> FROM <source>
//! [WHERE <expr>]` and evaluate the predicate against a per-message
//! [`Context`] (field -> value). Supports `AND`/`OR`/`NOT`, comparisons
//! (`=`, `!=`, `<`, `>`, `<=`, `>=`, `LIKE`) over string/number operands, and
//! `%`/`_` SQL wildcards in `LIKE`. This is intentionally small — the WASM
//! transform track (TODO.md) is the escape hatch for untrusted, heavier logic.

use std::collections::HashMap;

use crate::RoutingError;
use crate::RoutingResult;

/// A message's fields, used during rule evaluation.
pub type Context = HashMap<String, String>;

#[derive(Debug, Clone)]
pub enum Value {
    Ident(String),
    Str(String),
    Num(f64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
    Like,
}

#[derive(Debug, Clone)]
pub enum Expr {
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    Cmp(Value, Op, Value),
}

/// A compiled rule: a source stream plus an optional projection and predicate.
#[derive(Debug, Clone)]
pub struct Rule {
    pub source: String,
    pub fields: Option<Vec<String>>,
    pub predicate: Expr,
}

impl Rule {
    pub fn parse(input: &str) -> RoutingResult<Rule> {
        let mut p = Parser::new(input)?;
        p.expect_keyword("SELECT")?;
        let fields = p.parse_projection()?;
        p.expect_keyword("FROM")?;
        let source = p.expect_ident()?;
        let predicate = if p.peek_keyword("WHERE") {
            p.advance();
            p.parse_or()?
        } else {
            Expr::Cmp(Value::Ident("*".into()), Op::Eq, Value::Str("true".into()))
        };
        if !p.at_end() {
            return Err(RoutingError::new("trailing tokens after rule"));
        }
        Ok(Rule {
            source,
            fields,
            predicate,
        })
    }

    /// Does the predicate pass for `ctx`?
    pub fn matches(&self, ctx: &Context) -> bool {
        eval(&self.predicate, ctx)
    }

    /// Fields selected by the projection (`None` means all).
    pub fn projected(&self, ctx: &Context) -> Context {
        match &self.fields {
            None => ctx.clone(),
            Some(fields) => {
                let mut out = Context::new();
                for f in fields {
                    if let Some(v) = ctx.get(f) {
                        out.insert(f.clone(), v.clone());
                    }
                }
                out
            }
        }
    }
}

fn eval(e: &Expr, ctx: &Context) -> bool {
    match e {
        Expr::And(a, b) => eval(a, ctx) && eval(b, ctx),
        Expr::Or(a, b) => eval(a, ctx) || eval(b, ctx),
        Expr::Not(a) => !eval(a, ctx),
        Expr::Cmp(l, op, r) => compare(l, *op, r, ctx),
    }
}

fn operand_str(v: &Value, ctx: &Context) -> String {
    match v {
        Value::Ident(name) => ctx.get(name).cloned().unwrap_or_default(),
        Value::Str(s) => s.clone(),
        Value::Num(n) => n.to_string(),
    }
}

fn compare(l: &Value, op: Op, r: &Value, ctx: &Context) -> bool {
    let ls = operand_str(l, ctx);
    let rs = operand_str(r, ctx);
    let both_num = ls.parse::<f64>().is_ok() && rs.parse::<f64>().is_ok();
    match op {
        Op::Eq => ls == rs,
        Op::Ne => ls != rs,
        Op::Like => like(&rs, &ls),
        Op::Lt | Op::Gt | Op::Le | Op::Ge if both_num => {
            let a = ls.parse::<f64>().unwrap();
            let b = rs.parse::<f64>().unwrap();
            match op {
                Op::Lt => a < b,
                Op::Gt => a > b,
                Op::Le => a <= b,
                Op::Ge => a >= b,
                _ => unreachable!(),
            }
        }
        Op::Lt | Op::Gt | Op::Le | Op::Ge => {
            let ord = ls.cmp(&rs);
            match op {
                Op::Lt => ord == std::cmp::Ordering::Less,
                Op::Gt => ord == std::cmp::Ordering::Greater,
                Op::Le => ord != std::cmp::Ordering::Greater,
                Op::Ge => ord != std::cmp::Ordering::Less,
                _ => unreachable!(),
            }
        }
    }
}

/// SQL `LIKE`: `%` matches any sequence, `_` matches any single char.
fn like(pattern: &str, value: &str) -> bool {
    fn rec(p: &[char], v: &[char]) -> bool {
        let mut pi = 0;
        let mut vi = 0;
        while pi < p.len() {
            match p[pi] {
                '%' => {
                    // Try to match the rest greedily at every position.
                    for skip in vi..=v.len() {
                        if rec(&p[pi + 1..], &v[skip..]) {
                            return true;
                        }
                    }
                    return false;
                }
                '_' => {
                    if vi >= v.len() {
                        return false;
                    }
                    pi += 1;
                    vi += 1;
                }
                c => {
                    if vi >= v.len() || v[vi] != c {
                        return false;
                    }
                    pi += 1;
                    vi += 1;
                }
            }
        }
        vi == v.len()
    }
    rec(&pattern.chars().collect::<Vec<_>>(), &value.chars().collect::<Vec<_>>())
}

// --- Parser -------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    Op(String),
    Str(String),
    Num(f64),
    Star,
    Comma,
    LParen,
    RParen,
}

struct Parser {
    toks: Vec<Tok>,
    pos: usize,
}

impl Parser {
    fn new(input: &str) -> RoutingResult<Self> {
        Ok(Self {
            toks: tokenize(input)?,
            pos: 0,
        })
    }

    fn at_end(&self) -> bool {
        self.pos >= self.toks.len()
    }

    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }

    fn advance(&mut self) -> Option<Tok> {
        let t = self.toks.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn peek_keyword(&self, kw: &str) -> bool {
        matches!(self.peek(), Some(Tok::Ident(s)) if s.eq_ignore_ascii_case(kw))
    }

    fn expect_keyword(&mut self, kw: &str) -> RoutingResult<()> {
        match self.advance() {
            Some(Tok::Ident(s)) if s.eq_ignore_ascii_case(kw) => Ok(()),
            other => Err(RoutingError::new(format!("expected {kw}, found {other:?}"))),
        }
    }

    fn expect_ident(&mut self) -> RoutingResult<String> {
        match self.advance() {
            Some(Tok::Ident(s)) => Ok(s),
            other => Err(RoutingError::new(format!("expected identifier, found {other:?}"))),
        }
    }

    fn parse_projection(&mut self) -> RoutingResult<Option<Vec<String>>> {
        match self.peek() {
            Some(Tok::Star) => {
                self.advance();
                Ok(None)
            }
            Some(Tok::Ident(_)) => {
                let mut fields = Vec::new();
                loop {
                    fields.push(self.expect_ident()?);
                    if matches!(self.peek(), Some(Tok::Comma)) {
                        self.advance();
                    } else {
                        break;
                    }
                }
                Ok(Some(fields))
            }
            other => Err(RoutingError::new(format!("bad projection: {other:?}"))),
        }
    }

    fn parse_or(&mut self) -> RoutingResult<Expr> {
        let mut left = self.parse_and()?;
        while self.peek_keyword("OR") {
            self.advance();
            let right = self.parse_and()?;
            left = Expr::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> RoutingResult<Expr> {
        let mut left = self.parse_unary()?;
        while self.peek_keyword("AND") {
            self.advance();
            let right = self.parse_unary()?;
            left = Expr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> RoutingResult<Expr> {
        if self.peek_keyword("NOT") {
            self.advance();
            return Ok(Expr::Not(Box::new(self.parse_unary()?)));
        }
        if matches!(self.peek(), Some(Tok::LParen)) {
            self.advance();
            let e = self.parse_or()?;
            match self.advance() {
                Some(Tok::RParen) => Ok(e),
                _ => Err(RoutingError::new("expected )")),
            }
        } else {
            self.parse_cmp()
        }
    }

    fn parse_cmp(&mut self) -> RoutingResult<Expr> {
        let left = self.parse_operand()?;
        let op = if self.peek_keyword("LIKE") {
            self.advance();
            Op::Like
        } else {
            match self.advance() {
                Some(Tok::Op(s)) => match s.as_str() {
                    "=" => Op::Eq,
                    "!=" | "<>" => Op::Ne,
                    "<" => Op::Lt,
                    ">" => Op::Gt,
                    "<=" => Op::Le,
                    ">=" => Op::Ge,
                    other => return Err(RoutingError::new(format!("bad operator {other}"))),
                },
                other => {
                    return Err(RoutingError::new(format!(
                        "expected operator, found {other:?}"
                    )))
                }
            }
        };
        let right = self.parse_operand()?;
        Ok(Expr::Cmp(left, op, right))
    }

    fn parse_operand(&mut self) -> RoutingResult<Value> {
        match self.advance() {
            Some(Tok::Ident(s)) => Ok(Value::Ident(s)),
            Some(Tok::Str(s)) => Ok(Value::Str(s)),
            Some(Tok::Num(n)) => Ok(Value::Num(n)),
            other => Err(RoutingError::new(format!("expected operand, found {other:?}"))),
        }
    }
}

fn tokenize(input: &str) -> RoutingResult<Vec<Tok>> {
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;
    let mut out = Vec::new();
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        if c == '*' {
            out.push(Tok::Star);
            i += 1;
            continue;
        }
        if c == ',' {
            out.push(Tok::Comma);
            i += 1;
            continue;
        }
        if c == '(' {
            out.push(Tok::LParen);
            i += 1;
            continue;
        }
        if c == ')' {
            out.push(Tok::RParen);
            i += 1;
            continue;
        }
        if c == '\'' || c == '"' {
            let quote = c;
            i += 1;
            let start = i;
            while i < chars.len() && chars[i] != quote {
                i += 1;
            }
            if i >= chars.len() {
                return Err(RoutingError::new("unterminated string"));
            }
            out.push(Tok::Str(chars[start..i].iter().collect()));
            i += 1;
            continue;
        }
        if c.is_ascii_digit() || (c == '-' && chars.get(i + 1).map(|x| x.is_ascii_digit()).unwrap_or(false)) {
            let start = i;
            while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                i += 1;
            }
            let s: String = chars[start..i].iter().collect();
            let n = s
                .parse::<f64>()
                .map_err(|_| RoutingError::new("bad number"))?;
            out.push(Tok::Num(n));
            continue;
        }
        if c.is_alphabetic() || c == '_' {
            let start = i;
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let s: String = chars[start..i].iter().collect();
            out.push(Tok::Ident(s));
            continue;
        }
        // multi-char operators
        let two: String = chars[i..(i + 2).min(chars.len())].iter().collect();
        if matches!(two.as_str(), "!=" | "<>" | "<=" | ">=") {
            out.push(Tok::Op(two));
            i += 2;
            continue;
        }
        if c == '=' || c == '<' || c == '>' {
            out.push(Tok::Op(c.to_string()));
            i += 1;
            continue;
        }
        return Err(RoutingError::new(format!("unexpected character '{}'", c)));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(pairs: &[(&str, &str)]) -> Context {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn parse_and_match() {
        let r = Rule::parse("SELECT * FROM orders WHERE region = 'eu' AND amount > 100").unwrap();
        assert!(r.matches(&ctx(&[("region", "eu"), ("amount", "150")])));
        assert!(!r.matches(&ctx(&[("region", "us"), ("amount", "150")])));
        assert!(!r.matches(&ctx(&[("region", "eu"), ("amount", "50")])));
    }

    #[test]
    fn or_and_not_precedence() {
        let r = Rule::parse("SELECT a FROM s WHERE a = 1 OR b = 2 AND NOT c = 3").unwrap();
        assert!(r.matches(&ctx(&[("a", "1")])));
        assert!(r.matches(&ctx(&[("b", "2"), ("c", "9")])));
        assert!(!r.matches(&ctx(&[("b", "2"), ("c", "3")])));
    }

    #[test]
    fn like_wildcards() {
        let r = Rule::parse("SELECT * FROM s WHERE name LIKE 'foo%'").unwrap();
        assert!(r.matches(&ctx(&[("name", "foobar")])));
        assert!(!r.matches(&ctx(&[("name", "barfoo")])));
    }

    #[test]
    fn projection_selects_fields() {
        let r = Rule::parse("SELECT a, b FROM s WHERE a = 'x'").unwrap();
        let c = ctx(&[("a", "x"), ("b", "y"), ("c", "z")]);
        let p = r.projected(&c);
        assert_eq!(p.get("a"), Some(&"x".to_string()));
        assert_eq!(p.get("b"), Some(&"y".to_string()));
        assert!(p.get("c").is_none());
    }
}
