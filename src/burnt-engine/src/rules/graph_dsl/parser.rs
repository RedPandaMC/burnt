//! Recursive-descent parser for the graph-query DSL.
//!
//! Converts a stream of tokens (from [`lexer`](super::lexer)) into a
//! [`Pattern`] IR tree. Stops at the first error and returns it with
//! source position.

use std::sync::Arc;

use crate::rules::graph_dsl::error::{ParseError, ParseErrorKind};
use crate::rules::graph_dsl::ir::{
    Capture, Head, Pattern, PatternBody, PredArg, Predicate, Prefix, Value,
};
use crate::rules::graph_dsl::lexer::{Lexer, Token, TokenKind};

/// Parse a single top-level pattern from `source`.
///
/// The DSL accepts exactly one pattern per `detect` or `exclude` block —
/// composition happens through nested patterns and `#and`/`#or`/`#not`
/// predicates, not through top-level sequencing.
pub fn parse_pattern(source: &str) -> Result<Pattern, ParseError> {
    let tokens = Lexer::new(source).tokenize()?;
    if tokens.is_empty() {
        return Err(ParseError::new(ParseErrorKind::EmptyPattern, 1, 1));
    }
    let mut p = Parser::new(tokens);
    let pattern = p.parse_pattern()?;
    if let Some(extra) = p.peek().cloned() {
        return Err(ParseError::new(
            ParseErrorKind::UnexpectedToken {
                token: format!("{:?}", extra.kind),
            },
            extra.line,
            extra.column,
        ));
    }
    Ok(pattern)
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn bump(&mut self) -> Option<Token> {
        let t = self.tokens.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn expect_lparen(&mut self) -> Result<(u32, u32), ParseError> {
        let Some(tok) = self.bump() else {
            return Err(ParseError::new(ParseErrorKind::UnexpectedEof, 0, 0));
        };
        if !matches!(tok.kind, TokenKind::LParen) {
            return Err(ParseError::new(
                ParseErrorKind::UnexpectedToken {
                    token: format!("{:?}", tok.kind),
                },
                tok.line,
                tok.column,
            ));
        }
        Ok((tok.line, tok.column))
    }

    /// Parse a single pattern starting from a `(` token.
    fn parse_pattern(&mut self) -> Result<Pattern, ParseError> {
        let (line, column) = self.expect_lparen()?;

        // The head identifier comes next. The lexer keeps the prefix +
        // kind glued as one ident (`op:Read`, `ast/Call`); we split here.
        let head_tok = self.bump().ok_or_else(|| {
            ParseError::new(ParseErrorKind::UnexpectedEof, line, column)
        })?;
        let head = match head_tok.kind {
            TokenKind::Ident(ref s) => parse_head(s.as_ref(), head_tok.line, head_tok.column)?,
            other => {
                return Err(ParseError::new(
                    ParseErrorKind::InvalidHead {
                        token: format!("{other:?}"),
                    },
                    head_tok.line,
                    head_tok.column,
                ));
            }
        };

        let mut props: Vec<(String, Value)> = Vec::new();
        let mut body: Vec<PatternBody> = Vec::new();

        // Body is a sequence of `:prop value`, `@cap`, `(pattern)`, or
        // `(#predicate ...)` until the matching `)`.
        loop {
            let Some(tok) = self.peek().cloned() else {
                return Err(ParseError::new(
                    ParseErrorKind::UnbalancedOpenParen,
                    line,
                    column,
                ));
            };
            match &tok.kind {
                TokenKind::RParen => {
                    self.bump();
                    break;
                }
                TokenKind::Colon(name) => {
                    self.bump();
                    let value = self.parse_value().map_err(|e| {
                        // Replace generic error with prop-context error
                        // when the prop turned out to have no value.
                        if matches!(e.kind, ParseErrorKind::UnexpectedEof) {
                            ParseError::new(
                                ParseErrorKind::MissingPropValue {
                                    name: name.to_string(),
                                },
                                tok.line,
                                tok.column,
                            )
                        } else {
                            e
                        }
                    })?;
                    props.push((name.to_string(), value));
                }
                TokenKind::At(name) => {
                    self.bump();
                    body.push(PatternBody::Capture(Capture {
                        name: name.to_string(),
                        line: tok.line,
                        column: tok.column,
                    }));
                }
                TokenKind::LParen => {
                    // Either a nested pattern or a predicate. Disambiguate
                    // by looking at the token after the `(`.
                    let next = self.tokens.get(self.pos + 1);
                    match next.map(|t| &t.kind) {
                        Some(TokenKind::Hash(_)) => {
                            let pred = self.parse_predicate()?;
                            body.push(PatternBody::Predicate(pred));
                        }
                        _ => {
                            let sub = self.parse_pattern()?;
                            body.push(PatternBody::Sub(sub));
                        }
                    }
                }
                _ => {
                    return Err(ParseError::new(
                        ParseErrorKind::UnexpectedToken {
                            token: format!("{:?}", tok.kind),
                        },
                        tok.line,
                        tok.column,
                    ));
                }
            }
        }

        Ok(Pattern {
            head,
            props,
            body,
            line,
            column,
        })
    }

    /// Parse a `(#name args...)` predicate. `(` already pending.
    fn parse_predicate(&mut self) -> Result<Predicate, ParseError> {
        let (line, column) = self.expect_lparen()?;
        let head_tok = self.bump().ok_or_else(|| {
            ParseError::new(ParseErrorKind::UnexpectedEof, line, column)
        })?;
        let name = match head_tok.kind {
            TokenKind::Hash(ref s) => s.to_string(),
            other => {
                return Err(ParseError::new(
                    ParseErrorKind::InvalidPredicate {
                        token: format!("{other:?}"),
                    },
                    head_tok.line,
                    head_tok.column,
                ));
            }
        };

        let mut args: Vec<PredArg> = Vec::new();
        loop {
            let Some(tok) = self.peek().cloned() else {
                return Err(ParseError::new(
                    ParseErrorKind::UnbalancedOpenParen,
                    line,
                    column,
                ));
            };
            match &tok.kind {
                TokenKind::RParen => {
                    self.bump();
                    break;
                }
                TokenKind::LParen => {
                    let next = self.tokens.get(self.pos + 1);
                    match next.map(|t| &t.kind) {
                        Some(TokenKind::Hash(_)) => {
                            args.push(PredArg::Predicate(self.parse_predicate()?));
                        }
                        _ => {
                            args.push(PredArg::Pattern(self.parse_pattern()?));
                        }
                    }
                }
                TokenKind::Colon(name) => {
                    // `:key` inside a predicate is a single keyword arg.
                    // If the caller wants `:key value` pairs (e.g. `:as @n`,
                    // `:confidence "High"`), they're two consecutive args
                    // and the predicate registry pairs them at dispatch
                    // time. Standalone `:stage` (no value) is valid for
                    // flag-style predicates like `(#has-overlay :stage)`.
                    self.bump();
                    args.push(PredArg::Value(Value::Ident(Arc::from(format!(":{name}")))));
                }
                _ => {
                    args.push(PredArg::Value(self.parse_value()?));
                }
            }
        }

        Ok(Predicate {
            name,
            args,
            line,
            column,
        })
    }

    /// Parse a `Value` (literal, capture ref, ident, list).
    fn parse_value(&mut self) -> Result<Value, ParseError> {
        let Some(tok) = self.bump() else {
            return Err(ParseError::new(ParseErrorKind::UnexpectedEof, 0, 0));
        };
        match tok.kind {
            TokenKind::String(s) => Ok(Value::String(s)),
            TokenKind::Number(n) => Ok(Value::Number(n)),
            TokenKind::Size(b) => Ok(Value::Size(b)),
            TokenKind::DurationMs(ms) => Ok(Value::DurationMs(ms)),
            TokenKind::Bool(b) => Ok(Value::Bool(b)),
            TokenKind::At(name) => Ok(Value::CaptureRef(name)),
            TokenKind::Ident(s) => Ok(Value::Ident(s)),
            TokenKind::LBracket => self.parse_list(tok.line, tok.column),
            other => Err(ParseError::new(
                ParseErrorKind::UnexpectedToken {
                    token: format!("{other:?}"),
                },
                tok.line,
                tok.column,
            )),
        }
    }

    fn parse_list(&mut self, line: u32, column: u32) -> Result<Value, ParseError> {
        let mut items = Vec::new();
        loop {
            let Some(tok) = self.peek().cloned() else {
                return Err(ParseError::new(
                    ParseErrorKind::UnbalancedOpenParen,
                    line,
                    column,
                ));
            };
            if matches!(tok.kind, TokenKind::RBracket) {
                self.bump();
                return Ok(Value::List(items));
            }
            items.push(self.parse_value()?);
        }
    }
}

fn parse_head(token: &str, line: u32, column: u32) -> Result<Head, ParseError> {
    // Heads are `<prefix>:<Kind>` or `<prefix>/<Kind>`. The lexer keeps
    // them as a single ident; split by the first separator.
    let sep_idx = token
        .find(|c: char| c == ':' || c == '/')
        .ok_or_else(|| {
            ParseError::new(
                ParseErrorKind::InvalidHead {
                    token: token.to_string(),
                },
                line,
                column,
            )
        })?;
    let prefix_str = &token[..sep_idx];
    let separator = &token[sep_idx..=sep_idx];
    let kind = &token[sep_idx + 1..];
    if kind.is_empty() {
        return Err(ParseError::new(
            ParseErrorKind::InvalidHead {
                token: token.to_string(),
            },
            line,
            column,
        ));
    }
    let prefix = Prefix::from_token(prefix_str).ok_or_else(|| {
        ParseError::new(
            ParseErrorKind::UnknownPrefix {
                prefix: prefix_str.to_string(),
            },
            line,
            column,
        )
    })?;
    // Validate the separator matches the prefix's expected form.
    let expected_sep = prefix.separator();
    if separator != expected_sep.to_string() {
        return Err(ParseError::new(
            ParseErrorKind::InvalidHead {
                token: token.to_string(),
            },
            line,
            column,
        ));
    }
    Ok(Head {
        prefix,
        kind: kind.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_pattern() {
        let p = parse_pattern("(op:Read)").unwrap();
        assert_eq!(p.head.prefix, Prefix::Op);
        assert_eq!(p.head.kind, "Read");
        assert!(p.props.is_empty());
        assert!(p.body.is_empty());
    }

    #[test]
    fn parses_props_and_capture() {
        let p = parse_pattern(r#"(op:Action :method "collect" @call)"#).unwrap();
        assert_eq!(p.props.len(), 1);
        assert_eq!(p.props[0].0, "method");
        if let Value::String(ref s) = p.props[0].1 {
            assert_eq!(s.as_ref(), "collect");
        } else {
            panic!();
        }
        assert_eq!(p.body.len(), 1);
        if let PatternBody::Capture(ref c) = p.body[0] {
            assert_eq!(c.name, "call");
        } else {
            panic!();
        }
    }

    #[test]
    fn parses_nested_pattern() {
        let src = r#"(op:Action (ast/Call :method "collect"))"#;
        let p = parse_pattern(src).unwrap();
        assert_eq!(p.body.len(), 1);
        if let PatternBody::Sub(ref inner) = p.body[0] {
            assert_eq!(inner.head.prefix, Prefix::Ast);
            assert_eq!(inner.head.kind, "Call");
        } else {
            panic!();
        }
    }

    #[test]
    fn parses_predicate_with_args() {
        let src = r#"(op:Read (#eq? @method "collect"))"#;
        let p = parse_pattern(src).unwrap();
        assert_eq!(p.body.len(), 1);
        if let PatternBody::Predicate(ref pred) = p.body[0] {
            assert_eq!(pred.name, "eq?");
            assert_eq!(pred.args.len(), 2);
            if let PredArg::Value(Value::CaptureRef(ref s)) = pred.args[0] {
                assert_eq!(s.as_ref(), "method");
            } else {
                panic!();
            }
        } else {
            panic!();
        }
    }

    #[test]
    fn parses_composite_predicate() {
        let src = r#"(op:Read
                       (#or
                         (#kind @arg "FString")
                         (#kind @arg "PercentFormat")))"#;
        let p = parse_pattern(src).unwrap();
        if let PatternBody::Predicate(ref or_pred) = p.body[0] {
            assert_eq!(or_pred.name, "or");
            assert_eq!(or_pred.args.len(), 2);
            assert!(matches!(or_pred.args[0], PredArg::Predicate(_)));
        } else {
            panic!();
        }
    }

    #[test]
    fn parses_list_literal() {
        let src = r#"(op:Read :kwargs/missing ["partitionColumn" "lowerBound"])"#;
        let p = parse_pattern(src).unwrap();
        assert_eq!(p.props[0].0, "kwargs/missing");
        if let Value::List(ref items) = p.props[0].1 {
            assert_eq!(items.len(), 2);
        } else {
            panic!();
        }
    }

    #[test]
    fn parses_size_and_duration_literals_as_values() {
        let src = r#"(op:Shuffle (#observed-bytes-gt @x 1Gi) (#slower-than @y 5s))"#;
        let p = parse_pattern(src).unwrap();
        if let PatternBody::Predicate(ref pred) = p.body[0] {
            assert!(matches!(pred.args[1], PredArg::Value(Value::Size(_))));
            if let PredArg::Value(Value::Size(b)) = pred.args[1] {
                assert_eq!(b, 1024 * 1024 * 1024);
            }
        }
        if let PatternBody::Predicate(ref pred) = p.body[1] {
            if let PredArg::Value(Value::DurationMs(ms)) = pred.args[1] {
                assert_eq!(ms, 5_000);
            }
        }
    }

    #[test]
    fn rejects_unknown_prefix() {
        let err = parse_pattern("(weird:Foo)").unwrap_err();
        assert!(matches!(err.kind, ParseErrorKind::UnknownPrefix { .. }));
    }

    #[test]
    fn rejects_mismatched_separator() {
        // `ast` uses `/`; using `:` should be flagged.
        let err = parse_pattern("(ast:Call)").unwrap_err();
        assert!(matches!(err.kind, ParseErrorKind::InvalidHead { .. }));
    }

    #[test]
    fn rejects_extra_tokens_after_pattern() {
        let err = parse_pattern("(op:Read) (op:Write)").unwrap_err();
        assert!(matches!(err.kind, ParseErrorKind::UnexpectedToken { .. }));
    }

    #[test]
    fn rejects_unbalanced_paren() {
        let err = parse_pattern("(op:Read").unwrap_err();
        assert!(matches!(err.kind, ParseErrorKind::UnbalancedOpenParen));
    }

    #[test]
    fn rejects_empty_input() {
        let err = parse_pattern("").unwrap_err();
        assert!(matches!(err.kind, ParseErrorKind::EmptyPattern));
        let err = parse_pattern("   \n  ").unwrap_err();
        assert!(matches!(err.kind, ParseErrorKind::EmptyPattern));
    }

    #[test]
    fn parses_kw_value_inside_predicate() {
        let src = r#"(op:Read (#when (#has-overlay :stage) :confidence "High"))"#;
        let p = parse_pattern(src).unwrap();
        if let PatternBody::Predicate(ref pred) = p.body[0] {
            assert_eq!(pred.name, "when");
            // args: inner predicate, then :confidence (keyword), then "High"
            assert_eq!(pred.args.len(), 3);
            assert!(matches!(pred.args[0], PredArg::Predicate(_)));
            if let PredArg::Value(Value::Ident(ref s)) = pred.args[1] {
                assert_eq!(s.as_ref(), ":confidence");
            } else {
                panic!("expected :confidence keyword");
            }
            if let PredArg::Value(Value::String(ref s)) = pred.args[2] {
                assert_eq!(s.as_ref(), "High");
            } else {
                panic!("expected \"High\" string");
            }
        }
    }

    #[test]
    fn parses_standalone_keyword_arg_in_predicate() {
        // `(#has-overlay :stage)` — bare `:stage` is one arg, no value follows.
        let src = r#"(op:Read (#has-overlay :stage))"#;
        let p = parse_pattern(src).unwrap();
        if let PatternBody::Predicate(ref pred) = p.body[0] {
            assert_eq!(pred.name, "has-overlay");
            assert_eq!(pred.args.len(), 1);
            if let PredArg::Value(Value::Ident(ref s)) = pred.args[0] {
                assert_eq!(s.as_ref(), ":stage");
            } else {
                panic!();
            }
        }
    }

    #[test]
    fn parses_bn002_style_full_pattern() {
        let src = r#"
            (op:Read
              (ast/Call :method-chain ["spark" "sql"] :arg/0 @arg)
              (#any
                (#kind @arg "FString")
                (#kind @arg "PercentFormat")
                (#kind @arg "DotFormat")
                (#kind @arg "BinaryOp")))
        "#;
        let p = parse_pattern(src).unwrap();
        assert_eq!(p.head.prefix, Prefix::Op);
        assert_eq!(p.head.kind, "Read");
        // Body has the nested ast/Call sub-pattern and the #any predicate.
        assert_eq!(p.body.len(), 2);
        assert!(matches!(p.body[0], PatternBody::Sub(_)));
        assert!(matches!(p.body[1], PatternBody::Predicate(_)));
    }
}
