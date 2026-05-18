//! Tokenizer for the graph-query DSL.
//!
//! Hand-rolled scanner — no external crate. Streams `Token`s with
//! 1-indexed positions so `ParseError` can point at the offending source
//! location. Whitespace and `;`-style line comments are skipped.

use std::sync::Arc;

use crate::rules::graph_dsl::error::{ParseError, ParseErrorKind};

#[derive(Debug, Clone, PartialEq)]
pub(super) enum TokenKind {
    /// `(` — opens a pattern or predicate.
    LParen,
    /// `)` — closes a pattern or predicate.
    RParen,
    /// `[` — opens a literal list.
    LBracket,
    /// `]` — closes a literal list.
    RBracket,
    /// `@name` — capture or capture reference. Bare name without the `@`.
    At(Arc<str>),
    /// `#name` — predicate head. Bare name without the `#`.
    Hash(Arc<str>),
    /// `:name` — property keyword. Bare name without the `:`.
    Colon(Arc<str>),
    /// `"…"` — string literal.
    String(Arc<str>),
    /// Bare identifier. Heads like `op:Read` arrive as one token of the
    /// form `"op:Read"` (the parser splits on the separator) so we don't
    /// have to distinguish them in the lexer.
    Ident(Arc<str>),
    /// Numeric literal — integer, float, size (`1Gi`), or duration (`5ms`).
    /// Parsed eagerly so the parser doesn't have to redo the work.
    Number(f64),
    /// Size literal in bytes — `1B`, `1KiB`, `1MiB`, `1GiB`, `1Gi`, `1MB`,
    /// `1KB`, `1TB`, etc.
    Size(u64),
    /// Duration literal in milliseconds — `100ms`, `5s`, `2m`, `1h`.
    DurationMs(u64),
    /// `true` / `false` keyword.
    Bool(bool),
}

#[derive(Debug, Clone)]
pub(super) struct Token {
    pub kind: TokenKind,
    pub line: u32,
    pub column: u32,
}

pub(super) struct Lexer<'a> {
    src: &'a [u8],
    pos: usize,
    line: u32,
    column: u32,
}

impl<'a> Lexer<'a> {
    pub fn new(src: &'a str) -> Self {
        Self {
            src: src.as_bytes(),
            pos: 0,
            line: 1,
            column: 1,
        }
    }

    pub fn tokenize(mut self) -> Result<Vec<Token>, ParseError> {
        let mut tokens = Vec::new();
        while let Some(tok) = self.next_token()? {
            tokens.push(tok);
        }
        Ok(tokens)
    }

    fn next_token(&mut self) -> Result<Option<Token>, ParseError> {
        loop {
            self.skip_ws_and_comments();
            let Some(c) = self.peek() else {
                return Ok(None);
            };
            let (line, column) = (self.line, self.column);
            match c {
                b'(' => {
                    self.advance();
                    return Ok(Some(Token {
                        kind: TokenKind::LParen,
                        line,
                        column,
                    }));
                }
                b')' => {
                    self.advance();
                    return Ok(Some(Token {
                        kind: TokenKind::RParen,
                        line,
                        column,
                    }));
                }
                b'[' => {
                    self.advance();
                    return Ok(Some(Token {
                        kind: TokenKind::LBracket,
                        line,
                        column,
                    }));
                }
                b']' => {
                    self.advance();
                    return Ok(Some(Token {
                        kind: TokenKind::RBracket,
                        line,
                        column,
                    }));
                }
                b'@' => {
                    self.advance();
                    let name = self.read_ident();
                    return Ok(Some(Token {
                        kind: TokenKind::At(Arc::from(name)),
                        line,
                        column,
                    }));
                }
                b'#' => {
                    self.advance();
                    let name = self.read_ident();
                    return Ok(Some(Token {
                        kind: TokenKind::Hash(Arc::from(name)),
                        line,
                        column,
                    }));
                }
                b':' => {
                    self.advance();
                    let name = self.read_ident();
                    return Ok(Some(Token {
                        kind: TokenKind::Colon(Arc::from(name)),
                        line,
                        column,
                    }));
                }
                b'"' => {
                    let s = self.read_string(line, column)?;
                    return Ok(Some(Token {
                        kind: TokenKind::String(Arc::from(s)),
                        line,
                        column,
                    }));
                }
                b'-' | b'0'..=b'9' => {
                    let token = self.read_number_or_size(line, column)?;
                    return Ok(Some(token));
                }
                c if is_ident_start(c) => {
                    let name = self.read_ident();
                    let kind = match name.as_str() {
                        "true" => TokenKind::Bool(true),
                        "false" => TokenKind::Bool(false),
                        _ => TokenKind::Ident(Arc::from(name)),
                    };
                    return Ok(Some(Token { kind, line, column }));
                }
                _ => {
                    return Err(ParseError::new(
                        ParseErrorKind::UnexpectedToken {
                            token: (c as char).to_string(),
                        },
                        line,
                        column,
                    ));
                }
            }
        }
    }

    fn skip_ws_and_comments(&mut self) {
        while let Some(c) = self.peek() {
            match c {
                b' ' | b'\t' | b'\r' => self.advance(),
                b'\n' => self.advance(),
                b';' => {
                    while let Some(c2) = self.peek() {
                        if c2 == b'\n' {
                            break;
                        }
                        self.advance();
                    }
                }
                _ => return,
            }
        }
    }

    fn peek(&self) -> Option<u8> {
        self.src.get(self.pos).copied()
    }

    fn advance(&mut self) {
        if let Some(c) = self.peek() {
            self.pos += 1;
            if c == b'\n' {
                self.line += 1;
                self.column = 1;
            } else {
                self.column += 1;
            }
        }
    }

    fn read_ident(&mut self) -> String {
        let start = self.pos;
        while let Some(c) = self.peek() {
            if is_ident_cont(c) {
                self.advance();
            } else {
                break;
            }
        }
        // SAFETY: we only consumed ASCII / ident bytes.
        String::from_utf8_lossy(&self.src[start..self.pos]).into_owned()
    }

    fn read_string(&mut self, line: u32, column: u32) -> Result<String, ParseError> {
        self.advance(); // consume opening "
        let mut out = String::new();
        loop {
            let Some(c) = self.peek() else {
                return Err(ParseError::new(
                    ParseErrorKind::UnterminatedString,
                    line,
                    column,
                ));
            };
            match c {
                b'"' => {
                    self.advance();
                    return Ok(out);
                }
                b'\\' => {
                    self.advance();
                    match self.peek() {
                        Some(b'n') => {
                            out.push('\n');
                            self.advance();
                        }
                        Some(b't') => {
                            out.push('\t');
                            self.advance();
                        }
                        Some(b'r') => {
                            out.push('\r');
                            self.advance();
                        }
                        Some(b'\\') => {
                            out.push('\\');
                            self.advance();
                        }
                        Some(b'"') => {
                            out.push('"');
                            self.advance();
                        }
                        _ => {
                            // Unknown escape — keep the backslash literally
                            // so the matcher can use it as a regex
                            // backref. `(`#match? @x "\\d+")` is a real use.
                            out.push('\\');
                            if let Some(c2) = self.peek() {
                                out.push(c2 as char);
                                self.advance();
                            }
                        }
                    }
                }
                b'\n' => {
                    return Err(ParseError::new(
                        ParseErrorKind::UnterminatedString,
                        line,
                        column,
                    ));
                }
                _ => {
                    out.push(c as char);
                    self.advance();
                }
            }
        }
    }

    fn read_number_or_size(&mut self, line: u32, column: u32) -> Result<Token, ParseError> {
        let start = self.pos;
        // optional leading '-'
        if self.peek() == Some(b'-') {
            self.advance();
        }
        let mut saw_dot = false;
        while let Some(c) = self.peek() {
            match c {
                b'0'..=b'9' => self.advance(),
                b'.' if !saw_dot => {
                    saw_dot = true;
                    self.advance();
                }
                _ => break,
            }
        }
        let num_end = self.pos;

        // Suffix span — letters that may form a size or duration unit.
        while let Some(c) = self.peek() {
            if c.is_ascii_alphabetic() {
                self.advance();
            } else {
                break;
            }
        }
        let suffix_end = self.pos;
        let token = String::from_utf8_lossy(&self.src[start..suffix_end]).into_owned();
        let num_str = String::from_utf8_lossy(&self.src[start..num_end]).into_owned();
        let suffix = String::from_utf8_lossy(&self.src[num_end..suffix_end]).into_owned();

        if suffix.is_empty() {
            let n: f64 = num_str.parse().map_err(|_| {
                ParseError::new(
                    ParseErrorKind::InvalidNumber {
                        token: token.clone(),
                    },
                    line,
                    column,
                )
            })?;
            return Ok(Token {
                kind: TokenKind::Number(n),
                line,
                column,
            });
        }

        if let Some(bytes) = parse_size_suffix(&num_str, &suffix) {
            return Ok(Token {
                kind: TokenKind::Size(bytes),
                line,
                column,
            });
        }
        if let Some(ms) = parse_duration_suffix(&num_str, &suffix) {
            return Ok(Token {
                kind: TokenKind::DurationMs(ms),
                line,
                column,
            });
        }
        // Bare number followed by an unrelated identifier — surface as
        // an unknown suffix on the size form (the more common case) so
        // the error is useful.
        if suffix.chars().all(|c| {
            "BKMGTPibB".contains(c) || c.is_ascii_alphabetic()
        }) && suffix.len() <= 3
        {
            Err(ParseError::new(
                ParseErrorKind::InvalidSize { token },
                line,
                column,
            ))
        } else {
            Err(ParseError::new(
                ParseErrorKind::InvalidDuration { token },
                line,
                column,
            ))
        }
    }
}

fn is_ident_start(c: u8) -> bool {
    matches!(c, b'a'..=b'z' | b'A'..=b'Z' | b'_')
}

fn is_ident_cont(c: u8) -> bool {
    matches!(
        c,
        b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_' | b':' | b'/' | b'-' | b'.' | b'?' | b'!'
    )
}

fn parse_size_suffix(num: &str, suffix: &str) -> Option<u64> {
    let n: f64 = num.parse().ok()?;
    let factor: u64 = match suffix {
        "B" => 1,
        "KiB" | "Ki" => 1 << 10,
        "MiB" | "Mi" => 1 << 20,
        "GiB" | "Gi" => 1 << 30,
        "TiB" | "Ti" => 1u64 << 40,
        "PiB" | "Pi" => 1u64 << 50,
        "KB" => 1_000,
        "MB" => 1_000_000,
        "GB" => 1_000_000_000,
        "TB" => 1_000_000_000_000,
        "PB" => 1_000_000_000_000_000,
        _ => return None,
    };
    Some((n * factor as f64).round() as u64)
}

fn parse_duration_suffix(num: &str, suffix: &str) -> Option<u64> {
    let n: f64 = num.parse().ok()?;
    let factor: u64 = match suffix {
        "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        _ => return None,
    };
    Some((n * factor as f64).round() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lex(src: &str) -> Result<Vec<TokenKind>, ParseError> {
        Lexer::new(src)
            .tokenize()
            .map(|toks| toks.into_iter().map(|t| t.kind).collect())
    }

    #[test]
    fn parens_and_brackets() {
        let toks = lex("( ) [ ]").unwrap();
        assert_eq!(
            toks,
            vec![
                TokenKind::LParen,
                TokenKind::RParen,
                TokenKind::LBracket,
                TokenKind::RBracket,
            ]
        );
    }

    #[test]
    fn capture_predicate_keyword() {
        let toks = lex("@x #eq? :method").unwrap();
        assert!(matches!(toks[0], TokenKind::At(_)));
        assert!(matches!(toks[1], TokenKind::Hash(_)));
        assert!(matches!(toks[2], TokenKind::Colon(_)));
        if let TokenKind::At(ref s) = toks[0] {
            assert_eq!(s.as_ref(), "x");
        }
        if let TokenKind::Hash(ref s) = toks[1] {
            assert_eq!(s.as_ref(), "eq?");
        }
        if let TokenKind::Colon(ref s) = toks[2] {
            assert_eq!(s.as_ref(), "method");
        }
    }

    #[test]
    fn string_with_escapes() {
        let toks = lex(r#""hello\nworld""#).unwrap();
        if let TokenKind::String(ref s) = toks[0] {
            assert_eq!(s.as_ref(), "hello\nworld");
        } else {
            panic!("expected string, got {:?}", toks[0]);
        }
    }

    #[test]
    fn regex_string_keeps_backslash_d() {
        let toks = lex(r#""\d+""#).unwrap();
        if let TokenKind::String(ref s) = toks[0] {
            assert_eq!(s.as_ref(), r"\d+");
        } else {
            panic!();
        }
    }

    #[test]
    fn unterminated_string_errors() {
        let err = lex(r#""never closed"#).unwrap_err();
        assert!(matches!(err.kind, ParseErrorKind::UnterminatedString));
    }

    #[test]
    fn numbers_floats_negatives() {
        let toks = lex("0 42 -7 3.14").unwrap();
        assert_eq!(
            toks,
            vec![
                TokenKind::Number(0.0),
                TokenKind::Number(42.0),
                TokenKind::Number(-7.0),
                TokenKind::Number(3.14),
            ]
        );
    }

    #[test]
    fn size_literals_parse_to_bytes() {
        let toks = lex("1B 1KiB 1MiB 1Gi 1MB 1GB").unwrap();
        assert_eq!(
            toks,
            vec![
                TokenKind::Size(1),
                TokenKind::Size(1024),
                TokenKind::Size(1024 * 1024),
                TokenKind::Size(1024 * 1024 * 1024),
                TokenKind::Size(1_000_000),
                TokenKind::Size(1_000_000_000),
            ]
        );
    }

    #[test]
    fn duration_literals_parse_to_ms() {
        let toks = lex("100ms 5s 2m 1h").unwrap();
        assert_eq!(
            toks,
            vec![
                TokenKind::DurationMs(100),
                TokenKind::DurationMs(5_000),
                TokenKind::DurationMs(120_000),
                TokenKind::DurationMs(3_600_000),
            ]
        );
    }

    #[test]
    fn idents_and_heads() {
        let toks = lex("op:Read ast/Call").unwrap();
        if let TokenKind::Ident(ref s) = toks[0] {
            assert_eq!(s.as_ref(), "op:Read");
        }
        if let TokenKind::Ident(ref s) = toks[1] {
            assert_eq!(s.as_ref(), "ast/Call");
        }
    }

    #[test]
    fn bool_keywords() {
        let toks = lex("true false").unwrap();
        assert_eq!(toks, vec![TokenKind::Bool(true), TokenKind::Bool(false)]);
    }

    #[test]
    fn line_comments_skipped() {
        let toks = lex("; this is a comment\n42").unwrap();
        assert_eq!(toks, vec![TokenKind::Number(42.0)]);
    }

    #[test]
    fn position_tracking_lines_columns() {
        let toks = Lexer::new("(\n  op:Read)").tokenize().unwrap();
        assert_eq!(toks[0].line, 1);
        assert_eq!(toks[0].column, 1);
        assert_eq!(toks[1].line, 2);
        assert_eq!(toks[1].column, 3);
    }
}
