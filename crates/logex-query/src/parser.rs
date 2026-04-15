use alloy_primitives::{Address, B256, keccak256};

use crate::ast::*;
use crate::lexer::Token;

/// Parser error.
#[derive(Debug, Clone, thiserror::Error)]
#[error("parse error at token {pos}: {message}")]
pub struct ParseError {
    pub pos: usize,
    pub message: String,
}

/// Recursive descent parser for LogSQL.
pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    /// Parse a full LogSQL query.
    pub fn parse_query(&mut self) -> Result<Query, ParseError> {
        self.expect(&Token::Select)?;
        let select = self.parse_select_items()?;

        self.expect(&Token::From)?;
        let table = self.expect_ident()?;
        if table.to_lowercase() != "logs" {
            return Err(self.error(format!("expected table 'logs', got '{table}'")));
        }

        let where_clause = if self.peek_is(&Token::Where) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let group_by = if self.peek_is(&Token::GroupBy) {
            self.advance();
            self.parse_ident_list()?
        } else {
            vec![]
        };

        let order_by = if self.peek_is(&Token::OrderBy) {
            self.advance();
            self.parse_order_by_items()?
        } else {
            vec![]
        };

        let limit = if self.peek_is(&Token::Limit) {
            self.advance();
            Some(self.expect_number()? as u64)
        } else {
            None
        };

        // Optional semicolon
        if self.peek_is(&Token::Semicolon) {
            self.advance();
        }

        if !self.peek_is(&Token::Eof) {
            return Err(self.error(format!("unexpected token: {:?}", self.peek())));
        }

        Ok(Query {
            select,
            where_clause,
            group_by,
            order_by,
            limit,
        })
    }

    // ---- Select items ----

    fn parse_select_items(&mut self) -> Result<Vec<SelectItem>, ParseError> {
        let mut items = Vec::new();
        items.push(self.parse_select_item()?);

        while self.peek_is(&Token::Comma) {
            self.advance();
            items.push(self.parse_select_item()?);
        }

        Ok(items)
    }

    fn parse_select_item(&mut self) -> Result<SelectItem, ParseError> {
        if self.peek_is(&Token::Star) {
            self.advance();
            return Ok(SelectItem::Star);
        }

        // Check for function call: IDENT(...)
        if let Token::Ident(name) = self.peek().clone()
            && self.pos + 1 < self.tokens.len()
            && self.tokens[self.pos + 1] == Token::LParen
        {
            self.advance(); // consume ident
            let args = self.parse_function_args()?;
            let alias = self.parse_optional_alias()?;
            return Ok(SelectItem::Function {
                name: name.to_uppercase(),
                args,
                alias,
            });
        }

        // Column reference
        let name = self.expect_ident()?;
        let alias = self.parse_optional_alias()?;
        Ok(SelectItem::Column { name, alias })
    }

    fn parse_optional_alias(&mut self) -> Result<Option<String>, ParseError> {
        if self.peek_is(&Token::As) {
            self.advance();
            Ok(Some(self.expect_ident()?))
        } else {
            Ok(None)
        }
    }

    fn parse_function_args(&mut self) -> Result<Vec<Expr>, ParseError> {
        self.expect(&Token::LParen)?;
        let mut args = Vec::new();

        if self.peek_is(&Token::Star) {
            args.push(Expr::Star);
            self.advance();
        } else if !self.peek_is(&Token::RParen) {
            args.push(self.parse_expr()?);
            while self.peek_is(&Token::Comma) {
                self.advance();
                args.push(self.parse_expr()?);
            }
        }

        self.expect(&Token::RParen)?;
        Ok(args)
    }

    // ---- Expressions (precedence climbing) ----

    fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_and()?;
        while self.peek_is(&Token::Or) {
            self.advance();
            let right = self.parse_and()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinOp::Or,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_not()?;
        while self.peek_is(&Token::And) {
            self.advance();
            let right = self.parse_not()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinOp::And,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Expr, ParseError> {
        if self.peek_is(&Token::Not) {
            self.advance();
            let expr = self.parse_comparison()?;
            Ok(Expr::Not(Box::new(expr)))
        } else {
            self.parse_comparison()
        }
    }

    fn parse_comparison(&mut self) -> Result<Expr, ParseError> {
        let left = self.parse_additive()?;

        // BETWEEN
        if self.peek_is(&Token::Between) {
            self.advance();
            let low = self.parse_additive()?;
            self.expect(&Token::And)?;
            let high = self.parse_additive()?;
            return Ok(Expr::Between {
                expr: Box::new(left),
                low: Box::new(low),
                high: Box::new(high),
            });
        }

        // IN / NOT IN
        if self.peek_is(&Token::In) {
            self.advance();
            let list = self.parse_in_list()?;
            return Ok(Expr::InList {
                expr: Box::new(left),
                list,
                negated: false,
            });
        }
        if self.peek_is(&Token::Not) {
            let saved = self.pos;
            self.advance();
            if self.peek_is(&Token::In) {
                self.advance();
                let list = self.parse_in_list()?;
                return Ok(Expr::InList {
                    expr: Box::new(left),
                    list,
                    negated: true,
                });
            }
            self.pos = saved; // backtrack
        }

        // Comparison operators
        let op = match self.peek() {
            Token::Eq => Some(BinOp::Eq),
            Token::Ne => Some(BinOp::Ne),
            Token::Lt => Some(BinOp::Lt),
            Token::Gt => Some(BinOp::Gt),
            Token::Le => Some(BinOp::Le),
            Token::Ge => Some(BinOp::Ge),
            _ => None,
        };

        if let Some(op) = op {
            self.advance();
            let right = self.parse_additive()?;
            Ok(Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        } else {
            Ok(left)
        }
    }

    fn parse_additive(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_primary()?;
        loop {
            let op = match self.peek() {
                Token::Plus => Some(BinOp::Add),
                Token::Minus => Some(BinOp::Sub),
                _ => None,
            };
            if let Some(op) = op {
                self.advance();
                let right = self.parse_primary()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_primary(&mut self) -> Result<Expr, ParseError> {
        match self.peek().clone() {
            Token::Number(n) => {
                self.advance();
                Ok(Expr::Number(n))
            }
            Token::StringLit(s) => {
                self.advance();
                Ok(Expr::StringLit(s))
            }
            Token::Event(sig) => {
                self.advance();
                let hash = keccak256(sig.as_bytes());
                Ok(Expr::EventHash(B256::from(hash)))
            }
            Token::Address(addr_str) => {
                self.advance();
                let addr = parse_address(&addr_str).map_err(|e| self.error(e))?;
                // Left-pad 20-byte address to 32 bytes for topic matching
                let mut padded = [0u8; 32];
                padded[12..].copy_from_slice(addr.as_slice());
                Ok(Expr::AddressPadded(B256::from(padded)))
            }
            Token::Latest => {
                self.advance();
                Ok(Expr::Latest)
            }
            Token::Star => {
                self.advance();
                Ok(Expr::Star)
            }
            Token::LParen => {
                self.advance();
                let expr = self.parse_expr()?;
                self.expect(&Token::RParen)?;
                Ok(expr)
            }
            Token::Ident(name) => {
                // Check for function call
                if self.pos + 1 < self.tokens.len() && self.tokens[self.pos + 1] == Token::LParen {
                    self.advance();
                    let args = self.parse_function_args()?;
                    Ok(Expr::Function {
                        name: name.to_lowercase(),
                        args,
                    })
                } else {
                    self.advance();
                    Ok(Expr::Column(name))
                }
            }
            _ => Err(self.error(format!("unexpected token: {:?}", self.peek()))),
        }
    }

    fn parse_in_list(&mut self) -> Result<Vec<Expr>, ParseError> {
        self.expect(&Token::LParen)?;
        let mut list = vec![self.parse_expr()?];
        while self.peek_is(&Token::Comma) {
            self.advance();
            list.push(self.parse_expr()?);
        }
        self.expect(&Token::RParen)?;
        Ok(list)
    }

    // ---- ORDER BY ----

    fn parse_order_by_items(&mut self) -> Result<Vec<OrderByItem>, ParseError> {
        let mut items = vec![self.parse_order_by_item()?];
        while self.peek_is(&Token::Comma) {
            self.advance();
            items.push(self.parse_order_by_item()?);
        }
        Ok(items)
    }

    fn parse_order_by_item(&mut self) -> Result<OrderByItem, ParseError> {
        let expr = self.parse_primary()?;
        let desc = if self.peek_is(&Token::Desc) {
            self.advance();
            true
        } else {
            if self.peek_is(&Token::Asc) {
                self.advance();
            }
            false
        };
        Ok(OrderByItem { expr, desc })
    }

    // ---- Helpers ----

    fn parse_ident_list(&mut self) -> Result<Vec<String>, ParseError> {
        let mut list = vec![self.expect_ident()?];
        while self.peek_is(&Token::Comma) {
            self.advance();
            list.push(self.expect_ident()?);
        }
        Ok(list)
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn peek_is(&self, expected: &Token) -> bool {
        std::mem::discriminant(self.peek()) == std::mem::discriminant(expected)
    }

    fn advance(&mut self) {
        self.pos += 1;
    }

    fn expect(&mut self, expected: &Token) -> Result<(), ParseError> {
        if self.peek_is(expected) {
            self.advance();
            Ok(())
        } else {
            Err(self.error(format!("expected {expected:?}, got {:?}", self.peek())))
        }
    }

    fn expect_ident(&mut self) -> Result<String, ParseError> {
        if let Token::Ident(name) = self.peek().clone() {
            self.advance();
            Ok(name)
        } else {
            Err(self.error(format!("expected identifier, got {:?}", self.peek())))
        }
    }

    fn expect_number(&mut self) -> Result<i64, ParseError> {
        if let Token::Number(n) = self.peek() {
            let n = *n;
            self.advance();
            Ok(n)
        } else {
            Err(self.error(format!("expected number, got {:?}", self.peek())))
        }
    }

    fn error(&self, message: String) -> ParseError {
        ParseError {
            pos: self.pos,
            message,
        }
    }
}

/// Parse a hex address string (with or without 0x prefix) into an Address.
fn parse_address(s: &str) -> Result<Address, String> {
    let hex = s.strip_prefix("0x").unwrap_or(s);
    if hex.len() != 40 {
        return Err(format!(
            "invalid address length: expected 40 hex chars, got {}",
            hex.len()
        ));
    }
    let bytes = hex::decode(hex).map_err(|e| format!("invalid hex in address: {e}"))?;
    Ok(Address::from_slice(&bytes))
}

/// High-level parse function: tokenize + parse in one step.
pub fn parse(input: &str) -> Result<Query, String> {
    let tokens = crate::lexer::tokenize(input).map_err(|e| e.to_string())?;
    let mut parser = Parser::new(tokens);
    parser.parse_query().map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_star() {
        let q = parse("SELECT * FROM logs").unwrap();
        assert_eq!(q.select, vec![SelectItem::Star]);
        assert!(q.where_clause.is_none());
        assert!(q.group_by.is_empty());
        assert!(q.order_by.is_empty());
        assert!(q.limit.is_none());
    }

    #[test]
    fn test_select_columns_with_alias() {
        let q = parse("SELECT block_number, address AS addr FROM logs").unwrap();
        assert_eq!(q.select.len(), 2);
        assert_eq!(
            q.select[0],
            SelectItem::Column {
                name: "block_number".into(),
                alias: None
            }
        );
        assert_eq!(
            q.select[1],
            SelectItem::Column {
                name: "address".into(),
                alias: Some("addr".into())
            }
        );
    }

    #[test]
    fn test_where_simple_eq() {
        let q = parse("SELECT * FROM logs WHERE block_number = 100").unwrap();
        let w = q.where_clause.unwrap();
        match w {
            Expr::BinaryOp {
                left,
                op: BinOp::Eq,
                right,
            } => {
                assert_eq!(*left, Expr::Column("block_number".into()));
                assert_eq!(*right, Expr::Number(100));
            }
            _ => panic!("expected binary eq"),
        }
    }

    #[test]
    fn test_where_and() {
        let q = parse(
            "SELECT * FROM logs WHERE address = '0xdAC17F958D2ee523a2206206994597C13D831ec7' AND block_number >= 100",
        )
        .unwrap();
        let w = q.where_clause.unwrap();
        match w {
            Expr::BinaryOp { op: BinOp::And, .. } => {}
            _ => panic!("expected AND"),
        }
    }

    #[test]
    fn test_event_literal_keccak() {
        let q = parse("SELECT * FROM logs WHERE topic0 = event'Transfer(address,address,uint256)'")
            .unwrap();
        let w = q.where_clause.unwrap();
        match w {
            Expr::BinaryOp {
                right,
                op: BinOp::Eq,
                ..
            } => {
                if let Expr::EventHash(hash) = *right {
                    // keccak256("Transfer(address,address,uint256)")
                    let expected = keccak256(b"Transfer(address,address,uint256)");
                    assert_eq!(hash, B256::from(expected));
                } else {
                    panic!("expected EventHash");
                }
            }
            _ => panic!("expected eq"),
        }
    }

    #[test]
    fn test_address_literal_padding() {
        let q = parse(
            "SELECT * FROM logs WHERE topic2 = address'0xdAC17F958D2ee523a2206206994597C13D831ec7'",
        )
        .unwrap();
        let w = q.where_clause.unwrap();
        match w {
            Expr::BinaryOp { right, .. } => {
                if let Expr::AddressPadded(b) = *right {
                    // First 12 bytes should be zero, last 20 bytes should be the address
                    assert_eq!(&b.as_slice()[..12], &[0u8; 12]);
                    assert_eq!(b.as_slice()[12], 0xDA);
                } else {
                    panic!("expected AddressPadded");
                }
            }
            _ => panic!("expected comparison"),
        }
    }

    #[test]
    fn test_latest_keyword() {
        let q = parse("SELECT * FROM logs WHERE block_number >= latest - 1000").unwrap();
        let w = q.where_clause.unwrap();
        match w {
            Expr::BinaryOp {
                right,
                op: BinOp::Ge,
                ..
            } => match *right {
                Expr::BinaryOp {
                    left,
                    op: BinOp::Sub,
                    right,
                } => {
                    assert_eq!(*left, Expr::Latest);
                    assert_eq!(*right, Expr::Number(1000));
                }
                _ => panic!("expected sub"),
            },
            _ => panic!("expected ge"),
        }
    }

    #[test]
    fn test_between() {
        let q =
            parse("SELECT * FROM logs WHERE block_number BETWEEN 18000000 AND 18100000").unwrap();
        let w = q.where_clause.unwrap();
        match w {
            Expr::Between { expr, low, high } => {
                assert_eq!(*expr, Expr::Column("block_number".into()));
                assert_eq!(*low, Expr::Number(18000000));
                assert_eq!(*high, Expr::Number(18100000));
            }
            _ => panic!("expected between"),
        }
    }

    #[test]
    fn test_group_by_order_by_limit() {
        let q = parse(
            "SELECT block_number, COUNT(*) AS event_count FROM logs WHERE address = '0xAA' GROUP BY block_number ORDER BY event_count DESC LIMIT 10",
        )
        .unwrap();
        assert_eq!(q.group_by, vec!["block_number"]);
        assert_eq!(q.order_by.len(), 1);
        assert!(q.order_by[0].desc);
        assert_eq!(q.limit, Some(10));
    }

    #[test]
    fn test_keywords_are_case_insensitive() {
        let q = parse(
            "select block_number, count(*) as total from logs group by block_number order by total desc limit 5",
        )
        .unwrap();

        assert_eq!(q.select.len(), 2);
        assert_eq!(q.group_by, vec!["block_number"]);
        assert_eq!(q.order_by.len(), 1);
        assert!(q.order_by[0].desc);
        assert_eq!(q.limit, Some(5));
    }

    #[test]
    fn test_function_in_select() {
        let q = parse("SELECT COUNT(*) AS total FROM logs").unwrap();
        match &q.select[0] {
            SelectItem::Function { name, args, alias } => {
                assert_eq!(name, "COUNT");
                assert_eq!(args, &[Expr::Star]);
                assert_eq!(alias.as_deref(), Some("total"));
            }
            _ => panic!("expected function"),
        }
    }

    #[test]
    fn test_decode_function() {
        let q = parse(
            "SELECT decode(data, 'uint256') AS value FROM logs WHERE topic0 = event'Transfer(address,address,uint256)'",
        )
        .unwrap();
        match &q.select[0] {
            SelectItem::Function { name, args, alias } => {
                assert_eq!(name, "DECODE");
                assert_eq!(args.len(), 2);
                assert_eq!(args[0], Expr::Column("data".into()));
                assert_eq!(args[1], Expr::StringLit("uint256".into()));
                assert_eq!(alias.as_deref(), Some("value"));
            }
            _ => panic!("expected function"),
        }
    }

    #[test]
    fn test_in_list() {
        let q = parse("SELECT * FROM logs WHERE source IN (0, 1)").unwrap();
        let w = q.where_clause.unwrap();
        match w {
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                assert_eq!(*expr, Expr::Column("source".into()));
                assert_eq!(list, vec![Expr::Number(0), Expr::Number(1)]);
                assert!(!negated);
            }
            _ => panic!("expected IN list"),
        }
    }

    #[test]
    fn test_full_query() {
        let input = r#"
            SELECT
                topic1 AS from_address,
                topic2 AS to_address,
                decode(data, 'uint256') AS value,
                block_number,
                timestamp
            FROM logs
            WHERE topic0 = event'Transfer(address,address,uint256)'
              AND topic1 = address'0xdAC17F958D2ee523a2206206994597C13D831ec7'
              AND block_number BETWEEN 18000000 AND 18100000
            ORDER BY block_number DESC
            LIMIT 50
        "#;
        let q = parse(input).unwrap();
        assert_eq!(q.select.len(), 5);
        assert!(q.where_clause.is_some());
        assert!(q.order_by[0].desc);
        assert_eq!(q.limit, Some(50));
    }

    #[test]
    fn test_semicolon_optional() {
        let q1 = parse("SELECT * FROM logs;").unwrap();
        let q2 = parse("SELECT * FROM logs").unwrap();
        assert_eq!(q1, q2);
    }

    #[test]
    fn test_invalid_table() {
        let err = parse("SELECT * FROM blocks").unwrap_err();
        assert!(err.contains("expected table 'logs'"));
    }

    #[test]
    fn test_comment_handling() {
        let q = parse("-- Get all logs\nSELECT * FROM logs").unwrap();
        assert_eq!(q.select, vec![SelectItem::Star]);
    }
}
