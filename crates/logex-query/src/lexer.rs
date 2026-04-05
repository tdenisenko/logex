/// Tokens produced by the lexer.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Select,
    From,
    Where,
    And,
    Or,
    Not,
    As,
    Between,
    In,
    GroupBy, // Emitted as a single token during lexing
    OrderBy, // Emitted as a single token during lexing
    Asc,
    Desc,
    Limit,
    Latest,

    // Identifiers and literals
    Ident(String),
    Number(i64),
    StringLit(String),

    // LogSQL extensions
    Event(String),   // event'...'
    Address(String), // address'...'

    // Symbols
    Star,
    Comma,
    LParen,
    RParen,
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
    Plus,
    Minus,
    Semicolon,

    Eof,
}

/// Lexer error.
#[derive(Debug, Clone, thiserror::Error)]
#[error("lexer error at position {pos}: {message}")]
pub struct LexError {
    pub pos: usize,
    pub message: String,
}

/// Tokenize a LogSQL input string.
pub fn tokenize(input: &str) -> Result<Vec<Token>, LexError> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        // Skip whitespace
        if chars[i].is_whitespace() {
            i += 1;
            continue;
        }

        // Skip line comments
        if i + 1 < chars.len() && chars[i] == '-' && chars[i + 1] == '-' {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }

        let start = i;

        // Symbols
        match chars[i] {
            '*' => {
                tokens.push(Token::Star);
                i += 1;
                continue;
            }
            ',' => {
                tokens.push(Token::Comma);
                i += 1;
                continue;
            }
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
                continue;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
                continue;
            }
            '+' => {
                tokens.push(Token::Plus);
                i += 1;
                continue;
            }
            '-' if i + 1 < chars.len() && chars[i + 1].is_ascii_digit() => {
                // Negative number — fall through to number parsing
            }
            '-' => {
                tokens.push(Token::Minus);
                i += 1;
                continue;
            }
            ';' => {
                tokens.push(Token::Semicolon);
                i += 1;
                continue;
            }
            '=' => {
                tokens.push(Token::Eq);
                i += 1;
                continue;
            }
            '!' if i + 1 < chars.len() && chars[i + 1] == '=' => {
                tokens.push(Token::Ne);
                i += 2;
                continue;
            }
            '<' if i + 1 < chars.len() && chars[i + 1] == '>' => {
                tokens.push(Token::Ne);
                i += 2;
                continue;
            }
            '<' if i + 1 < chars.len() && chars[i + 1] == '=' => {
                tokens.push(Token::Le);
                i += 2;
                continue;
            }
            '<' => {
                tokens.push(Token::Lt);
                i += 1;
                continue;
            }
            '>' if i + 1 < chars.len() && chars[i + 1] == '=' => {
                tokens.push(Token::Ge);
                i += 2;
                continue;
            }
            '>' => {
                tokens.push(Token::Gt);
                i += 1;
                continue;
            }
            _ => {}
        }

        // String literal: 'text'
        if chars[i] == '\'' {
            i += 1;
            let mut s = String::new();
            while i < chars.len() && chars[i] != '\'' {
                s.push(chars[i]);
                i += 1;
            }
            if i >= chars.len() {
                return Err(LexError {
                    pos: start,
                    message: "unterminated string literal".into(),
                });
            }
            i += 1; // consume closing quote
            tokens.push(Token::StringLit(s));
            continue;
        }

        // Number (possibly negative)
        if chars[i].is_ascii_digit()
            || (chars[i] == '-' && i + 1 < chars.len() && chars[i + 1].is_ascii_digit())
        {
            let mut num_str = String::new();
            if chars[i] == '-' {
                num_str.push('-');
                i += 1;
            }
            while i < chars.len() && chars[i].is_ascii_digit() {
                num_str.push(chars[i]);
                i += 1;
            }
            let n = num_str.parse::<i64>().map_err(|_| LexError {
                pos: start,
                message: format!("invalid number: {num_str}"),
            })?;
            tokens.push(Token::Number(n));
            continue;
        }

        // Identifier or keyword
        if chars[i].is_ascii_alphabetic() || chars[i] == '_' {
            let mut word = String::new();
            while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                word.push(chars[i]);
                i += 1;
            }

            let upper = word.to_uppercase();

            // Check for event'...' and address'...' extensions
            if upper == "EVENT" && i < chars.len() && chars[i] == '\'' {
                i += 1; // consume opening quote
                let mut sig = String::new();
                while i < chars.len() && chars[i] != '\'' {
                    sig.push(chars[i]);
                    i += 1;
                }
                if i >= chars.len() {
                    return Err(LexError {
                        pos: start,
                        message: "unterminated event literal".into(),
                    });
                }
                i += 1; // consume closing quote
                tokens.push(Token::Event(sig));
                continue;
            }

            if upper == "ADDRESS" && i < chars.len() && chars[i] == '\'' {
                i += 1;
                let mut addr = String::new();
                while i < chars.len() && chars[i] != '\'' {
                    addr.push(chars[i]);
                    i += 1;
                }
                if i >= chars.len() {
                    return Err(LexError {
                        pos: start,
                        message: "unterminated address literal".into(),
                    });
                }
                i += 1;
                tokens.push(Token::Address(addr));
                continue;
            }

            // Check for GROUP BY and ORDER BY (look ahead)
            if (upper == "GROUP" || upper == "ORDER") && i < chars.len() {
                let saved = i;
                // Skip whitespace
                while i < chars.len() && chars[i].is_whitespace() {
                    i += 1;
                }
                // Check for BY
                if i + 1 < chars.len() {
                    let next: String = chars[i..i + 2].iter().collect();
                    if next.to_uppercase() == "BY"
                        && (i + 2 >= chars.len() || !chars[i + 2].is_ascii_alphanumeric())
                    {
                        i += 2;
                        tokens.push(if upper == "GROUP" {
                            Token::GroupBy
                        } else {
                            Token::OrderBy
                        });
                        continue;
                    }
                }
                i = saved; // backtrack
            }

            // Keywords
            let token = match upper.as_str() {
                "SELECT" => Token::Select,
                "FROM" => Token::From,
                "WHERE" => Token::Where,
                "AND" => Token::And,
                "OR" => Token::Or,
                "NOT" => Token::Not,
                "AS" => Token::As,
                "BETWEEN" => Token::Between,
                "IN" => Token::In,
                "ASC" => Token::Asc,
                "DESC" => Token::Desc,
                "LIMIT" => Token::Limit,
                "LATEST" => Token::Latest,
                _ => Token::Ident(word),
            };
            tokens.push(token);
            continue;
        }

        return Err(LexError {
            pos: start,
            message: format!("unexpected character: '{}'", chars[i]),
        });
    }

    tokens.push(Token::Eof);
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let tokens = tokenize("SELECT * FROM logs").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Select,
                Token::Star,
                Token::From,
                Token::Ident("logs".into()),
                Token::Eof
            ]
        );
    }

    #[test]
    fn test_where_clause() {
        let tokens = tokenize("SELECT * FROM logs WHERE block_number >= 100").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Select,
                Token::Star,
                Token::From,
                Token::Ident("logs".into()),
                Token::Where,
                Token::Ident("block_number".into()),
                Token::Ge,
                Token::Number(100),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_event_literal() {
        let tokens = tokenize("WHERE topic0 = event'Transfer(address,address,uint256)'").unwrap();
        assert!(matches!(&tokens[3], Token::Event(s) if s == "Transfer(address,address,uint256)"));
    }

    #[test]
    fn test_address_literal() {
        let tokens = tokenize("WHERE topic1 = address'0xABC'").unwrap();
        assert!(matches!(&tokens[3], Token::Address(s) if s == "0xABC"));
    }

    #[test]
    fn test_latest_keyword() {
        let tokens = tokenize("WHERE block_number >= latest - 1000").unwrap();
        assert!(tokens.contains(&Token::Latest));
        assert!(tokens.contains(&Token::Minus));
        assert!(tokens.contains(&Token::Number(1000)));
    }

    #[test]
    fn test_group_by_order_by() {
        let tokens =
            tokenize("SELECT block_number FROM logs GROUP BY block_number ORDER BY block_number DESC LIMIT 10").unwrap();
        assert!(tokens.contains(&Token::GroupBy));
        assert!(tokens.contains(&Token::OrderBy));
        assert!(tokens.contains(&Token::Desc));
        assert!(tokens.contains(&Token::Limit));
    }

    #[test]
    fn test_string_literal() {
        let tokens =
            tokenize("WHERE address = '0xdAC17F958D2ee523a2206206994597C13D831ec7'").unwrap();
        assert!(matches!(
            &tokens[3],
            Token::StringLit(s) if s == "0xdAC17F958D2ee523a2206206994597C13D831ec7"
        ));
    }

    #[test]
    fn test_line_comment() {
        let tokens = tokenize("SELECT * -- this is a comment\nFROM logs").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Select,
                Token::Star,
                Token::From,
                Token::Ident("logs".into()),
                Token::Eof
            ]
        );
    }

    #[test]
    fn test_not_equal() {
        let t1 = tokenize("a != b").unwrap();
        assert!(t1.contains(&Token::Ne));

        let t2 = tokenize("a <> b").unwrap();
        assert!(t2.contains(&Token::Ne));
    }
}
