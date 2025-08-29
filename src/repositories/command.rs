use super::parameter::Parameter;

pub enum CommandType {
    Text,
    StoredProcedure,
}

pub struct Command {
    pub text: String,
    pub command_type: CommandType,
    pub parameters: Vec<Parameter>,
}

impl Command {
    pub fn query(text: &str) -> Self {
        Self {
            text: text.into(),
            command_type: CommandType::Text,
            parameters: Vec::new(),
        }
    }

    pub fn stored_procedure(name: &str) -> Self {
        Self {
            text: name.into(),
            command_type: CommandType::StoredProcedure,
            parameters: Vec::new(),
        }
    }

    pub fn with_param(mut self, param: Parameter) -> Self {
        self.parameters.push(param);
        self
    }

    pub fn build(&self) -> (String, Vec<Box<dyn tiberius::ToSql + Send + Sync>>) {
        let params: Vec<Box<dyn tiberius::ToSql + Send + Sync>> = self
            .parameters
            .iter()
            .map(|p| p.value.to_tiberius())
            .collect();
        match self.command_type {
            CommandType::Text => {
                // Allow using named parameters (e.g., @id) in text queries by rewriting
                // them to positional placeholders (@P1, @P2, ...), which Tiberius expects.
                let mut sql = self.text.clone();
                for (i, p) in self.parameters.iter().enumerate() {
                    let name = p.name.as_str();
                    let trimmed = if name.starts_with('@') { &name[1..] } else { name };
                    // If the user already uses ordinal placeholders (P1, P2, ...), skip rewrite.
                    if trimmed.len() >= 2
                        && trimmed.as_bytes()[0] == b'P'
                        && trimmed[1..].bytes().all(|b| b.is_ascii_digit())
                    {
                        continue;
                    }
                    let needle = format!("@{}", trimmed);
                    let replacement = format!("@P{}", i + 1);
                    sql = replace_param_token(&sql, &needle, &replacement);
                }
                (sql, params)
            }
            CommandType::StoredProcedure => {
                let mut sql = format!("EXEC {}", self.text);
                if !self.parameters.is_empty() {
                    let param_str = self
                        .parameters
                        .iter()
                        .enumerate()
                        .map(|(i, p)| {
                            let name = if p.name.starts_with("@") {
                                p.name.trim_start_matches('@')
                            } else {
                                p.name.as_str()
                            };
                            format!("@{} = @P{}", name, i + 1)
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    sql.push(' ');
                    sql.push_str(&param_str);
                }
                (sql, params)
            }
        }
    }
}

// Replace all occurrences of `needle` in `haystack` that end at an identifier boundary
// (i.e., the character following the match is not [A-Za-z0-9_]). This prevents replacing
// `@id` inside `@id2`.
fn replace_param_token(haystack: &str, needle: &str, replacement: &str) -> String {
    let bytes = haystack.as_bytes();
    let nbytes = needle.as_bytes();
    let mut i = 0;
    let mut out = String::with_capacity(haystack.len());
    while i < bytes.len() {
        if i + nbytes.len() <= bytes.len() && &bytes[i..i + nbytes.len()] == nbytes {
            // Boundary check: next char must be absent or not [A-Za-z0-9_]
            let boundary_ok = match bytes.get(i + nbytes.len()) {
                None => true,
                Some(&c) => {
                    !(c.is_ascii_alphanumeric() || c == b'_')
                }
            };
            if boundary_ok {
                out.push_str(replacement);
                i += nbytes.len();
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}
