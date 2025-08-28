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
            CommandType::Text => (self.text.clone(), params),
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
