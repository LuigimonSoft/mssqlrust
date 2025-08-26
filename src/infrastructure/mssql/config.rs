use tiberius::{AuthMethod, Config};

#[derive(Debug, Clone)]
pub struct MssqlConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: String,
    pub trust_cert: bool,
}

impl MssqlConfig {
    pub fn new(
        host: &str,
        port: u16,
        username: &str,
        password: &str,
        database: &str,
        trust_cert: bool,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            username: username.into(),
            password: password.into(),
            database: database.into(),
            trust_cert,
        }
    }

    pub fn to_config(&self) -> Config {
        let mut cfg = Config::new();
        cfg.host(&self.host);
        cfg.port(self.port);
        cfg.database(&self.database);
        cfg.authentication(AuthMethod::sql_server(&self.username, &self.password));
        if self.trust_cert {
            cfg.trust_cert();
        }
        cfg
    }
}
