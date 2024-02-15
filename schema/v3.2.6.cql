ALTER TABLE cluster ADD force_tls_disabled BOOLEAN;
ALTER TABLE cluster ADD force_non_ssl_session_port BOOLEAN;
ALTER TABLE cluster ADD host text;