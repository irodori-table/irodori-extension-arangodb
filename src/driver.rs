use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use reqwest::{Client, RequestBuilder};
use serde_json::{json, Map, Value};
use tokio::runtime::Runtime;

use crate::abi::{self, IrodoriConnectorBuffer};
use crate::{ABI_VERSION, CONFIG_JSON, DRIVER_LINKED, ENGINE, MANIFEST_JSON};

static CONNECTIONS: OnceLock<Mutex<HashMap<String, ArangoConnection>>> = OnceLock::new();
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

#[derive(Clone)]
struct ArangoConnection {
    client: Client,
    config: ArangoConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ArangoConfig {
    base_url: String,
    database: String,
    username: Option<String>,
    password: Option<String>,
    bearer_token: Option<String>,
    redaction_values: Vec<String>,
}

type QueryOutput = (Vec<String>, Vec<Vec<Value>>, bool);

fn connections() -> &'static Mutex<HashMap<String, ArangoConnection>> {
    CONNECTIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn runtime() -> Result<&'static Runtime, String> {
    if let Some(runtime) = RUNTIME.get() {
        return Ok(runtime);
    }
    let runtime = Runtime::new().map_err(|err| format!("create tokio runtime failed: {err}"))?;
    let _ = RUNTIME.set(runtime);
    RUNTIME
        .get()
        .ok_or_else(|| "create tokio runtime failed.".to_string())
}

pub fn call_json(request: IrodoriConnectorBuffer) -> IrodoriConnectorBuffer {
    let request = match abi::parse_request(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let method = match abi::request_method(request.as_ref()) {
        Ok(method) => method,
        Err(response) => return response,
    };

    match method {
        "health" | "ping" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
        ])),
        "describe" | "capabilities" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
            (
                "manifest".to_string(),
                serde_json::from_str(MANIFEST_JSON).unwrap_or(Value::Null),
            ),
            (
                "config".to_string(),
                serde_json::from_str(CONFIG_JSON).unwrap_or(Value::Null),
            ),
        ])),
        "manifest" => abi::owned_buffer(MANIFEST_JSON.to_string()),
        "config" => abi::owned_buffer(CONFIG_JSON.to_string()),
        "connect" => connect(request.as_ref().expect("connect has request")),
        "query" => query(request.as_ref().expect("query has request")),
        "metadata" => metadata(request.as_ref().expect("metadata has request")),
        "close" => close(request.as_ref().expect("close has request")),
        other => abi::error(
            "connector.unknownMethod",
            format!("unknown connector method: {other}"),
        ),
    }
}

fn connect(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let config = match ArangoConfig::from_request(request) {
        Ok(config) => config,
        Err(err) => return abi::error("connector.invalidRequest", err),
    };
    let connection = ArangoConnection {
        client: Client::new(),
        config,
    };
    let version = match runtime().and_then(|runtime| runtime.block_on(load_version(&connection))) {
        Ok(version) => version,
        Err(err) => return abi::error("connector.connectFailed", connection.config.redact(&err)),
    };
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let response = Map::from_iter([
        ("engine".to_string(), Value::String(ENGINE.to_string())),
        (
            "connectionId".to_string(),
            Value::String(connection_id.clone()),
        ),
        ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
        (
            "endpoint".to_string(),
            Value::String(connection.config.base_url.clone()),
        ),
        (
            "database".to_string(),
            Value::String(connection.config.database.clone()),
        ),
        ("serverVersion".to_string(), Value::String(version)),
    ]);
    guard.insert(connection_id, connection);
    abi::ok(response)
}

fn query(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let Some(statement) = abi::string_field(request, "aql")
        .or_else(|| abi::string_field(request, "query"))
        .or_else(|| abi::string_field(request, "sql"))
        .or_else(|| abi::string_field(request, "statement"))
    else {
        return abi::error(
            "connector.invalidRequest",
            "query requires a string aql, query, sql, or statement field.",
        );
    };
    let connection = match connection(&connection_id) {
        Ok(connection) => connection,
        Err(response) => return response,
    };
    match runtime().and_then(|runtime| {
        runtime.block_on(run_aql(&connection, statement, abi::max_rows(request)))
    }) {
        Ok((columns, rows, truncated)) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            (
                "columns".to_string(),
                Value::Array(columns.into_iter().map(Value::String).collect()),
            ),
            (
                "rows".to_string(),
                Value::Array(rows.into_iter().map(Value::Array).collect()),
            ),
            ("truncated".to_string(), Value::Bool(truncated)),
        ])),
        Err(err) => abi::error("connector.queryFailed", connection.config.redact(&err)),
    }
}

fn metadata(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let connection = match connection(&connection_id) {
        Ok(connection) => connection,
        Err(response) => return response,
    };
    match runtime().and_then(|runtime| runtime.block_on(load_metadata(&connection))) {
        Ok(metadata) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            ("metadata".to_string(), metadata),
        ])),
        Err(err) => abi::error("connector.metadataFailed", connection.config.redact(&err)),
    }
}

fn close(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let existed = guard.remove(&connection_id).is_some();
    abi::ok(Map::from_iter([
        ("connectionId".to_string(), Value::String(connection_id)),
        ("closed".to_string(), Value::Bool(existed)),
    ]))
}

impl ArangoConnection {
    fn auth(&self, builder: RequestBuilder) -> RequestBuilder {
        if let Some(token) = self.config.bearer_token.as_deref() {
            builder.bearer_auth(token)
        } else if let Some(username) = self.config.username.as_deref() {
            builder.basic_auth(username, self.config.password.as_deref())
        } else {
            builder
        }
    }

    fn db_url(&self, path: &str) -> String {
        format!(
            "{}/_db/{}/{}",
            self.config.base_url,
            url_component(&self.config.database),
            path.trim_start_matches('/')
        )
    }
}

impl ArangoConfig {
    fn from_request(request: &Value) -> Result<Self, String> {
        let base_url = option_string(request, &["connectionString", "url", "dsn"])
            .unwrap_or_else(|| build_url(request));
        let database =
            option_string(request, &["database", "db"]).unwrap_or_else(|| "_system".to_string());
        let username = option_string(request, &["user", "username"]);
        let password = option_string(request, &["password"]);
        let bearer_token = option_string(request, &["token", "jwt", "bearerToken"]);
        let mut redaction_values = Vec::new();
        push_sensitive(&mut redaction_values, password.as_deref());
        push_sensitive(&mut redaction_values, bearer_token.as_deref());
        collect_url_auth(&base_url, &mut redaction_values);
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            database,
            username,
            password,
            bearer_token,
            redaction_values,
        })
    }

    fn redact(&self, message: &str) -> String {
        self.redaction_values.iter().fold(
            message.replace(&self.base_url, "<arangodb-url>"),
            |message, secret| {
                if secret.is_empty() {
                    message
                } else {
                    message.replace(secret, "****")
                }
            },
        )
    }
}

async fn load_version(connection: &ArangoConnection) -> Result<String, String> {
    let value = get_json(connection, "/_api/version").await?;
    Ok(value
        .get("version")
        .and_then(Value::as_str)
        .map(|version| format!("ArangoDB {version}"))
        .unwrap_or_else(|| "ArangoDB".to_string()))
}

async fn run_aql(
    connection: &ArangoConnection,
    statement: &str,
    cap: usize,
) -> Result<QueryOutput, String> {
    let response = connection
        .auth(connection.client.post(connection.db_url("/_api/cursor")))
        .json(&json!({
            "query": statement,
            "batchSize": cap,
            "count": true
        }))
        .send()
        .await
        .map_err(|err| format!("ArangoDB cursor request failed: {err}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| format!("ArangoDB cursor response read failed: {err}"))?;
    if !status.is_success() {
        return Err(format!("ArangoDB cursor returned HTTP {status}: {text}"));
    }
    let value = serde_json::from_str::<Value>(&text)
        .map_err(|err| format!("ArangoDB cursor JSON failed: {err}: {text}"))?;
    Ok(rows_from_result(
        value
            .get("result")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
        value
            .get("hasMore")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    ))
}

async fn load_metadata(connection: &ArangoConnection) -> Result<Value, String> {
    let value = get_db_json(connection, "/_api/collection?excludeSystem=true").await?;
    let collections = value
        .get("result")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let objects = collections
        .into_iter()
        .filter_map(|collection| {
            let name = collection.get("name").and_then(Value::as_str)?;
            Some(json!({
                "schema": connection.config.database,
                "name": name,
                "kind": if collection.get("type").and_then(Value::as_i64) == Some(3) { "edgeCollection" } else { "documentCollection" },
                "columns": [
                    {"name": "_key", "dataType": "string", "nullable": false, "ordinal": 1},
                    {"name": "_id", "dataType": "string", "nullable": false, "ordinal": 2},
                    {"name": "_rev", "dataType": "string", "nullable": false, "ordinal": 3},
                    {"name": "document", "dataType": "json", "nullable": true, "ordinal": 4}
                ],
                "indexes": [],
                "primaryKey": [{"name": "_key", "keyType": "primary"}],
                "foreignKeys": [],
                "details": collection
            }))
        })
        .collect::<Vec<_>>();
    Ok(json!({ "schemas": [{ "name": connection.config.database, "objects": objects }] }))
}

async fn get_json(connection: &ArangoConnection, path: &str) -> Result<Value, String> {
    let response = connection
        .auth(
            connection
                .client
                .get(format!("{}{}", connection.config.base_url, path)),
        )
        .send()
        .await
        .map_err(|err| format!("ArangoDB request failed: {err}"))?;
    json_response(response).await
}

async fn get_db_json(connection: &ArangoConnection, path: &str) -> Result<Value, String> {
    let response = connection
        .auth(connection.client.get(connection.db_url(path)))
        .send()
        .await
        .map_err(|err| format!("ArangoDB request failed: {err}"))?;
    json_response(response).await
}

async fn json_response(response: reqwest::Response) -> Result<Value, String> {
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| format!("ArangoDB response read failed: {err}"))?;
    if !status.is_success() {
        return Err(format!("ArangoDB returned HTTP {status}: {text}"));
    }
    serde_json::from_str::<Value>(&text)
        .map_err(|err| format!("ArangoDB JSON parse failed: {err}: {text}"))
}

fn rows_from_result(items: Vec<Value>, truncated: bool) -> QueryOutput {
    let mut columns = Vec::new();
    for item in &items {
        if let Some(object) = item.as_object() {
            for key in object.keys() {
                if !columns.iter().any(|column| column == key) {
                    columns.push(key.clone());
                }
            }
        }
    }
    if columns.is_empty() && !items.is_empty() {
        columns.push("value".to_string());
    }
    let rows = items
        .iter()
        .map(|item| {
            if let Some(object) = item.as_object() {
                columns
                    .iter()
                    .map(|column| object.get(column).cloned().unwrap_or(Value::Null))
                    .collect()
            } else {
                vec![item.clone()]
            }
        })
        .collect();
    (columns, rows, truncated)
}

fn build_url(request: &Value) -> String {
    let host = option_string(request, &["host", "endpoint"]).unwrap_or_else(|| "127.0.0.1".into());
    let port = option_string(request, &["port"]).unwrap_or_else(|| "8529".into());
    let scheme = if bool_option(request, &["tls", "ssl"]).unwrap_or(false) {
        "https"
    } else {
        "http"
    };
    format!("{scheme}://{host}:{port}")
}

fn connection(connection_id: &str) -> Result<ArangoConnection, IrodoriConnectorBuffer> {
    let guard = connections().lock().map_err(|_| {
        abi::error(
            "connector.statePoisoned",
            "Connector connection state is poisoned.",
        )
    })?;
    guard.get(connection_id).cloned().ok_or_else(|| {
        abi::error(
            "connector.connectionNotFound",
            format!("no open connection: {connection_id}"),
        )
    })
}

fn request_containers(request: &Value) -> Vec<&Value> {
    [
        Some(request),
        request.get("profile"),
        request.get("options"),
        request.get("auth"),
        request.get("secrets"),
        request
            .get("profile")
            .and_then(|profile| profile.get("options")),
        request
            .get("profile")
            .and_then(|profile| profile.get("auth")),
        request
            .get("profile")
            .and_then(|profile| profile.get("secrets")),
    ]
    .into_iter()
    .flatten()
    .collect()
}

fn option_string(request: &Value, fields: &[&str]) -> Option<String> {
    request_containers(request)
        .into_iter()
        .find_map(|container| {
            fields.iter().find_map(|field| {
                container
                    .get(*field)
                    .map(|value| match value {
                        Value::String(value) => value.clone(),
                        Value::Number(value) => value.to_string(),
                        Value::Bool(value) => value.to_string(),
                        _ => String::new(),
                    })
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
            })
        })
}

fn bool_option(request: &Value, fields: &[&str]) -> Option<bool> {
    request_containers(request)
        .into_iter()
        .find_map(|container| {
            fields
                .iter()
                .find_map(|field| container.get(*field).and_then(Value::as_bool))
        })
}

fn url_component(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![byte as char]
            }
            _ => format!("%{byte:02X}").chars().collect(),
        })
        .collect()
}

fn push_sensitive(values: &mut Vec<String>, value: Option<&str>) {
    if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
        if !values.iter().any(|existing| existing == value) {
            values.push(value.to_string());
        }
    }
}

fn collect_url_auth(url: &str, values: &mut Vec<String>) {
    let Some(after_scheme) = url.split_once("://").map(|(_, rest)| rest) else {
        return;
    };
    let Some(auth) = after_scheme
        .split('/')
        .next()
        .and_then(|host| host.split('@').next())
    else {
        return;
    };
    if auth.contains(':') {
        for part in auth.split(':') {
            push_sensitive(values, Some(part));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_from_objects() {
        let (columns, rows, truncated) = rows_from_result(
            vec![json!({"a": 1, "b": "x"}), json!({"b": "y", "c": true})],
            false,
        );
        assert_eq!(columns, vec!["a", "b", "c"]);
        assert_eq!(rows[0], vec![json!(1), json!("x"), Value::Null]);
        assert_eq!(rows[1], vec![Value::Null, json!("y"), json!(true)]);
        assert!(!truncated);
    }

    #[test]
    fn builds_config_from_profile() {
        let config = ArangoConfig::from_request(&json!({
            "profile": {
                "host": "arango.local",
                "port": 8530,
                "database": "app",
                "user": "root",
                "password": "secret"
            }
        }))
        .unwrap();
        assert_eq!(config.base_url, "http://arango.local:8530");
        assert_eq!(config.database, "app");
    }
}
