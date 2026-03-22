use serde_json::Value;

pub const ENGINE: &str = "tiflash";
pub const ADAPTER: &str = "tiflash-sql";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlExecutionPlan<Request> {
    pub request: Request,
    pub sql: String,
}

impl<Request> SqlExecutionPlan<Request> {
    pub fn new(request: Request, sql: impl Into<String>) -> Self {
        Self {
            request,
            sql: sql.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineExecutionResult {
    pub columns: Vec<EngineColumn>,
    pub rows: Vec<Vec<Value>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineColumn {
    pub name: String,
    pub engine_type: String,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineExecutionError {
    AdapterUnavailable {
        message: Option<String>,
    },
    EngineFailure {
        code: Option<String>,
        message: String,
    },
}

pub fn is_missing_column(engine_code: Option<&str>, engine_message: &str) -> bool {
    let normalized_message = engine_message.to_ascii_lowercase();

    engine_code == Some("1054")
        || normalized_message.contains("unknown column")
        || normalized_message.contains("no such column")
}

pub fn canonicalize_nested_value(value: Value) -> Value {
    match value {
        Value::Null => Value::Null,
        Value::Object(object) => {
            let mut entries: Vec<_> = object.into_iter().collect();
            entries.sort_by(|left, right| left.0.cmp(&right.0));

            let mut canonical = serde_json::Map::new();
            for (key, value) in entries {
                canonical.insert(key, canonicalize_nested_value(value));
            }

            Value::Object(canonical)
        }
        Value::Array(values) => {
            Value::Array(values.into_iter().map(canonicalize_nested_value).collect())
        }
        other => other,
    }
}

pub fn normalize_json_value(value: Value) -> Value {
    match value {
        Value::Null => Value::Null,
        Value::String(token) => match serde_json::from_str::<Value>(&token) {
            Ok(parsed) => Value::String(render_canonical_json(&parsed)),
            Err(_) => Value::String(token),
        },
        other => Value::String(render_canonical_json(&other)),
    }
}

fn render_canonical_json(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => serde_json::to_string(value).expect("string should serialize"),
        Value::Array(values) => {
            let rendered = values
                .iter()
                .map(render_canonical_json)
                .collect::<Vec<_>>()
                .join(",");
            format!("[{rendered}]")
        }
        Value::Object(values) => {
            let mut entries: Vec<_> = values.iter().collect();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            let rendered = entries
                .into_iter()
                .map(|(key, value)| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(key).expect("key should serialize"),
                        render_canonical_json(value)
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{rendered}}}")
        }
    }
}
