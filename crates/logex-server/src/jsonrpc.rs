use serde::de::{IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value, value::RawValue};

/// An HTTP JSON-RPC document. Nonempty batches remain unsupported.
#[derive(Debug)]
pub enum JsonRpcDocument {
    Request(JsonRpcRequest),
    InvalidRequest,
    UnsupportedBatch,
}

/// A structurally validated request. Missing ID means notification, not a null ID.
#[derive(Debug)]
pub struct JsonRpcRequest {
    pub(crate) method: String,
    pub(crate) params: Option<Value>,
    pub(crate) id: Option<Box<RawValue>>,
}

impl<'de> Deserialize<'de> for JsonRpcDocument {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct DocumentVisitor;
        impl<'de> Visitor<'de> for DocumentVisitor {
            type Value = JsonRpcDocument;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON-RPC document")
            }

            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut version: Option<WireString> = None;
                let mut method: Option<WireString> = None;
                let mut params: Option<WireParams> = None;
                let mut id: Option<Box<RawValue>> = None;
                let mut duplicate = false;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "jsonrpc" => read_field(&mut map, &mut version, &mut duplicate)?,
                        "method" => read_field(&mut map, &mut method, &mut duplicate)?,
                        "params" => read_field(&mut map, &mut params, &mut duplicate)?,
                        "id" => read_field(&mut map, &mut id, &mut duplicate)?,
                        _ => {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }
                }
                // Shape errors are recorded only after the whole object is consumed.
                // The JSON extractor can still report any malformed/trailing syntax.
                let (Some(WireString(Some(version))), Some(WireString(Some(method)))) =
                    (version, method)
                else {
                    return Ok(JsonRpcDocument::InvalidRequest);
                };
                let params = match params {
                    None => None,
                    Some(WireParams(Some(value))) => Some(value),
                    Some(WireParams(None)) => return Ok(JsonRpcDocument::InvalidRequest),
                };
                let valid_id = id.as_ref().is_none_or(|id| {
                    // RawValue validates syntax without rounding numeric tokens.
                    matches!(id.get().as_bytes().first(), Some(b'"' | b'-' | b'0'..=b'9'))
                        || id.get() == "null"
                });
                Ok(if duplicate || version != "2.0" || !valid_id {
                    JsonRpcDocument::InvalidRequest
                } else {
                    JsonRpcDocument::Request(JsonRpcRequest { method, params, id })
                })
            }

            fn visit_seq<S: SeqAccess<'de>>(
                self,
                mut sequence: S,
            ) -> Result<Self::Value, S::Error> {
                let mut empty = true;
                // Validate syntax without retaining or executing unsupported members.
                while sequence.next_element::<IgnoredAny>()?.is_some() {
                    empty = false;
                }
                Ok(if empty {
                    JsonRpcDocument::InvalidRequest
                } else {
                    JsonRpcDocument::UnsupportedBatch
                })
            }
            fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<Self::Value, E> {
                Ok(JsonRpcDocument::InvalidRequest)
            }
            fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<Self::Value, E> {
                Ok(JsonRpcDocument::InvalidRequest)
            }
            fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<Self::Value, E> {
                Ok(JsonRpcDocument::InvalidRequest)
            }
            fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<Self::Value, E> {
                Ok(JsonRpcDocument::InvalidRequest)
            }
            fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<Self::Value, E> {
                Ok(JsonRpcDocument::InvalidRequest)
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(JsonRpcDocument::InvalidRequest)
            }
        }
        deserializer.deserialize_any(DocumentVisitor)
    }
}

fn read_field<'de, M: MapAccess<'de>, T: Deserialize<'de>>(
    map: &mut M,
    field: &mut Option<T>,
    duplicate: &mut bool,
) -> Result<(), M::Error> {
    if field.is_some() {
        *duplicate = true;
        map.next_value::<IgnoredAny>()?;
    } else {
        *field = Some(map.next_value()?);
    }
    Ok(())
}

/// A literal string or a consumed invalid shape; never a Value-coerced string.
struct WireString(Option<String>);

impl<'de> Deserialize<'de> for WireString {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct StringVisitor;
        impl<'de> Visitor<'de> for StringVisitor {
            type Value = WireString;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a request string field")
            }
            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
                Ok(WireString(Some(value.to_owned())))
            }
            fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Self::Value, E> {
                Ok(WireString(Some(value)))
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
                Ok(WireString(None))
            }
            fn visit_seq<S: SeqAccess<'de>>(
                self,
                mut sequence: S,
            ) -> Result<Self::Value, S::Error> {
                while sequence.next_element::<IgnoredAny>()?.is_some() {}
                Ok(WireString(None))
            }
            fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<Self::Value, E> {
                Ok(WireString(None))
            }
            fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<Self::Value, E> {
                Ok(WireString(None))
            }
            fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<Self::Value, E> {
                Ok(WireString(None))
            }
            fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<Self::Value, E> {
                Ok(WireString(None))
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(WireString(None))
            }
        }
        deserializer.deserialize_any(StringVisitor)
    }
}

/// Preserve the literal params root container, with ordinary nested JSON values.
struct WireParams(Option<Value>);

impl<'de> Deserialize<'de> for WireParams {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ParamsVisitor;
        impl<'de> Visitor<'de> for ParamsVisitor {
            type Value = WireParams;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a params array or object")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut values = Map::new();
                while let Some((key, value)) = map.next_entry::<String, Value>()? {
                    values.insert(key, value);
                }
                Ok(WireParams(Some(Value::Object(values))))
            }
            fn visit_seq<S: SeqAccess<'de>>(
                self,
                mut sequence: S,
            ) -> Result<Self::Value, S::Error> {
                let mut values = Vec::new();
                while let Some(value) = sequence.next_element::<Value>()? {
                    values.push(value);
                }
                Ok(WireParams(Some(Value::Array(values))))
            }
            fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<Self::Value, E> {
                Ok(WireParams(None))
            }
            fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<Self::Value, E> {
                Ok(WireParams(None))
            }
            fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<Self::Value, E> {
                Ok(WireParams(None))
            }
            fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<Self::Value, E> {
                Ok(WireParams(None))
            }
            fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<Self::Value, E> {
                Ok(WireParams(None))
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(WireParams(None))
            }
        }
        deserializer.deserialize_any(ParamsVisitor)
    }
}

/// A JSON-RPC 2.0 response.
#[derive(Debug, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
    pub id: Box<RawValue>,
}

/// A JSON-RPC 2.0 error.
#[derive(Debug, Serialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
}

impl JsonRpcResponse {
    pub fn parse_error() -> Self {
        Self::error(Box::<RawValue>::default(), -32700, "Parse error".into())
    }

    pub fn invalid_request() -> Self {
        Self::error(Box::<RawValue>::default(), -32600, "Invalid Request".into())
    }

    pub fn success(id: Box<RawValue>, result: serde_json::Value) -> Self {
        Self {
            jsonrpc: "2.0",
            result: Some(result),
            error: None,
            id,
        }
    }

    pub fn error(id: Box<RawValue>, code: i64, message: String) -> Self {
        Self {
            jsonrpc: "2.0",
            result: None,
            error: Some(JsonRpcError { code, message }),
            id,
        }
    }

    pub fn method_not_found(id: Box<RawValue>) -> Self {
        Self::error(id, -32601, "Method not found".into())
    }

    pub fn invalid_params(id: Box<RawValue>, msg: String) -> Self {
        Self::error(id, -32602, msg)
    }

    pub fn internal_error(id: Box<RawValue>, msg: String) -> Self {
        Self::error(id, -32603, msg)
    }
}
