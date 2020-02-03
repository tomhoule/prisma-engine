use serde::{Deserialize, Serialize};
use std::borrow::Cow;

pub type DateTime = chrono::DateTime<chrono::Utc>;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum DatabaseValue<'a> {
    #[serde(borrow)]
    String(Cow<'a, str>),
    I32(i32),
    F64(f64), // maybe replace with decimal
    DateTime(DateTime),
    Boolean(bool),
}
