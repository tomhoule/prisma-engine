use crate::schema::Column;

pub(crate) struct RecordWriter<'a, T> {
    pub(crate) data: &'a T,
    pub(crate) columns: &'a [Column],
}

#[derive(Debug)]
struct WriteError(String);

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for WriteError {}

impl serde::ser::Error for WriteError {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        WriteError(msg.to_string())
    }
}

impl<'a, T> serde::Serialize for RecordWriter<'a, T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        todo!()
    }
}
