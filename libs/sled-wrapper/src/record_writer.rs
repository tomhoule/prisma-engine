use crate::{
    schema::{Column, Table},
    DatabaseValue,
};
use serde::{ser::Impossible, Serialize, Serializer};

// fn validate_insert_type(value: &DatabaseValue<'_>, column: &Column) -> anyhow::Result<()> {
//     match (&column.r#type, value) {
//         (ColumnType::String, DatabaseValue::String(_)) => Ok(()),
//         (ColumnType::I32, DatabaseValue::I32(_)) => Ok(()),
//         (ColumnType::F64, DatabaseValue::F64(_)) => Ok(()),
//         (ColumnType::Boolean, DatabaseValue::Boolean(_)) => Ok(()),
//         _ => anyhow::bail!("Column type mismatch for {colname}.", colname = column.name),
//     }
// }

// pub(crate) fn write_row(data: impl ToRow, table: &Table) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
//     let id_bytes = {
//         let mut values = Vec::with_capacity(table.id_columns.len());

//         for col in table.id_columns.iter() {
//             let database_value = data
//                 .get_column(col.name)
//                 .ok_or_else(|| anyhow::anyhow!("Missing column {} in input", col.name))?;

//             validate_insert_type(&database_value, col)?;

//             values.push(database_value);
//         }

//         serde_json::to_vec(&values)?
//     };

//     let value_bytes = {
//         let mut values = Vec::with_capacity(table.columsn.len());

//         for col in table.columns.iter() {
//             let database_value = data
//                 .get_column(&col.name)
//                 .ok_or_else(|| anyhow::anyhow!("Missing column {} in input", col.name))?;

//             validate_insert_type(&database_value, col)?;

//             values.push(database_value);
//         }

//         serde_json::to_vec(&values)?
//     };

//     Ok((id_bytes, value_bytes))
// }

// pub trait ToRow {
//     fn get_column<'a>(&'a self, column_name: &str) -> Option<DatabaseValue<'a>>;
// }

// impl<'a, T> ToRow for HashMap<T, DatabaseValue<'a>>
// where
//     T: AsRef<str>,
// {
//     fn get_column<'b>(&'b self, column_name: &str) -> Option<DatabaseValue<'b>> {
//         self.get(column_name)
//     }
// }

// problem: serializer impls determine the order of the fields. We don't want this. VTables?
// read up on the flatbuffers vtable
// flatbuffer of (colidx, bytes)?

// impl ToRow for HashMap<String, crate::DatabaseValue> {
//     fn to_row(&self, table: &Table) -> Result<(Vec<u8>, Vec<u8>), WriteError> {
//         let key_bytes = {
//             let values = table.id_columns.iter().map().collect();
//         };
//     }
// }

pub fn write_row<T: Serialize>(data: T, table: &Table) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
    let id_bytes = {
        let mut id_writer = RecordWriter {
            columns: &table.id_columns,
            buf: Vec::new(),
        };
        data.serialize(&mut id_writer)?;
        id_writer.into_inner()
    };

    let value_bytes = {
        let mut value_writer = RecordWriter {
            columns: &table.columns,
            buf: Vec::new(),
        };

        data.serialize(&mut value_writer)?;
        value_writer.into_inner()
    };

    Ok((id_bytes, value_bytes))
}

#[derive(Debug)]
pub enum WriteError {
    Serialization(serde_json::Error),
    Schema(anyhow::Error),
}

pub(crate) struct ValueSerializer<'a> {
    pub(crate) buf: &'a mut Vec<u8>,
}

impl serde::ser::Serializer for ValueSerializer<'_> {
    type Ok = ();
    type Error = WriteError;
    type SerializeSeq = Impossible<Self::Ok, Self::Error>;
    type SerializeTuple = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleStruct = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = Impossible<Self::Ok, Self::Error>;
    type SerializeMap = Impossible<Self::Ok, Self::Error>;
    type SerializeStruct = Impossible<Self::Ok, Self::Error>;
    type SerializeStructVariant = Impossible<Self::Ok, Self::Error>;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        self.buf.extend_from_slice(&v.to_be_bytes());

        Ok(())
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        self.buf.extend(v.as_bytes());

        Ok(())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        unimplemented!()
    }
    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_newtype_struct<T: ?Sized>(self, name: &'static str, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        unimplemented!()
    }
    fn serialize_newtype_variant<T: ?Sized>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        unimplemented!()
    }
    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        unimplemented!()
    }
    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        unimplemented!()
    }
    fn serialize_tuple_struct(self, name: &'static str, len: usize) -> Result<Self::SerializeTupleStruct, Self::Error> {
        unimplemented!()
    }
    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        unimplemented!()
    }
    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        unimplemented!()
    }
    fn serialize_struct(self, name: &'static str, len: usize) -> Result<Self::SerializeStruct, Self::Error> {
        unimplemented!()
    }
    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        unimplemented!()
    }
    fn collect_str<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: core::fmt::Display,
    {
        unimplemented!()
    }
}

struct RecordWriter<'a> {
    columns: &'a [Column],
    buf: Vec<u8>,
}

impl RecordWriter<'_> {
    fn into_inner(self) -> Vec<u8> {
        self.buf
    }
}

impl serde::ser::SerializeStruct for &mut RecordWriter<'_> {
    type Ok = ();
    type Error = WriteError;

    fn serialize_field<T: ?Sized>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        use std::io::Write;

        let idx: Option<usize> = self.columns.iter().position(|col| col.name == key);

        if let Some(idx) = idx {
            let idx: u8 = idx as u8;

            self.buf.push(idx);
            let value_serializer = ValueSerializer { buf: &mut self.buf };
            value.serialize(value_serializer)?;
        }
        // .ok_or_else(|| WriteError::Schema(anyhow::anyhow!("Could not find {} in table definition", key)))?;

        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a, 'b> Serializer for &'b mut RecordWriter<'a> {
    type Ok = ();
    type Error = WriteError;
    type SerializeSeq = Impossible<Self::Ok, Self::Error>;
    type SerializeTuple = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleStruct = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = Impossible<Self::Ok, Self::Error>;
    type SerializeMap = Impossible<Self::Ok, Self::Error>;
    type SerializeStruct = Self;
    type SerializeStructVariant = Impossible<Self::Ok, Self::Error>;
    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        unimplemented!()
    }
    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }
    fn serialize_newtype_struct<T: ?Sized>(self, name: &'static str, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        unimplemented!()
    }
    fn serialize_newtype_variant<T: ?Sized>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        unimplemented!()
    }
    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        unimplemented!()
    }
    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        unimplemented!()
    }
    fn serialize_tuple_struct(self, name: &'static str, len: usize) -> Result<Self::SerializeTupleStruct, Self::Error> {
        unimplemented!()
    }
    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        unimplemented!()
    }
    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        unimplemented!()
    }
    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(self)
    }
    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        unimplemented!()
    }
    fn collect_str<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: core::fmt::Display,
    {
        unimplemented!()
    }
}

// impl<'a, S> serde::ser::SerializeStruct for StructWriter<'a, S>
// where
//     S: serde::ser::SerializeStruct<Ok = (), Error = serde_json::Error>,
// {
//     type Ok = ();
//     type Error = WriteError;

//     fn serialize_field<T: ?Sized>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
//     where
//         T: Serialize,
//     {
//         if let Some(column) = self.columns.iter().find(|col| col.name == key) {
//             self.serializer
//                 .serialize_field(key, value)
//                 .map_err(WriteError::Serialization)
//         } else {
//             Err(WriteError::Schema(anyhow::anyhow!("unknown column: {}", key)))
//         }
//     }

//     fn end(self) -> Result<Self::Ok, Self::Error> {
//         self.serializer.end().map_err(WriteError::Serialization)
//     }
// }

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteError::Schema(msg) => write!(f, "{}", msg),
            WriteError::Serialization(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for WriteError {}

impl serde::ser::Error for WriteError {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        WriteError::Schema(anyhow::anyhow!("{}", msg))
    }
}

// impl<'a> Serializer for RecordWriter<'a> {
//     type Ok = ();
//     type Error = serde_json::Error;
//     type SerializeSeq = <&'a mut serde_json::Serializer<Vec<u8>> as serde::Serializer>::SerializeSeq;
//     type SerializeTuple = <&'a mut serde_json::Serializer<Vec<u8>> as serde::Serializer>::SerializeTuple;
//     type SerializeTupleStruct = <&'a mut serde_json::Serializer<Vec<u8>> as serde::Serializer>::SerializeTupleStruct;
//     type SerializeTupleVariant = <&'a mut serde_json::Serializer<Vec<u8>> as serde::Serializer>::SerializeTupleVariant;
//     type SerializeMap = <&'a mut serde_json::Serializer<Vec<u8>> as serde::Serializer>::SerializeMap;
//     type SerializeStruct = &'a mut StructWriter<'a>;
//     type SerializeStructVariant =
//         <&'a mut serde_json::Serializer<Vec<u8>> as serde::Serializer>::SerializeStructVariant;
//     fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
//         panic!("can't serialize non-struct, non-map, non-seq value as row")
//         // self.serializer.serialize_bool(v)
//     }

//     fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
//         panic!("can't serialize non-struct, non-map, non-seq value as row")
//         // self.serializer.serialize_i8(v)
//     }

//     fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
//         panic!("can't serialize non-struct, non-map, non-seq value as row")
//         // self.serializer.serialize_i16(v)
//     }

//     fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
//         panic!("can't serialize non-struct, non-map, non-seq value as row")
//         // self.serializer.serialize_i32(v)
//     }

//     fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
//         panic!("can't serialize non-struct, non-map, non-seq value as row")
//         // self.serializer.serialize_i64(v)
//     }

//     fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
//         panic!("can't serialize non-struct, non-map, non-seq value as row")
//         // self.serializer.serialize_u8(v)
//     }

//     fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
//         panic!("can't serialize non-struct, non-map, non-seq value as row")
//         // self.serializer.serialize_u16(v)
//     }

//     fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
//         panic!("can't serialize non-struct, non-map, non-seq value as row")
//         // self.serializer.serialize_u32(v)
//     }

//     fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
//         panic!("can't serialize non-struct, non-map, non-seq value as row")
//         // self.serializer.serialize_u64(v)
//     }

//     fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
//         panic!("can't serialize non-struct, non-map, non-seq value as row")
//         // self.serializer.serialize_f32(v)
//     }

//     fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
//         panic!("can't serialize non-struct, non-map, non-seq value as row")
//         // self.serializer.serialize_f64(v)
//     }

//     fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
//         panic!("can't serialize non-struct, non-map, non-seq value as row")
//         // self.serializer.serialize_char(v)
//     }

//     fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
//         panic!("can't serialize non-struct, non-map, non-seq value as row")
//         // self.serializer.serialize_str(v)
//     }

//     fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
//         panic!("can't serialize non-struct, non-map, non-seq value as row")
//         // self.serializer.serialize_bytes(v)
//     }

//     fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
//         panic!("can't serialize non-struct, non-map, non-seq value as row")
//         // self.serializer.serialize_none()
//     }

//     fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
//     where
//         T: serde::Serialize,
//     {
//         panic!("can't serialize non-struct, non-map, non-seq value as row")
//         // self.serializer.serialize_some(value)
//     }

//     fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
//         panic!("can't serialize non-struct, non-map, non-seq value as row")
//         // self.serializer.serialize_unit()
//     }

//     fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
//         panic!("can't serialize non-struct, non-map, non-seq value as row")
//         // self.serializer.serialize_unit_struct(name)
//     }

//     fn serialize_unit_variant(
//         self,
//         name: &'static str,
//         variant_index: u32,
//         variant: &'static str,
//     ) -> Result<Self::Ok, Self::Error> {
//         panic!("can't serialize non-struct, non-map, non-seq value as row")
//         // self.serializer.serialize_unit_variant(name, variant_index, variant)
//     }

//     fn serialize_newtype_struct<T: ?Sized>(self, name: &'static str, value: &T) -> Result<Self::Ok, Self::Error>
//     where
//         T: serde::Serialize,
//     {
//         panic!("can't serialize non-struct, non-map, non-seq value as row")
//         // self.serializer.serialize_newtype_struct(name, value)
//     }

//     fn serialize_newtype_variant<T: ?Sized>(
//         self,
//         name: &'static str,
//         variant_index: u32,
//         variant: &'static str,
//         value: &T,
//     ) -> Result<Self::Ok, Self::Error>
//     where
//         T: serde::Serialize,
//     {
//         panic!("can't serialize non-struct, non-map, non-seq value as row")

//         // self.serializer
//         //     .serialize_newtype_variant(name, variant_index, variant, value)
//     }

//     fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
//         todo!("serialize seq")
//     }

//     fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
//         panic!("can't serialize non-struct, non-map, non-seq value as row")
//     }

//     fn serialize_tuple_struct(self, name: &'static str, len: usize) -> Result<Self::SerializeTupleStruct, Self::Error> {
//         panic!("can't serialize non-struct, non-map, non-seq value as row")
//     }

//     fn serialize_tuple_variant(
//         self,
//         name: &'static str,
//         variant_index: u32,
//         variant: &'static str,
//         len: usize,
//     ) -> Result<Self::SerializeTupleVariant, Self::Error> {
//         panic!("can't serialize non-struct, non-map, non-seq value as row")
//     }

//     fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
//         // self.serializer.serialize_map(len)
//         todo!()
//     }

//     fn serialize_struct(self, name: &'static str, len: usize) -> Result<Self::SerializeStruct, Self::Error> {
//         let serializer = self.serializer.serialize_struct(name, len)?;
//         let serializer = StructWriter {
//             serializer: &mut serializer,
//             columns: &self.columns,
//         };

//         Ok(serializer)
//     }

//     fn serialize_struct_variant(
//         self,
//         name: &'static str,
//         variant_index: u32,
//         variant: &'static str,
//         len: usize,
//     ) -> Result<Self::SerializeStructVariant, Self::Error> {
//         panic!("can't serialize non-struct, non-map, non-seq value as row")
//     }

//     // fn collect_str<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
//     // where
//     //     T: core::fmt::Display,
//     // {
//     //     panic!("can't serialize non-struct, non-map, non-seq value as row")
//     // }
// }

// impl<'a, T> serde::Serialize for RecordWriter<'a, T>
// where
//     T: serde::Serialize,
// {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: serde::Serializer,
//     {
//         use crate::schema::ColumnType;

//         let serialize_struct = serializer.serialize_struct("sled-wrapper record");

//         for column in self.columns {
//             match column.r#type {
//                 ColumnType::String => todo!(),
//                 ColumnType::I32 => todo!(),
//                 ColumnType::F64 => todo!(),
//                 ColumnType::Boolean => todo!(),
//             }
//         }
//         todo!()
//     }
// }
