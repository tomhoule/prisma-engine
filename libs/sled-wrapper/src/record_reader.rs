use crate::schema;
use crate::DatabaseValue;
use serde::{Deserialize, Deserializer};
use std::io::Read;

pub(crate) fn read_record<'a, 'schema, T>(
    key: &'a [u8],
    value: &'a [u8],
    table_metadata: &'schema crate::schema::Table,
) -> Result<T, anyhow::Error>
where
    T: Deserialize<'a>,
{
    let deserializer = RecordDeserializer {
        key,
        value,
        table_metadata,
    };

    let t = T::deserialize(deserializer)?;
    // let (key, value) = (serde_json::from_slice(key)?, serde_json::from_slice(value)?);
    // let t = T::deserialize(RecordDeserializer {
    //     key,
    //     value,
    //     table_metadata,
    // })?;

    Ok(t)
}

struct RecordDeserializer<'a, 'meta> {
    key: &'a [u8],
    value: &'a [u8],
    table_metadata: &'meta schema::Table,
}

impl<'a, 'meta> RecordDeserializer {
    fn read_values(&self) -> Vec<DatabaseValue<'a>> {
        let mut values = Vec::with_capacity(self.table_metadata.columns.len());
        let cursor = std::io::Cursor::new(self.value);

        let mut values_left = cursor.position() < (self.value.len() as u64 - 1);

        let mut colid: [u8; 1] = [0];

        values
    }
}

impl<'de, 'meta> Deserializer<'de> for RecordDeserializer<'de, 'meta> {
    type Error = serde::de::value::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }
    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }
    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }
    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }
    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }
    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }
    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }
    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }
    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }
    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }
    fn deserialize_unit_struct<V>(self, name: &'static str, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }
    fn deserialize_newtype_struct<V>(self, name: &'static str, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }
    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }
    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }
    fn deserialize_tuple_struct<V>(self, name: &'static str, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }
    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let values: Vec<DatabaseValue<'de>> = self.read_values()?;
        let id_cols_count = self.table_metadata.id_columns.len();
        let id_cols_iter = self.table_metadata.id_columns.iter().zip(fields.iter());

        for (col, name) in id_cols_iter {
            debug_assert_eq!(col.name.as_str(), *name);

            match col.r#type {
                schema::ColumnType::String => todo!(),
                _ => todo!(),
            }
        }

        unimplemented!()
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }
    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }
    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        unimplemented!()
    }
}

// struct RecordDeserializer<'a, 'meta> {
//     key: Vec<DatabaseValue<'a>>,
//     value: Vec<DatabaseValue<'a>>,
//     table_metadata: &'meta crate::schema::Table,
// }

// impl<'de, 'b> serde::Deserializer<'de> for RecordDeserializer<'de, 'b> {
//     type Error = serde::de::value::Error;

//     fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
//     where
//         V: serde::de::Visitor<'de>,
//     {
//         panic!("deserialize_any on record deserializer")
//     }

//     fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
//     where
//         V: serde::de::Visitor<'de>,
//     {
//         let iterator = self
//             .key
//             .into_iter()
//             .chain(self.value.into_iter())
//             .map(|value| ValueDeserializer { value });
//         let seq_access = serde::de::value::SeqDeserializer::new(iterator);
//         visitor.visit_seq(seq_access)
//     }

//     fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
//     where
//         V: serde::de::Visitor<'de>,
//     {
//         unimplemented!("deserialize_tuple for row")
//     }

//     fn deserialize_tuple_struct<V>(self, name: &'static str, len: usize, visitor: V) -> Result<V::Value, Self::Error>
//     where
//         V: serde::de::Visitor<'de>,
//     {
//         unimplemented!("deserialize_tuple_struct for row")
//     }

//     fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
//     where
//         V: serde::de::Visitor<'de>,
//     {
//         debug_assert_eq!(self.table_metadata.columns.len(), self.key.len() + self.value.len());

//         let keys = self
//             .table_metadata
//             .id_columns
//             .iter()
//             .chain(self.table_metadata.columns.iter())
//             .map(|col| col.name.as_str());
//         let values = self
//             .key
//             .into_iter()
//             .chain(self.value.into_iter())
//             .map(|value| ValueDeserializer { value });

//         let deserializer = serde::de::value::MapDeserializer::new(keys.zip(values));

//         visitor.visit_map(deserializer)
//     }

//     fn deserialize_struct<V>(
//         self,
//         _name: &'static str,
//         _fields: &'static [&'static str],
//         visitor: V,
//     ) -> Result<V::Value, Self::Error>
//     where
//         V: serde::de::Visitor<'de>,
//     {
//         self.deserialize_map(visitor)
//     }

//     serde::forward_to_deserialize_any! {
//         i8 i16 i32 i64 i128
//         u8 u16 u32 u64 u128
//         enum ignored_any identifier
//         unit option byte_buf
//         unit_struct newtype_struct bool
//         f32 f64 char str string bytes
//     }
// }

// impl<'de> serde::de::IntoDeserializer<'de> for ValueDeserializer<'de> {
//     type Deserializer = ValueDeserializer<'de>;

//     fn into_deserializer(self) -> Self {
//         self
//     }
// }

// struct ValueDeserializer<'a> {
//     value: DatabaseValue<'a>,
// }

// impl<'de> Deserializer<'de> for ValueDeserializer<'de> {
//     type Error = serde::de::value::Error;

//     fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
//     where
//         V: serde::de::Visitor<'de>,
//     {
//         match self.value {
//             _ => todo!("value deserializer"),
//         }
//     }

//     serde::forward_to_deserialize_any! {
//         i8 i16 i32 i64 i128
//         u8 u16 u32 u64 u128
//         struct enum ignored_any identifier map
//         tuple tuple_struct unit option byte_buf
//         unit_struct newtype_struct seq bool
//         f32 f64 char str string bytes
//     }
// }
