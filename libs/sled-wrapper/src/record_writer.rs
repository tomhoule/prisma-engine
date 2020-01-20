use crate::schema::Table;

pub(crate) struct RecordWriter<'a, T> {
    data: &'a T,
    table_definition: &'a Table,
}

#[derive(Debug)]
struct WriteError(String);


impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
     }

}

impl std::error::Error for WriteError {

}

impl serde::ser::Error for WriteError {
    fn custom<T>(msg:T)->Self where T:std::fmt::Display  { WriteError(msg.to_string()) }
}

impl<'a, T> serde::Serializer for RecordWriter<'a, T> {
    type Ok = ();
    type Error = WriteError;
    type SerializeSeq: serde::ser::Impossible<(), WriteError>;
    type SerializeTuple: serde::ser::Impossible<(), WriteError>;
    type SerializeTupleStruct: serde::ser::Impossible<(), WriteError>;
    type SerializeTupleVariant: serde::ser::Impossible<(), WriteError>;
    type SerializeMap: serde::ser::Impossible<(), WriteError>;
    type SerializeStruct: serde::ser::Impossible<(), WriteError>;
    type SerializeStructVariant: serde::ser::Impossible<(), WriteError>;
    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> { unimplemented!() }
    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> { unimplemented!() }
    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> { unimplemented!() }
    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> { unimplemented!() }
    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> { unimplemented!() }
    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> { unimplemented!() }
    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> { unimplemented!() }
    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> { unimplemented!() }
    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> { unimplemented!() }
    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> { unimplemented!() }
    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> { unimplemented!() }
    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> { unimplemented!() }
    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> { unimplemented!() }
    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> { unimplemented!() }
    fn serialize_none(self) -> Result<Self::Ok, Self::Error> { unimplemented!() }
    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize { unimplemented!() }
    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> { unimplemented!() }
    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> { unimplemented!() }
    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> { unimplemented!() }
    fn serialize_newtype_struct<T: ?Sized>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize { unimplemented!() }
    fn serialize_newtype_variant<T: ?Sized>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize { unimplemented!() }
    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> { unimplemented!() }
    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> { unimplemented!() }
    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> { unimplemented!() }
    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> { unimplemented!() }
    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> { unimplemented!() }
    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> { unimplemented!() }
    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> { unimplemented!() }
    fn collect_str<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: std::fmt::Display { unimplemented!() }

}
