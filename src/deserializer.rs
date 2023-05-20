use crate::reader::{Map, Value};
use core::fmt;

use serde::{
    de::{MapAccess, SeqAccess, Visitor},
    Deserialize
};
use std::borrow::Cow;

impl<'de> Deserialize<'de> for Value<'de> {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = Value<'de>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("any valid value")
            }

            #[inline]
            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
                Ok(Value::Bool(value))
            }

            #[inline]
            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
                Ok(Value::Int(value))
            }

            #[inline]
            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
                Ok(Value::Int(value as i64))
            }

            #[inline]
            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E> {
                Ok(Value::Float(value))
            }

            #[inline]
            fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::String(Cow::Borrowed(value)))
            }

            #[inline]
            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::String(Cow::Owned(String::from(value))))
            }

            #[inline]
            fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
                Ok(Value::String(Cow::Owned(value)))
            }

            #[inline]
            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(Value::Null)
            }

            #[inline]
            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                Deserialize::deserialize(deserializer)
            }

            #[inline]
            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(Value::Null)
            }

            #[inline]
            fn visit_seq<V>(self, mut visitor: V) -> Result<Self::Value, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let mut vec = Vec::new();

                while let Some(elem) = visitor.next_element()? {
                    vec.push(elem);
                }

                Ok(Value::Array(vec))
            }

            fn visit_map<V>(self, mut visitor: V) -> Result<Self::Value, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut map = Map::with_capacity(visitor.size_hint().unwrap_or(0));
                while let Some((key, value)) = visitor.next_entry()? {
                    map.insert(key, value);
                }

                Ok(Value::Object(map))
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

// impl<'de> Deserialize<'de> for Value {
//     #[inline]
//     fn deserialize_in_place<D>(deserializer: D, place: &mut Self) -> Result<(), D::Error>
//     where
//         D: serde::Deserializer<'de>,
//     {
//         deserializer.deserialize_any(ValueInPlaceVisitor(place))
//     }

// }

// struct ValueInPlaceVisitor<'a>(&'a mut Value);

// impl<'a, 'de> Visitor<'de> for ValueInPlaceVisitor<'a> {
//     type Value = ();

//     fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
//         formatter.write_str("any valid value")
//     }

//     #[inline]
//     fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
//         *self.0 = Value::Bool(value);
//         Ok(())
//     }

//     #[inline]
//     fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
//         *self.0 = Value::Int(value);
//         Ok(())
//     }

//     #[inline]
//     fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
//         *self.0 = Value::Int(value as i64);
//         Ok(())
//     }

//     #[inline]
//     fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E> {
//         *self.0 = Value::Float(value);
//         Ok(())
//     }

//     #[inline]
//     fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
//     where
//         E: serde::de::Error,
//     {
//         match self.0 {
//             Value::String(str) => {
//                 str.clear();
//                 str.push_str(value);
//             }
//             _ => {
//                 *self.0 = Value::String(String::from(value));
//             }
//         }
//         Ok(())
//     }

//     #[inline]
//     fn visit_none<E>(self) -> Result<Self::Value, E> {
//         *self.0 = Value::Null;
//         Ok(())
//     }

//     #[inline]
//     fn visit_seq<V>(self, mut visitor: V) -> Result<Self::Value, V::Error>
//     where
//         V: SeqAccess<'de>,
//     {
//         let size = visitor.size_hint().unwrap_or(0);

//         let vec = match self.0 {
//             Value::Array(vec) => {
//                 if vec.capacity() < size {
//                     vec.reserve(size - vec.capacity());
//                 }
//                 vec
//             }
//             _ => {
//                 *self.0 = Value::Array(Vec::with_capacity(size));
//                 match self.0 {
//                     Value::Array(vec) => vec,
//                     _ => unreachable!(),
//                 }
//             }
//         };

//         for i in 0..vec.len() {
//             let next = {
//                 let value = InPlaceSeed(&mut vec[i]);
//                 visitor.next_element_seed(value)?
//             };
//             if next.is_none() {
//                 vec.truncate(i);
//                 return Ok(());
//             }
//         }

//         while let Some(elem) = visitor.next_element()? {
//             vec.push(elem);
//         }

//         Ok(())
//     }

//     #[inline]
//     fn visit_map<V>(self, mut visitor: V) -> Result<Self::Value, V::Error>
//     where
//         V: MapAccess<'de>,
//     {
//         let size = visitor.size_hint().unwrap_or(0);

//         let object = match self.0 {
//             Value::Object(object) => {
//                 if object.capacity() < size {
//                     object.reserve(size - object.capacity());
//                 }
//                 object
//             }
//             _ => {
//                 *self.0 = Value::Object(Vec::with_capacity(size));
//                 match self.0 {
//                     Value::Object(object) => object,
//                     _ => unreachable!(),
//                 }
//             }
//         };

//         for i in 0..object.len() {
//             let next = {
//                 let (k, v) = &mut object[i];
//                 let key = InPlaceSeed(k);
//                 let v = InPlaceSeed(v);
//                 visitor.next_entry_seed(key, v)?
//             };
//             if next.is_none() {
//                 object.truncate(i);
//                 return Ok(());
//             }
//         }

//         while let Some((key, value)) = visitor.next_entry()? {
//             object.push((key, value));
//         }

//         Ok(())
//     }
// }
