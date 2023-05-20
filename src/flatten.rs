#![doc = include_str!("../README.md")]

use std::borrow::Cow;

use crate::reader::Object;
use crate::reader::Value;

pub fn flatten(object: Object<'_>) -> Object<'_> {
    let mut has_objects = false;

    for (_, value) in object.iter() {
        if value.is_object() {
            has_objects = true;
            break;
        }
    }

    if !has_objects {
        return object;
    }

    let mut dest = Object::new();
    dest.reserve(object.len());
    insert_object(&mut dest, None, object);
    dest
}

fn insert_object<'a>(dest: &mut Object<'a>, key: Option<Cow<str>>, object: Object<'a>) {
    for (k, value) in object {
        let key = match key {
            None => k,
            Some(ref key) => Cow::Owned(format!("{}.{}", key, k)),
        };

        match value {
            Value::Object(object) => {
                insert_object(dest, Some(key), object);
            }
            other => {
                dest.insert(key, other);
            }
        };
    }
}
