use crate::*;
use crate::{SnapshotValue, Value};

// Opaque to the C interface.
/// Represents a value obtained from a reader, before or after snapshot occurs.
pub(crate) enum PossumValue {
    ReaderValue(Value),
    SnapshotValue(SnapshotValue<Value>),
}

impl AsRef<Value> for PossumValue {
    fn as_ref(&self) -> &Value {
        match self {
            Self::ReaderValue(value) => value,
            Self::SnapshotValue(sv) => *&sv,
        }
    }
}

impl Deref for PossumValue {
    type Target = Value;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::ReaderValue(value) => value,
            Self::SnapshotValue(sv) => *&sv,
        }
    }
}
