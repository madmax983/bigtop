//! Strongly-typed identifiers. Never a bare `String` for an id.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Process-local sequence mixed into every generated id.
static ID_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Build a unique id like `task-18f2ab41c0-2a1f-3`.
fn fresh_id(prefix: &str) -> String {
    let seq = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos: u128 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let pid = std::process::id();
    format!("{prefix}-{nanos:x}-{pid:x}-{seq:x}")
}

macro_rules! define_id {
    ($name:ident, $prefix:literal, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            #[doc = concat!("Generate a fresh `", stringify!($name), "`.")]
            #[must_use]
            pub fn generate() -> Self {
                Self(fresh_id($prefix))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl From<String> for $name {
            fn from(s: String) -> Self {
                Self(s)
            }
        }
    };
}

define_id!(JobId, "job", "Unique identifier for a `BigTop` job.");
define_id!(TaskId, "task", "Unique identifier for a single task.");
define_id!(NodeId, "node", "Unique identifier for an agent node.");

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn generated_ids_are_unique_and_prefixed() {
        let mut seen = HashSet::new();
        for _ in 0..1000 {
            let id = TaskId::generate();
            assert!(id.as_ref().starts_with("task-"), "id: {id}");
            assert!(seen.insert(id.to_string()), "duplicate id: {id}");
        }
    }

    #[test]
    fn ids_roundtrip_through_json() {
        let id = JobId::from("job-abc".to_string());
        let json = serde_json::to_string(&id).expect("serialize");
        assert_eq!(json, r#""job-abc""#);
        let back: JobId = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(id, back);
    }

    #[test]
    fn ids_sort_lexicographically() {
        let mut ids = [
            TaskId::from("task-9".to_string()),
            TaskId::from("task-10".to_string()),
        ];
        ids.sort();
        assert_eq!(ids[0].as_ref(), "task-10");
    }
}
