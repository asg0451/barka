const SEGMENT_PREFIX_TAIL: &str = "data";

/// Full S3 key prefix for segment objects.
///
/// Returns `{leading}/data` when `optional_leading` is `Some(leading)`, or just `data` when `None`.
/// Leading/trailing slashes are trimmed; empty strings are treated as `None`.
pub fn segment_key_prefix(optional_leading: Option<&str>) -> String {
    match optional_leading {
        None => SEGMENT_PREFIX_TAIL.to_string(),
        Some(s) => {
            let s = s.trim_matches('/');
            if s.is_empty() {
                SEGMENT_PREFIX_TAIL.to_string()
            } else {
                format!("{s}/{SEGMENT_PREFIX_TAIL}")
            }
        }
    }
}

/// S3 key prefix for a specific topic-partition's segment objects.
///
/// Returns `{base}/{topic}/{partition}`, e.g. `myenv/data/events/3`.
pub fn partition_data_prefix(base: &str, topic: &str, partition: u32) -> String {
    format!("{base}/{topic}/{partition}")
}

/// Leader election namespace for a topic-partition, used as the S3 lock prefix.
pub fn leader_namespace(topic: &str, partition: u32) -> String {
    format!("{topic}/{partition}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_key_prefix_none() {
        assert_eq!(segment_key_prefix(None), "data");
    }

    #[test]
    fn segment_key_prefix_some() {
        assert_eq!(segment_key_prefix(Some("pfx")), "pfx/data");
    }

    #[test]
    fn segment_key_prefix_trims_slashes() {
        assert_eq!(segment_key_prefix(Some("/pfx/")), "pfx/data");
    }

    #[test]
    fn segment_key_prefix_empty_string() {
        assert_eq!(segment_key_prefix(Some("")), "data");
    }

    #[test]
    fn partition_data_prefix_basic() {
        assert_eq!(partition_data_prefix("pfx/data", "events", 3), "pfx/data/events/3");
    }

    #[test]
    fn leader_namespace_basic() {
        assert_eq!(leader_namespace("events", 3), "events/3");
    }
}
