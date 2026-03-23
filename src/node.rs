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
}
