use regex::Regex;

use crate::{PiperError, Value};

pub fn regexp(s: String, re: String) -> Value {
    match Regex::new(&re) {
        Ok(re) => re.is_match(&s).into(),
        Err(e) => PiperError::ExternalError(e.to_string()).into(),
    }
}

pub fn regexp_extract(s: String, re: String, idx: Option<usize>) -> Value {
    match Regex::new(&re) {
        Ok(re) => {
            let idx = idx.unwrap_or(1);
            re.captures(&s)
                .and_then(|cap| cap.get(idx).map(|m| m.as_str().to_string()))
                .unwrap_or_default()
                .into()
        }
        Err(e) => PiperError::ExternalError(e.to_string()).into(),
    }
}

pub fn regexp_extract_all(s: String, re: String) -> Value {
    match Regex::new(&re) {
        Ok(re) => re
            .captures_iter(&s)
            .filter_map(|cap| cap.get(0).map(|s| s.as_str().to_string()))
            .collect::<Vec<String>>()
            .into(),
        Err(e) => PiperError::ExternalError(e.to_string()).into(),
    }
}

pub fn regexp_replace(s: String, re: String, replace: String) -> Value {
    match Regex::new(&re) {
        Ok(re) => re.replace_all(&s, replace.as_str()).to_string().into(),
        Err(e) => PiperError::ExternalError(e.to_string()).into(),
    }
}

#[cfg(test)]
mod tests {
    use crate::{IntoValue, Value};

    #[test]
    fn test_regexp() {
        assert!(super::regexp("hello world".to_string(), "[abc".to_string()).is_error());
        assert!(
            super::regexp("hello world".to_string(), "^hello".to_string())
                .get_bool()
                .unwrap()
        );
        assert!(
            !super::regexp("hello world".to_string(), "^world".to_string())
                .get_bool()
                .unwrap()
        );
    }

    #[test]
    fn test_regexp_extract() {
        assert!(
            super::regexp_extract("hello world".to_string(), "[abc".to_string(), None).is_error()
        );
        assert_eq!(
            super::regexp_extract("hello world".to_string(), "^(hello)".to_string(), None),
            "hello".into_value()
        );
        assert_eq!(
            super::regexp_extract("hello world".to_string(), "^(hello)".to_string(), Some(0)),
            "hello".into_value()
        );
        assert_eq!(
            super::regexp_extract("hello world".to_string(), "^(hello)".to_string(), Some(1)),
            "hello".into_value()
        );
        assert_eq!(
            super::regexp_extract("hello world".to_string(), "^(world)".to_string(), None),
            "".into_value()
        );
        assert_eq!(
            super::regexp_extract("hello world".to_string(), "^(world)".to_string(), Some(0)),
            "".into_value()
        );
        assert_eq!(
            super::regexp_extract("hello world".to_string(), "^(world)".to_string(), Some(1)),
            "".into_value()
        );
    }

    #[test]
    fn test_regexp_extract_all() {
        assert!(
            super::regexp_extract_all("hello world".to_string(), "[abc".to_string()).is_error()
        );
        assert_eq!(
            super::regexp_extract_all("hello world".to_string(), "^(hello)".to_string()),
            vec!["hello".to_string()].into_value()
        );
        assert_eq!(
            super::regexp_extract_all("hello world".to_string(), "^(world)".to_string()),
            Value::Array(vec![])
        );
    }

    #[test]
    fn test_regexp_replace() {
        assert!(super::regexp_replace(
            "hello world".to_string(),
            "[abc".to_string(),
            "x".to_string()
        )
        .is_error());
        assert_eq!(
            super::regexp_replace(
                "hello world".to_string(),
                "^(hello)".to_string(),
                "x".to_string()
            ),
            "x world".into_value()
        );
        assert_eq!(
            super::regexp_replace(
                "hello world".to_string(),
                "^(world)".to_string(),
                "x".to_string()
            ),
            "hello world".into_value()
        );
    }
}
