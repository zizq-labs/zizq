// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! jq-style payload filtering for the List Jobs API.
//!
//! Compiles a jq expression at request time and evaluates it against each
//! job's payload. A job matches if the filter produces any truthy output
//! (not `false`, not `null`). Runtime errors (e.g. indexing a number) are
//! treated as non-matches rather than errors, because payloads are
//! heterogeneous in the store.

use jaq_core::load::{Arena, File, Loader};
use jaq_core::{Compiler, Ctx, RcIter};
use jaq_json::Val;

/// A compiled jq filter ready for repeated evaluation.
pub struct PayloadFilter {
    expression: String,
    filter: jaq_core::Filter<jaq_core::Native<Val>>,
}

impl PayloadFilter {
    /// The original jq expression string.
    pub fn expression(&self) -> &str {
        &self.expression
    }
}

impl std::fmt::Debug for PayloadFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PayloadFilter")
            .field("expression", &self.expression)
            .finish_non_exhaustive()
    }
}

impl PayloadFilter {
    /// Compile a jq expression. Returns a human-readable error on failure.
    pub fn compile(expression: &str) -> Result<Self, String> {
        let arena = Arena::default();
        let loader = Loader::new(jaq_std::defs().chain(jaq_json::defs()));

        let modules = loader
            .load(
                &arena,
                File {
                    path: (),
                    code: expression,
                },
            )
            .map_err(|errs| format_load_errors(errs))?;

        let filter = Compiler::default()
            .with_funs(jaq_std::funs().chain(jaq_json::funs()))
            .compile(modules)
            .map_err(|errs| format_compile_errors(errs))?;

        Ok(Self {
            expression: expression.to_string(),
            filter,
        })
    }

    /// Test whether a payload matches this filter.
    ///
    /// Returns `true` if the filter produces any truthy output (not `false`,
    /// not `null`). Runtime errors are treated as non-matches.
    pub fn matches(&self, payload: &serde_json::Value) -> bool {
        let val: Val = payload.clone().into();
        let inputs = RcIter::new(core::iter::empty());
        let ctx = Ctx::new([], &inputs);

        self.filter.run((ctx, val)).any(|result| match result {
            Ok(v) => is_truthy(&v),
            Err(_) => false,
        })
    }
}

fn is_truthy(v: &Val) -> bool {
    !matches!(v, Val::Bool(false) | Val::Null)
}

/// Handles expression parsing issues (syntax issues only).
fn format_load_errors(
    errs: Vec<(jaq_core::load::File<&str, ()>, jaq_core::load::Error<&str>)>,
) -> String {
    use jaq_core::load::Error;

    let mut messages = Vec::new();
    for (_, err) in &errs {
        match err {
            Error::Io(_) => {
                messages.push("unexpected I/O error".to_string());
            }
            Error::Lex(lexes) => {
                for (expected, got) in lexes {
                    messages.push(format!("expected {}, got \"{}\"", expected.as_str(), got));
                }
            }
            Error::Parse(parses) => {
                for (expected, got) in parses {
                    if got.is_empty() {
                        messages.push(format!("expected {}", expected.as_str()));
                    } else {
                        messages.push(format!("expected {}, got \"{}\"", expected.as_str(), got));
                    }
                }
            }
        }
    }

    if messages.is_empty() {
        "invalid filter expression".to_string()
    } else {
        messages.join("; ")
    }
}

/// Handles expression compile errors (valid syntax, invalid references).
fn format_compile_errors(
    errs: Vec<(
        jaq_core::load::File<&str, ()>,
        Vec<(&str, jaq_core::compile::Undefined)>,
    )>,
) -> String {
    let mut messages = Vec::new();
    for (_, es) in &errs {
        for (name, undef) in es {
            messages.push(format!("undefined {} \"{}\"", undef.as_str(), name));
        }
    }

    if messages.is_empty() {
        "failed to compile filter expression".to_string()
    } else {
        messages.join("; ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn compile_valid_expression() {
        PayloadFilter::compile(".user_id == 42").unwrap();
    }

    #[test]
    fn compile_invalid_expression() {
        let result = PayloadFilter::compile(".user_id ====");
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(!err.is_empty(), "expected error message, got empty");
    }

    #[test]
    fn compile_empty_expression_gives_readable_error() {
        let err = PayloadFilter::compile("").err().unwrap();
        assert!(
            err.contains("expected"),
            "expected human-readable error, got: {err}"
        );
    }

    #[test]
    fn compile_undefined_function_gives_readable_error() {
        let err = PayloadFilter::compile("x").err().unwrap();
        assert!(
            err.contains("undefined") && err.contains("x"),
            "expected 'undefined' mention, got: {err}"
        );
    }

    #[test]
    fn compile_bad_syntax_gives_readable_error() {
        let err = PayloadFilter::compile(".[*]").err().unwrap();
        assert!(
            err.contains("expected"),
            "expected human-readable error, got: {err}"
        );
    }

    #[test]
    fn matches_equality() {
        let f = PayloadFilter::compile(".user_id == 42").unwrap();
        assert!(f.matches(&json!({"user_id": 42})));
        assert!(!f.matches(&json!({"user_id": 43})));
    }

    #[test]
    fn matches_nested_field() {
        let f = PayloadFilter::compile(".user.email == \"a@b.com\"").unwrap();
        assert!(f.matches(&json!({"user": {"email": "a@b.com"}})));
        assert!(!f.matches(&json!({"user": {"email": "x@y.com"}})));
    }

    #[test]
    fn matches_contains() {
        let f = PayloadFilter::compile(".tags | contains([\"urgent\"])").unwrap();
        assert!(f.matches(&json!({"tags": ["urgent", "email"]})));
        assert!(!f.matches(&json!({"tags": ["email"]})));
    }

    #[test]
    fn matches_comparison() {
        let f = PayloadFilter::compile(".priority < 10").unwrap();
        assert!(f.matches(&json!({"priority": 5})));
        assert!(!f.matches(&json!({"priority": 15})));
    }

    #[test]
    fn matches_and() {
        let f = PayloadFilter::compile(".a == 1 and .b == 2").unwrap();
        assert!(f.matches(&json!({"a": 1, "b": 2})));
        assert!(!f.matches(&json!({"a": 1, "b": 3})));
    }

    #[test]
    fn runtime_error_is_non_match() {
        let f = PayloadFilter::compile(".y == 1").unwrap();
        assert!(!f.matches(&json!(42)));
    }

    #[test]
    fn missing_field_is_non_match() {
        let f = PayloadFilter::compile(".user_id == 42").unwrap();
        assert!(!f.matches(&json!({"other_field": "hello"})));
    }

    #[test]
    fn null_payload_is_non_match() {
        let f = PayloadFilter::compile(".x == 1").unwrap();
        assert!(!f.matches(&json!(null)));
    }

    #[test]
    fn identity_filter_matches_truthy() {
        let f = PayloadFilter::compile(".").unwrap();
        assert!(f.matches(&json!({"anything": true})));
        assert!(f.matches(&json!(42)));
        assert!(!f.matches(&json!(false)));
        assert!(!f.matches(&json!(null)));
    }

    #[test]
    fn select_filter() {
        let f = PayloadFilter::compile("select(.x > 5)").unwrap();
        assert!(f.matches(&json!({"x": 10})));
        assert!(!f.matches(&json!({"x": 3})));
    }

    #[test]
    fn regex_match() {
        let f = PayloadFilter::compile(".email | test(\"@example\\\\.com$\")").unwrap();
        assert!(f.matches(&json!({"email": "user@example.com"})));
        assert!(!f.matches(&json!({"email": "user@other.com"})));
    }
}
