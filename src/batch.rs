// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! jq-style expression evaluation for batched jobs.
//!
//! Batched jobs carry two jq expressions: a `when` predicate deciding
//! whether to fold a new enqueue into an existing pending batch, and a
//! `fold` reducer producing the merged payload. Both expressions run with
//! `$existing` bound to the current pending payload and `$new` bound to
//! the incoming payload; neither expression consumes a piped input.

use jaq_core::load::{Arena, File, Loader};
use jaq_core::{Compiler, Ctx, Vars, data, unwrap_valr};
use jaq_json::{Num, Val};

/// Compiled `when` and `fold` expressions ready for repeated evaluation.
pub struct BatchExpr {
    when_expr: String,
    fold_expr: String,
    when: jaq_core::Filter<data::JustLut<Val>>,
    fold: jaq_core::Filter<data::JustLut<Val>>,
}

impl BatchExpr {
    /// The original `when` expression string.
    pub fn when_expression(&self) -> &str {
        &self.when_expr
    }

    /// The original `fold` expression string.
    pub fn fold_expression(&self) -> &str {
        &self.fold_expr
    }
}

impl std::fmt::Debug for BatchExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchExpr")
            .field("when", &self.when_expr)
            .field("fold", &self.fold_expr)
            .finish_non_exhaustive()
    }
}

impl BatchExpr {
    /// Compile both `when` and `fold` expressions.
    ///
    /// Both expressions are compiled with `$existing` and `$new` declared
    /// as global variables. Errors from either expression are surfaced as
    /// a single human-readable string.
    pub fn compile(when_expr: &str, fold_expr: &str) -> Result<Self, String> {
        let when = compile_one(when_expr).map_err(|e| format!("when: {e}"))?;
        let fold = compile_one(fold_expr).map_err(|e| format!("fold: {e}"))?;
        Ok(Self {
            when_expr: when_expr.to_string(),
            fold_expr: fold_expr.to_string(),
            when,
            fold,
        })
    }

    /// Evaluate the `when` predicate.
    ///
    /// Returns `true` if the first output is truthy (not `false`, not
    /// `null`). No output, runtime errors, or a leading falsy value all
    /// return `false`, matching the conservative semantics used for
    /// payload filters.
    pub fn eval_when(&self, existing: &serde_json::Value, new: &serde_json::Value) -> bool {
        let (Some(existing_val), Some(new_val)) = (to_val(existing), to_val(new)) else {
            return false;
        };
        let ctx =
            Ctx::<data::JustLut<Val>>::new(&self.when.lut, Vars::new([existing_val, new_val]));

        // `when` and `fold` are expected to be self-contained expressions
        // that operate on `$existing`/`$new` rather than a piped input.
        // Passing `Val::Null` as the input matches jq's behaviour of
        // running a top-level expression against the implicit null input
        // when no data is piped in.
        self.when
            .id
            .run((ctx, Val::Null))
            .map(unwrap_valr)
            .next()
            .and_then(Result::ok)
            .map(|v| is_truthy(&v))
            .unwrap_or(false)
    }

    /// Dry-run both expressions against `payload` bound as both
    /// `$existing` and `$new` to surface shape errors at enqueue time
    /// rather than at first fold.
    ///
    /// Compile-time errors are caught by `compile`; this catches the
    /// runtime shape errors that only manifest against actual data
    /// (`.field` on the wrong type, `length` on an unsupported value,
    /// etc.). Returns `Err` if either expression errors, or if `fold`
    /// produces zero or multiple outputs. The boolean output of `when`
    /// is not consulted — only whether it evaluates without error.
    pub fn dry_run(&self, payload: &serde_json::Value) -> Result<(), String> {
        self.try_eval_when(payload, payload)
            .map_err(|e| format!("when: {e}"))?;
        self.eval_fold(payload, payload)
            .map(|_| ())
            .map_err(|e| format!("fold: {e}"))
    }

    /// Fallible variant of `eval_when` used only by `dry_run`. Runtime
    /// callers use `eval_when`, which folds runtime errors into `false`
    /// so a broken predicate never blocks a fresh enqueue from creating
    /// a new batch.
    fn try_eval_when(
        &self,
        existing: &serde_json::Value,
        new: &serde_json::Value,
    ) -> Result<bool, String> {
        let existing_val = to_val(existing)
            .ok_or_else(|| "existing payload could not be interpreted as JSON".to_string())?;
        let new_val = to_val(new)
            .ok_or_else(|| "new payload could not be interpreted as JSON".to_string())?;

        let ctx =
            Ctx::<data::JustLut<Val>>::new(&self.when.lut, Vars::new([existing_val, new_val]));

        match self.when.id.run((ctx, Val::Null)).map(unwrap_valr).next() {
            None => Ok(false),
            Some(Ok(v)) => Ok(is_truthy(&v)),
            Some(Err(e)) => Err(format!("expression failed at runtime: {e}")),
        }
    }

    /// Evaluate the `fold` reducer, returning the merged payload.
    ///
    /// Returns an error if the expression fails at runtime, produces no
    /// output, or produces more than one output. A reducer must yield
    /// exactly one merged payload; a multi-output expression indicates a
    /// misuse of jq that would silently lose data if we picked one.
    pub fn eval_fold(
        &self,
        existing: &serde_json::Value,
        new: &serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let existing_val = to_val(existing)
            .ok_or_else(|| "existing payload could not be interpreted as JSON".to_string())?;
        let new_val = to_val(new)
            .ok_or_else(|| "new payload could not be interpreted as JSON".to_string())?;

        let ctx =
            Ctx::<data::JustLut<Val>>::new(&self.fold.lut, Vars::new([existing_val, new_val]));

        let mut iter = self.fold.id.run((ctx, Val::Null)).map(unwrap_valr);
        let first = iter
            .next()
            .ok_or_else(|| "fold expression produced no output".to_string())?
            .map_err(|e| format!("fold expression failed at runtime: {e}"))?;

        if iter.next().is_some() {
            return Err("fold expression produced more than one output".to_string());
        }

        val_to_json(&first)
    }
}

/// Structural conversion from a jaq `Val` to a `serde_json::Value`.
///
/// `jaq_json::Val` is a JSON superset — it can hold non-UTF-8 byte
/// strings, non-string object keys, and arbitrary-precision numbers
/// that don't fit into JSON. Each of those cases becomes a hard error
/// rather than silent lossy coercion. Everything else walks
/// structurally.
///
/// Swap this out for a `serde::Serialize`-based path if `jaq-json` ever
/// grows one.
fn val_to_json(v: &Val) -> Result<serde_json::Value, String> {
    match v {
        Val::Null => Ok(serde_json::Value::Null),
        Val::Bool(b) => Ok(serde_json::Value::Bool(*b)),
        Val::Num(n) => num_to_number(n).map(serde_json::Value::Number),
        Val::BStr(bytes) => bytes_to_string(bytes, "byte string"),
        Val::TStr(bytes) => bytes_to_string(bytes, "text string"),
        Val::Arr(vec) => {
            let items: Result<Vec<_>, _> = vec.iter().map(val_to_json).collect();
            items.map(serde_json::Value::Array)
        }
        Val::Obj(map) => {
            let mut obj = serde_json::Map::with_capacity(map.len());
            for (k, v) in map.iter() {
                let key = object_key(k)?;
                obj.insert(key, val_to_json(v)?);
            }
            Ok(serde_json::Value::Object(obj))
        }
    }
}

fn bytes_to_string(bytes: &[u8], kind: &str) -> Result<serde_json::Value, String> {
    std::str::from_utf8(bytes)
        .map(|s| serde_json::Value::String(s.to_string()))
        .map_err(|e| format!("{kind} is not valid UTF-8: {e}"))
}

fn object_key(v: &Val) -> Result<String, String> {
    match v {
        Val::BStr(bytes) | Val::TStr(bytes) => std::str::from_utf8(bytes)
            .map(str::to_string)
            .map_err(|e| format!("object key is not valid UTF-8: {e}")),
        other => Err(format!(
            "object key is not a string (got {})",
            type_name(other)
        )),
    }
}

fn type_name(v: &Val) -> &'static str {
    match v {
        Val::Null => "null",
        Val::Bool(_) => "bool",
        Val::Num(_) => "number",
        Val::BStr(_) | Val::TStr(_) => "string",
        Val::Arr(_) => "array",
        Val::Obj(_) => "object",
    }
}

fn num_to_number(n: &Num) -> Result<serde_json::Number, String> {
    match n {
        Num::Int(i) => Ok(serde_json::Number::from(*i)),
        Num::BigInt(bi) => {
            // Round-trip via a decimal string to avoid depending on
            // num_traits directly. serde_json's parser reads bare
            // integers into a Number without loss when they fit i64/u64;
            // wider values may lose precision or fail depending on
            // features — surface any failure as a hard error rather
            // than silently coercing.
            parse_number_str(&bi.to_string(), "big integer")
        }
        Num::Float(f) => serde_json::Number::from_f64(*f).ok_or_else(|| {
            format!("floating-point value {f} is not representable in JSON (NaN or Infinity)")
        }),
        Num::Dec(s) => parse_number_str(s, "decimal number"),
    }
}

fn parse_number_str(s: &str, kind: &str) -> Result<serde_json::Number, String> {
    match serde_json::from_str::<serde_json::Value>(s) {
        Ok(serde_json::Value::Number(n)) => Ok(n),
        Ok(other) => Err(format!(
            "{kind} \"{s}\" parses to a non-numeric JSON value: {other}"
        )),
        Err(e) => Err(format!("{kind} \"{s}\" is not representable in JSON: {e}")),
    }
}

fn compile_one(expression: &str) -> Result<jaq_core::Filter<data::JustLut<Val>>, String> {
    let arena = Arena::default();
    let loader = Loader::new(
        jaq_core::defs()
            .chain(jaq_std::defs())
            .chain(jaq_json::defs()),
    );

    let modules = loader
        .load(
            &arena,
            File {
                path: (),
                code: expression,
            },
        )
        .map_err(format_load_errors)?;

    Compiler::default()
        .with_funs(
            jaq_core::funs()
                .chain(jaq_std::funs())
                .chain(jaq_json::funs()),
        )
        .with_global_vars(["$existing", "$new"])
        .compile(modules)
        .map_err(format_compile_errors)
}

fn to_val(v: &serde_json::Value) -> Option<Val> {
    serde_json::from_value::<Val>(v.clone()).ok()
}

fn is_truthy(v: &Val) -> bool {
    !matches!(v, Val::Bool(false) | Val::Null)
}

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
        "invalid expression".to_string()
    } else {
        messages.join("; ")
    }
}

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
        "failed to compile expression".to_string()
    } else {
        messages.join("; ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn compile_simple_expressions() {
        BatchExpr::compile("true", "$existing").unwrap();
    }

    #[test]
    fn compile_reports_when_expression_errors() {
        let err = BatchExpr::compile(".[*]", "$existing").unwrap_err();
        assert!(err.starts_with("when: "), "got: {err}");
    }

    #[test]
    fn compile_reports_fold_expression_errors() {
        let err = BatchExpr::compile("true", ".[*]").unwrap_err();
        assert!(err.starts_with("fold: "), "got: {err}");
    }

    #[test]
    fn compile_allows_existing_and_new_globals() {
        BatchExpr::compile(
            "($existing.items | length) < 3",
            "$existing | .items += $new.items",
        )
        .unwrap();
    }

    #[test]
    fn compile_rejects_undefined_globals() {
        let err = BatchExpr::compile("$bogus", "$existing").unwrap_err();
        assert!(
            err.contains("undefined") && err.contains("bogus"),
            "got: {err}"
        );
    }

    #[test]
    fn eval_when_true_literal() {
        let expr = BatchExpr::compile("true", "$existing").unwrap();
        assert!(expr.eval_when(&json!({}), &json!({})));
    }

    #[test]
    fn eval_when_false_literal() {
        let expr = BatchExpr::compile("false", "$existing").unwrap();
        assert!(!expr.eval_when(&json!({}), &json!({})));
    }

    #[test]
    fn eval_when_uses_existing_variable() {
        let expr = BatchExpr::compile("($existing.items | length) < 3", "$existing").unwrap();
        assert!(expr.eval_when(&json!({"items": [1, 2]}), &json!({})));
        assert!(!expr.eval_when(&json!({"items": [1, 2, 3]}), &json!({})));
    }

    #[test]
    fn eval_when_uses_new_variable() {
        let expr = BatchExpr::compile("$new.ok == true", "$existing").unwrap();
        assert!(expr.eval_when(&json!({}), &json!({"ok": true})));
        assert!(!expr.eval_when(&json!({}), &json!({"ok": false})));
    }

    #[test]
    fn eval_when_null_output_is_false() {
        let expr = BatchExpr::compile("null", "$existing").unwrap();
        assert!(!expr.eval_when(&json!({}), &json!({})));
    }

    #[test]
    fn eval_when_runtime_error_is_false() {
        // Indexing a number is a jq runtime error.
        let expr = BatchExpr::compile("$existing[0]", "$existing").unwrap();
        assert!(!expr.eval_when(&json!(42), &json!({})));
    }

    #[test]
    fn eval_fold_concatenates_arrays() {
        let expr = BatchExpr::compile("true", "$existing + $new").unwrap();
        let out = expr.eval_fold(&json!([1, 2]), &json!([3, 4])).unwrap();
        assert_eq!(out, json!([1, 2, 3, 4]));
    }

    #[test]
    fn eval_fold_appends_into_nested_field() {
        let expr = BatchExpr::compile("true", "$existing | .items += $new.items").unwrap();
        let out = expr
            .eval_fold(&json!({"items": [1]}), &json!({"items": [2, 3]}))
            .unwrap();
        assert_eq!(out, json!({"items": [1, 2, 3]}));
    }

    #[test]
    fn eval_fold_last_wins() {
        let expr = BatchExpr::compile("true", "$new").unwrap();
        let out = expr.eval_fold(&json!({"a": 1}), &json!({"b": 2})).unwrap();
        assert_eq!(out, json!({"b": 2}));
    }

    #[test]
    fn eval_fold_runtime_error_returns_err() {
        // `.a + .b` on non-object inputs errors at runtime.
        let expr = BatchExpr::compile("true", "$existing.a + $new.b").unwrap();
        let err = expr.eval_fold(&json!(1), &json!(2)).unwrap_err();
        assert!(err.contains("runtime"), "got: {err}");
    }

    #[test]
    fn eval_fold_rejects_multiple_outputs() {
        // The `,` operator emits multiple values; a reducer must produce
        // exactly one, so this is an error rather than a silent pick.
        let expr = BatchExpr::compile("true", "$existing, $new").unwrap();
        let err = expr
            .eval_fold(&json!("first"), &json!("second"))
            .unwrap_err();
        assert!(err.contains("more than one output"), "got: {err}");
    }

    #[test]
    fn eval_fold_dedup_via_unique() {
        let expr = BatchExpr::compile(
            "true",
            "$existing | .device_ids = (.device_ids + $new.device_ids | unique)",
        )
        .unwrap();
        let out = expr
            .eval_fold(
                &json!({"device_ids": ["a", "b"]}),
                &json!({"device_ids": ["b", "c"]}),
            )
            .unwrap();
        assert_eq!(out, json!({"device_ids": ["a", "b", "c"]}));
    }

    #[test]
    fn eval_fold_pipe_into_compound_assign() {
        // User-reported expression: `$existing | . += $new`.
        // Should produce $existing + $new (array concat).
        let expr = BatchExpr::compile("true", "$existing | . += $new").unwrap();
        let out = expr
            .eval_fold(&json!([{"a": 1}]), &json!([{"a": 2}]))
            .unwrap();
        assert_eq!(out, json!([{"a": 1}, {"a": 2}]));
    }

    #[test]
    fn eval_fold_pipe_into_array_constructor_using_input() {
        // User-reported expression: `$existing | [.[], $new[]]`.
        let expr = BatchExpr::compile("true", "$existing | [.[], $new[]]").unwrap();
        let out = expr
            .eval_fold(&json!([{"a": 1}]), &json!([{"a": 2}]))
            .unwrap();
        assert_eq!(out, json!([{"a": 1}, {"a": 2}]));
    }

    #[test]
    fn eval_fold_array_constructor_from_both_vars() {
        // User-reported expression: `$existing | [$existing[], $new[]]`.
        let expr = BatchExpr::compile("true", "$existing | [$existing[], $new[]]").unwrap();
        let out = expr
            .eval_fold(&json!([{"a": 1}]), &json!([{"a": 2}]))
            .unwrap();
        assert_eq!(out, json!([{"a": 1}, {"a": 2}]));
    }

    #[test]
    fn eval_fold_preserves_integer_precision() {
        let expr = BatchExpr::compile("true", "9007199254740993").unwrap();
        // 2^53 + 1 cannot be represented exactly as an f64, so a naive
        // format-and-reparse path could round-trip it as 9007199254740992.
        let out = expr.eval_fold(&json!({}), &json!({})).unwrap();
        assert_eq!(out, json!(9007199254740993_i64));
    }

    #[test]
    fn eval_fold_preserves_negative_integer_precision() {
        let expr = BatchExpr::compile("true", "-9007199254740993").unwrap();
        let out = expr.eval_fold(&json!({}), &json!({})).unwrap();
        assert_eq!(out, json!(-9007199254740993_i64));
    }

    #[test]
    fn eval_fold_preserves_nested_objects_and_arrays() {
        let expr = BatchExpr::compile("true", "$existing + $new").unwrap();
        let out = expr
            .eval_fold(
                &json!({"a": [1, 2], "b": {"c": "d"}}),
                &json!({"e": null, "f": true}),
            )
            .unwrap();
        assert_eq!(
            out,
            json!({"a": [1, 2], "b": {"c": "d"}, "e": null, "f": true})
        );
    }

    #[test]
    fn eval_fold_preserves_null_and_boolean_variants() {
        let expr = BatchExpr::compile("true", "[$existing, $new, null, false]").unwrap();
        let out = expr.eval_fold(&json!(true), &json!(null)).unwrap();
        assert_eq!(out, json!([true, null, null, false]));
    }

    #[test]
    fn dry_run_accepts_shape_compatible_expressions() {
        let expr = BatchExpr::compile(
            "($existing.items | length) < 3",
            "$existing | .items += $new.items",
        )
        .unwrap();
        expr.dry_run(&json!({"items": [1]})).unwrap();
    }

    #[test]
    fn dry_run_rejects_shape_incompatible_when() {
        // Indexing an array with a string field errors in jq. Missing
        // *object* keys silently return null, so this uses an array
        // payload to force the error.
        let expr = BatchExpr::compile("$existing.items == null", "$existing").unwrap();
        let err = expr.dry_run(&json!([1, 2])).unwrap_err();
        assert!(err.starts_with("when: "), "got: {err}");
    }

    #[test]
    fn dry_run_rejects_shape_incompatible_fold() {
        // Array indexing with a string key errors in jq.
        let expr = BatchExpr::compile("true", "$existing | .foo += $new.foo").unwrap();
        let err = expr.dry_run(&json!([1, 2])).unwrap_err();
        assert!(err.starts_with("fold: "), "got: {err}");
    }

    #[test]
    fn dry_run_rejects_multi_output_fold() {
        let expr = BatchExpr::compile("true", "$existing, $new").unwrap();
        let err = expr.dry_run(&json!({})).unwrap_err();
        assert!(err.starts_with("fold: "), "got: {err}");
    }

    #[test]
    fn dry_run_accepts_false_predicate() {
        // A `when` that always returns false is legal — it just means the
        // batch always seals rather than folds. Dry-run should still pass.
        let expr = BatchExpr::compile("false", "$existing + $new").unwrap();
        expr.dry_run(&json!([1])).unwrap();
    }

    #[test]
    fn expressions_are_reusable() {
        let expr = BatchExpr::compile(
            "($existing.items | length) < 3",
            "$existing | .items += $new.items",
        )
        .unwrap();

        assert!(expr.eval_when(&json!({"items": [1]}), &json!({"items": [2]})));
        let out = expr
            .eval_fold(&json!({"items": [1]}), &json!({"items": [2]}))
            .unwrap();
        assert_eq!(out, json!({"items": [1, 2]}));

        // Reuse against different inputs.
        assert!(!expr.eval_when(&json!({"items": [1, 2, 3]}), &json!({"items": [4]})));
    }
}
