
use arrow::array::{Array, ArrayRef, OffsetSizeTrait, GenericStringArray};
use arrow::array::{ArrayAccessor, StringArray, LargeStringArray, StringViewArray};
use arrow::datatypes::{DataType, Int64Type};
use datafusion_common::{exec_err, plan_err, ScalarValue};
use datafusion_expr::ColumnarValue;
use datafusion_expr::TypeSignature;
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_expr::{Documentation, ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;
use regex::Regex;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

#[user_doc(
    doc_section(label = "Regular Expression Functions"),
    description = "Extracts a specific capture group matched by a regular expression from the specified string. If the regex did not match, or the specified group did not match, an empty string is returned.",
    syntax_example = "regexp_extract(str, regexp[, idx][, flags])",
    sql_example = r#"```sql
> SELECT regexp_extract('100-200', '(\\d+)-(\\d+)', 1);
+--------------------------------------------------------------+
| regexp_extract(Utf8("100-200"),Utf8("(\\d+)-(\\d+)"),Int64(1)) |
+--------------------------------------------------------------+
| 100                                                          |
+--------------------------------------------------------------+
> SELECT regexp_extract('100-200', '(\\d+)-(\\d+)', 2);
+--------------------------------------------------------------+
| regexp_extract(Utf8("100-200"),Utf8("(\\d+)-(\\d+)"),Int64(2)) |
+--------------------------------------------------------------+
| 200                                                          |
+--------------------------------------------------------------+
> SELECT regexp_extract('100-200', '(\\d+)-(\\d+)', 0);
+--------------------------------------------------------------+
| regexp_extract(Utf8("100-200"),Utf8("(\\d+)-(\\d+)"),Int64(0)) |
+--------------------------------------------------------------+
| 100-200                                                      |
+--------------------------------------------------------------+
> SELECT regexp_extract('abc123def', '[0-9]+', 0);
+-------------------------------------------------------+
| regexp_extract(Utf8("abc123def"),Utf8("[0-9]+"),Int64(0)) |
+-------------------------------------------------------+
| 123                                                   |
+-------------------------------------------------------+
> SELECT regexp_extract('foobar', 'bar', 3);
+-------------------------------------------------------+
| regexp_extract(Utf8("foobar"),Utf8("bar"),Int64(3)) |
+-------------------------------------------------------+
|                                                       |
+-------------------------------------------------------+
```
Additional examples can be found [here](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/regexp.rs)
"#,
    standard_argument(name = "str", prefix = "String"),
    argument(
        name = "regexp",
        description = "Regular expression to match against. Can be a constant, column, or function."
    ),
    argument(
        name = "idx",
        description = "The index of the capture group to extract (1-indexed). Index 0 returns the entire match. If not specified, defaults to 1. Can be a constant, column, or function."
    ),
    argument(
        name = "flags",
        description = r#"Optional regular expression flags that control the behavior of the regular expression. The following flags are supported:
- **i**: case-insensitive: letters match both upper and lower case
- **m**: multi-line mode: ^ and $ match begin/end of line
- **s**: allow . to match \n
- **R**: enables CRLF mode: when multi-line mode is enabled, \r\n is used
- **U**: swap the meaning of x* and x*?"#
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RegexpExtractFunc {
    signature: Signature,
}
impl Default for RegexpExtractFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpExtractFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    // 2-arg variants: str, regexp (idx defaults to 1)
                    TypeSignature::Exact(vec![Utf8, Utf8]),
                    TypeSignature::Exact(vec![LargeUtf8, LargeUtf8]),
                    TypeSignature::Exact(vec![Utf8View, Utf8View]),
                    // 3-arg variants: str, regexp, idx
                    TypeSignature::Exact(vec![Utf8, Utf8, Int64]),
                    TypeSignature::Exact(vec![LargeUtf8, LargeUtf8, Int64]),
                    TypeSignature::Exact(vec![Utf8View, Utf8View, Int64]),
                    // 3-arg variants: str, regexp, flags (idx defaults to 1)
                    TypeSignature::Exact(vec![Utf8, Utf8, Utf8]),
                    TypeSignature::Exact(vec![LargeUtf8, LargeUtf8, LargeUtf8]),
                    TypeSignature::Exact(vec![Utf8View, Utf8View, Utf8View]),
                    // 4-arg variants: str, regexp, idx, flags
                    TypeSignature::Exact(vec![Utf8, Utf8, Int64, Utf8]),
                    TypeSignature::Exact(vec![LargeUtf8, LargeUtf8, Int64, LargeUtf8]),
                    TypeSignature::Exact(vec![Utf8View, Utf8View, Int64, Utf8View]),
                ],
                Volatility::Immutable,
            ),
        }
    }    
}

impl ScalarUDFImpl for RegexpExtractFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_extract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;
        Ok(match &arg_types[0] {
            LargeUtf8 => LargeUtf8,
            Utf8 => Utf8,
            Utf8View => Utf8View,
            Null => Utf8,
            other => {
                return plan_err!(
                    "The regexp_extract function can only accept strings. Got {other}"
                );
            }
        })
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let args = &args.args;
        
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let is_scalar = len.is_none();
        let result = regexp_extract_func(args);
        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn regexp_extract_func(args: &[ColumnarValue]) -> Result<ArrayRef> {
    let inferred_length = args
        .iter()
        .find_map(|arg| match arg {
            ColumnarValue::Array(a) => Some(a.len()),
            _ => None,
        })
        .unwrap_or(1);

    let mut processed_args = args
        .iter()
        .map(|arg| arg.to_array(inferred_length))
        .collect::<Result<Vec<_>>>()?;

    // Handle optional idx parameter
    // If we have 2 args (str, pattern) or 3 args where the 3rd is a string (str, pattern, flags),
    // we need to insert a default idx value of 1
    let needs_default_idx = match processed_args.len() {
        2 => true, // (str, pattern) - missing both idx and flags
        3 => {
            // Check if 3rd argument is a string (flags) rather than int64 (idx)
            matches!(processed_args[2].data_type(), DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
        },
        _ => false,
    };

    if needs_default_idx {
        // Create default idx array with value 1
        let default_idx = Arc::new(arrow::array::Int64Array::from(
            vec![1i64; inferred_length]
        ));
        
        if processed_args.len() == 2 {
            // Insert idx at position 2: (str, pattern) -> (str, pattern, idx)
            processed_args.push(default_idx);
        } else {
            // Insert idx at position 2, shift flags to position 3: (str, pattern, flags) -> (str, pattern, idx, flags)
            let flags = processed_args.pop().unwrap();
            processed_args.push(default_idx);
            processed_args.push(flags);
        }
    }

    match processed_args[0].data_type() {
        DataType::Utf8 => regexp_extract_impl::<i32>(&processed_args),
        DataType::LargeUtf8 => regexp_extract_impl::<i64>(&processed_args),
        DataType::Utf8View => regexp_extract_utf8view(&processed_args),
        other => {
            internal_err!("Unsupported data type {other:?} for function regexp_extract")
        }
    }
}

fn regexp_extract_impl<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = args[0]
        .as_any()
        .downcast_ref::<GenericStringArray<T>>()
        .ok_or_else(|| {
            DataFusionError::Internal("Failed to downcast to StringArray".to_string())
        })?;
    
    let pattern_array = args[1]
        .as_any()
        .downcast_ref::<GenericStringArray<T>>()
        .ok_or_else(|| {
            DataFusionError::Internal("Failed to downcast pattern to StringArray".to_string())
        })?;
    
    let group_array = args[2]
        .as_any()
        .downcast_ref::<arrow::array::PrimitiveArray<Int64Type>>()
        .ok_or_else(|| {
            DataFusionError::Internal("Failed to downcast group to Int64Array".to_string())
        })?;
    
    let flags = if args.len() > 3 {
        Some(&args[3])
    } else {
        None
    };

    regexp_extract_generic(string_array, pattern_array, group_array, flags)
}

fn regexp_extract_utf8view(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = args[0]
        .as_any()
        .downcast_ref::<StringViewArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("Failed to downcast to StringViewArray".to_string())
        })?;
    
    let pattern_array = args[1]
        .as_any()
        .downcast_ref::<StringViewArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("Failed to downcast pattern to StringViewArray".to_string())
        })?;
    
    let group_array = args[2]
        .as_any()
        .downcast_ref::<arrow::array::PrimitiveArray<Int64Type>>()
        .ok_or_else(|| {
            DataFusionError::Internal("Failed to downcast group to Int64Array".to_string())
        })?;
    
    let flags = if args.len() > 3 {
        Some(&args[3])
    } else {
        None
    };

    regexp_extract_generic(string_array, pattern_array, group_array, flags)
}

fn regexp_extract_generic<'a, S, P>(
    string_array: S,
    pattern_array: P,
    group_array: &arrow::array::PrimitiveArray<Int64Type>,
    flags: Option<&ArrayRef>,
) -> Result<ArrayRef>
where
    S: ArrayAccessor<Item = &'a str>,
    P: ArrayAccessor<Item = &'a str>,
{
    let mut builder: arrow::array::StringBuilder = arrow::array::StringBuilder::with_capacity(
        string_array.len(),
        string_array.len() * 32,
    );
    
    let mut regex_cache: HashMap<(&str, Option<&str>), Regex> = HashMap::new();
    
    for i in 0..string_array.len() {
        if string_array.is_null(i) || pattern_array.is_null(i) || group_array.is_null(i) {
            builder.append_null();
            continue;
        }
        
        let str_val = string_array.value(i);
        let pattern = pattern_array.value(i);
        let group_idx = group_array.value(i);
        
        // Get flags if provided
        let flags_str = if let Some(f) = flags {
            match f.data_type() {
                DataType::Utf8 => {
                    let arr = f.as_any().downcast_ref::<StringArray>().unwrap();
                    if arr.is_null(i) { None } else { Some(arr.value(i)) }
                }
                DataType::LargeUtf8 => {
                    let arr = f.as_any().downcast_ref::<LargeStringArray>().unwrap();
                    if arr.is_null(i) { None } else { Some(arr.value(i)) }
                }
                DataType::Utf8View => {
                    let arr = f.as_any().downcast_ref::<StringViewArray>().unwrap();
                    if arr.is_null(i) { None } else { Some(arr.value(i)) }
                }
                _ => None,
            }
        } else {
            None
        };
        
        // Check for global flag which is not supported
        if let Some(flags) = flags_str {
            if flags.contains('g') {
                return exec_err!("regexp_extract() does not support the global 'g' flag");
            }
        }
        
        // Compile regex with caching
        let regex = super::compile_and_cache_regex(
            pattern,
            flags_str,
            &mut regex_cache,
        ).map_err(|e| DataFusionError::Execution(format!("Regex error: {e}")))?;
        
        // Validate group index
        if group_idx < 0 {
            return exec_err!("regexp_extract() group index must be non-negative, got {}", group_idx);
        }
        
        let result = if let Some(captures) = regex.captures(str_val) {
            if group_idx == 0 {
                // Group 0 returns the entire match
                captures.get(0).map_or("", |m| m.as_str())
            } else {
                // Groups are 1-indexed in Spark semantics
                let group_idx = group_idx as usize;
                captures.get(group_idx).map_or("", |m| m.as_str())
            }
        } else {
            // No match returns empty string
            ""
        };
        
        builder.append_value(result);
    }
    
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    
    #[test]
    fn test_regexp_extract_basic() {
        let strings = StringArray::from(vec![
            "100-200",
            "300-400",
            "no-match",
            "500-600",
        ]);
        let patterns = StringArray::from(vec!["(\\d+)-(\\d+)"; 4]);
        let groups = Int64Array::from(vec![1, 2, 1, 0]);
        
        let result = regexp_extract_impl::<i32>(&[
            Arc::new(strings),
            Arc::new(patterns),
            Arc::new(groups),
        ]).unwrap();
        
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "100");
        assert_eq!(result_array.value(1), "400");
        assert_eq!(result_array.value(2), "");
        assert_eq!(result_array.value(3), "500-600");
    }
    
    #[test]
    fn test_regexp_extract_with_flags() {
        let strings = StringArray::from(vec!["ABC", "abc", "AbC"]);
        let patterns = StringArray::from(vec!["(a)(b)(c)"; 3]);
        let groups = Int64Array::from(vec![2, 2, 0]);
        let flags = StringArray::from(vec!["i"; 3]);
        
        let result = regexp_extract_impl::<i32>(&[
            Arc::new(strings),
            Arc::new(patterns),
            Arc::new(groups),
            Arc::new(flags),
        ]).unwrap();
        
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "B");
        assert_eq!(result_array.value(1), "b");
        assert_eq!(result_array.value(2), "AbC");
    }
    
    #[test]
    fn test_regexp_extract_nulls() {
        let strings = StringArray::from(vec![
            Some("test123"),
            None,
            Some("test456"),
        ]);
        let patterns = StringArray::from(vec![
            Some("(\\d+)"),
            Some("(\\d+)"),
            Some("(\\d+)"),
        ]);
        let groups = Int64Array::from(vec![Some(1), Some(1), None]);
        
        let result = regexp_extract_impl::<i32>(&[
            Arc::new(strings),
            Arc::new(patterns),
            Arc::new(groups),
        ]).unwrap();
        
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "123");
        assert!(result_array.is_null(1));
        assert!(result_array.is_null(2));
    }
    
    #[test]
    fn test_regexp_extract_group_boundaries() {
        let strings = StringArray::from(vec![
            "a1b2c3",
            "a1b2c3",
            "a1b2c3",
            "a1b2c3",
        ]);
        let patterns = StringArray::from(vec!["(\\w)(\\d)"; 4]);
        let groups = Int64Array::from(vec![0, 1, 2, 3]);
        
        let result = regexp_extract_impl::<i32>(&[
            Arc::new(strings),
            Arc::new(patterns),
            Arc::new(groups),
        ]).unwrap();
        
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "a1"); // Group 0: entire match
        assert_eq!(result_array.value(1), "a");  // Group 1: first capture
        assert_eq!(result_array.value(2), "1");  // Group 2: second capture
        assert_eq!(result_array.value(3), "");   // Group 3: out of bounds
    }
    
    #[test]
    fn test_regexp_extract_negative_index() {
        let strings = StringArray::from(vec!["test123"]);
        let patterns = StringArray::from(vec!["(\\d+)"]);
        let groups = Int64Array::from(vec![-1]);
        
        let result = regexp_extract_impl::<i32>(&[
            Arc::new(strings),
            Arc::new(patterns),
            Arc::new(groups),
        ]);
        
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("group index must be non-negative"));
    }
    
    #[test]
    fn test_regexp_extract_empty_string() {
        let strings = StringArray::from(vec!["", "test", ""]);
        let patterns = StringArray::from(vec!["(\\w+)"; 3]);
        let groups = Int64Array::from(vec![1; 3]);
        
        let result = regexp_extract_impl::<i32>(&[
            Arc::new(strings),
            Arc::new(patterns),
            Arc::new(groups),
        ]).unwrap();
        
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "");
        assert_eq!(result_array.value(1), "test");
        assert_eq!(result_array.value(2), "");
    }
    
    #[test]
    fn test_regexp_extract_unicode() {
        let strings = StringArray::from(vec![
            "Hello 世界",
            "Привет мир",
            "🎉 celebration",
        ]);
        let patterns = StringArray::from(vec![
            "(\\w+)\\s+(\\w+)",
            "(\\w+)\\s+(\\w+)",
            "(\\S+)\\s+(\\w+)",
        ]);
        let groups = Int64Array::from(vec![2, 1, 1]);
        
        let result = regexp_extract_impl::<i32>(&[
            Arc::new(strings),
            Arc::new(patterns),
            Arc::new(groups),
        ]).unwrap();
        
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "世界");
        assert_eq!(result_array.value(1), "Привет");
        assert_eq!(result_array.value(2), "🎉");
    }
    
    #[test]
    fn test_regexp_extract_complex_pattern() {
        let strings = StringArray::from(vec![
            "user@example.com",
            "admin@test.org",
            "invalid-email",
        ]);
        let patterns = StringArray::from(vec![
            "^([^@]+)@([^@]+)$"; 3
        ]);
        let groups = Int64Array::from(vec![1, 2, 0]);
        
        let result = regexp_extract_impl::<i32>(&[
            Arc::new(strings),
            Arc::new(patterns),
            Arc::new(groups),
        ]).unwrap();
        
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "user");
        assert_eq!(result_array.value(1), "test.org");
        assert_eq!(result_array.value(2), "");
    }
    
    #[test]
    fn test_regexp_extract_multiline_flag() {
        let strings = StringArray::from(vec![
            "line1\nline2",
            "first\nsecond",
        ]);
        let patterns = StringArray::from(vec![
            "^(\\w+)$"; 2
        ]);
        let groups = Int64Array::from(vec![1; 2]);
        let flags = StringArray::from(vec!["m"; 2]);
        
        let result = regexp_extract_impl::<i32>(&[
            Arc::new(strings),
            Arc::new(patterns),
            Arc::new(groups),
            Arc::new(flags),
        ]).unwrap();
        
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "line1");
        assert_eq!(result_array.value(1), "first");
    }
    
    #[test]
    fn test_regexp_extract_global_flag_error() {
        let strings = StringArray::from(vec!["test"]);
        let patterns = StringArray::from(vec!["(\\w+)"]);
        let groups = Int64Array::from(vec![1]);
        let flags = StringArray::from(vec!["g"]);
        
        let result = regexp_extract_impl::<i32>(&[
            Arc::new(strings),
            Arc::new(patterns),
            Arc::new(groups),
            Arc::new(flags),
        ]);
        
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("global"));
    }
    
    #[test] 
    fn test_regexp_extract_large_string() {
        use arrow::array::LargeStringArray;
        
        let strings = LargeStringArray::from(vec![
            "test-123-abc",
            "prod-456-xyz",
        ]);
        let patterns = LargeStringArray::from(vec![
            "(\\w+)-(\\d+)-(\\w+)"; 2
        ]);
        let groups = Int64Array::from(vec![2, 3]);
        
        let result = regexp_extract_impl::<i64>(&[
            Arc::new(strings),
            Arc::new(patterns),
            Arc::new(groups),
        ]).unwrap();
        
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "123");
        assert_eq!(result_array.value(1), "xyz");
    }
    
    #[test]
    fn test_regexp_extract_utf8view() {
        let mut builder = arrow::array::StringViewBuilder::new();
        builder.append_value("hello-123-world");
        builder.append_value("foo-456-bar");
        let strings = builder.finish();
        
        let mut pattern_builder = arrow::array::StringViewBuilder::new();
        pattern_builder.append_value("(\\w+)-(\\d+)-(\\w+)");
        pattern_builder.append_value("(\\w+)-(\\d+)-(\\w+)");
        let patterns = pattern_builder.finish();
        
        let groups = Int64Array::from(vec![1, 2]);
        
        let result = regexp_extract_utf8view(&[
            Arc::new(strings),
            Arc::new(patterns),
            Arc::new(groups),
        ]).unwrap();
        
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "hello");
        assert_eq!(result_array.value(1), "456");
    }
    
    #[test]
    fn test_regexp_extract_optional_group_not_matched() {
        let strings = StringArray::from(vec![
            "abc",
            "abc:123",
            "def",
        ]);
        let patterns = StringArray::from(vec![
            "([a-z]+):?(\\d+)?",
            "([a-z]+):?(\\d+)?",  
            "([a-z]+):?(\\d+)?",
        ]);
        let groups = Int64Array::from(vec![2, 2, 2]);
        
        let result = regexp_extract_impl::<i32>(&[
            Arc::new(strings),
            Arc::new(patterns),
            Arc::new(groups),
        ]).unwrap();
        
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "");     // Group 2 doesn't match for "abc" (no digits)
        assert_eq!(result_array.value(1), "123");  // Group 2 matches for "abc:123"
        assert_eq!(result_array.value(2), "");     // Group 2 doesn't match for "def" (no digits)
    }
    
    #[test]
    fn test_regexp_extract_empty_pattern() {
        // Test empty pattern which matches at the beginning of the string
        let strings = StringArray::from(vec![
            "test",
            "abc",
            "",
        ]);
        let patterns = StringArray::from(vec![""; 3]);  // Empty pattern
        let groups = Int64Array::from(vec![0, 0, 0]);
        
        let result = regexp_extract_impl::<i32>(&[
            Arc::new(strings),
            Arc::new(patterns),
            Arc::new(groups),
        ]).unwrap();
        
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "");
        assert_eq!(result_array.value(1), "");
        assert_eq!(result_array.value(2), "");
    }
    
    #[test]
    fn test_regexp_extract_empty_pattern_with_group() {
        // Test empty pattern with group index > 0 (should return empty as there are no groups)
        let strings = StringArray::from(vec![
            "test",
            "abc",
        ]);
        let patterns = StringArray::from(vec![""; 2]);  // Empty pattern
        let groups = Int64Array::from(vec![1, 2]);
        
        let result = regexp_extract_impl::<i32>(&[
            Arc::new(strings),
            Arc::new(patterns),
            Arc::new(groups),
        ]).unwrap();
        
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "");
        assert_eq!(result_array.value(1), "");
    }
    
    #[test]
    fn test_regexp_extract_optional_idx_default() {
        // Test 2-arg form: str, pattern (idx defaults to 1)
        let strings = StringArray::from(vec![
            "100-200",
            "300-400",
            "no-match",
        ]);
        let patterns = StringArray::from(vec!["(\\d+)-(\\d+)"; 3]);
        
        let result = regexp_extract_func(&[
            ColumnarValue::Array(Arc::new(strings)),
            ColumnarValue::Array(Arc::new(patterns)),
        ]).unwrap();
        
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "100");
        assert_eq!(result_array.value(1), "300");
        assert_eq!(result_array.value(2), "");
    }
    
    #[test]
    fn test_regexp_extract_optional_idx_with_flags() {
        // Test 3-arg form: str, pattern, flags (idx defaults to 1)
        let strings = StringArray::from(vec!["ABC-123", "def-456"]);
        let patterns = StringArray::from(vec!["([a-z]+)-(\\d+)"; 2]);
        let flags = StringArray::from(vec!["i"; 2]); // case-insensitive
        
        let result = regexp_extract_func(&[
            ColumnarValue::Array(Arc::new(strings)),
            ColumnarValue::Array(Arc::new(patterns)),
            ColumnarValue::Array(Arc::new(flags)),
        ]).unwrap();
        
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "ABC");
        assert_eq!(result_array.value(1), "def");
    }
    
    #[test]
    fn test_regexp_extract_explicit_idx() {
        // Test 3-arg form: str, pattern, idx (explicitly specified)
        let strings = StringArray::from(vec!["100-200", "300-400"]);
        let patterns = StringArray::from(vec!["(\\d+)-(\\d+)"; 2]);
        let groups = Int64Array::from(vec![2, 1]); // Second group, then first group
        
        let result = regexp_extract_func(&[
            ColumnarValue::Array(Arc::new(strings)),
            ColumnarValue::Array(Arc::new(patterns)),
            ColumnarValue::Array(Arc::new(groups)),
        ]).unwrap();
        
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "200");
        assert_eq!(result_array.value(1), "300");
    }
    
    #[test]
    fn test_regexp_extract_all_args() {
        // Test 4-arg form: str, pattern, idx, flags
        let strings = StringArray::from(vec!["ABC-123", "def-456"]);
        let patterns = StringArray::from(vec!["([a-z]+)-(\\d+)"; 2]);
        let groups = Int64Array::from(vec![2, 1]); // Second group, then first group
        let flags = StringArray::from(vec!["i"; 2]); // case-insensitive
        
        let result = regexp_extract_func(&[
            ColumnarValue::Array(Arc::new(strings)),
            ColumnarValue::Array(Arc::new(patterns)),
            ColumnarValue::Array(Arc::new(groups)),
            ColumnarValue::Array(Arc::new(flags)),
        ]).unwrap();
        
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "123");
        assert_eq!(result_array.value(1), "def");
    }
    
    #[test]
    fn test_regexp_extract_scalar_optional_idx() {
        // Test scalar inputs with optional idx
        let string_scalar = ColumnarValue::Scalar(ScalarValue::Utf8(Some("test-123".to_string())));
        let pattern_scalar = ColumnarValue::Scalar(ScalarValue::Utf8(Some("([a-z]+)-(\\d+)".to_string())));
        
        let result = regexp_extract_func(&[
            string_scalar,
            pattern_scalar,
        ]).unwrap();
        
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "test");
    }
    
    #[test]
    fn test_regexp_extract_mixed_columnar_types() {
        // Test mixed scalar and array inputs
        let strings = StringArray::from(vec!["abc-123", "def-456"]);
        let pattern_scalar = ColumnarValue::Scalar(ScalarValue::Utf8(Some("([a-z]+)-(\\d+)".to_string())));
        
        let result = regexp_extract_func(&[
            ColumnarValue::Array(Arc::new(strings)),
            pattern_scalar,
        ]).unwrap();
        
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "abc");
        assert_eq!(result_array.value(1), "def");
    }
}