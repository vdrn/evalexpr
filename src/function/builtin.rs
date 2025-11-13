#[cfg(feature = "regex")]
use regex::Regex;

use crate::{
    Context, EvalexprError, Function, Value, ValueType, error::expect_function_argument_amount, value::numeric_types::{EvalexprFloat, EvalexprInt, EvalexprNumericTypes}
};

macro_rules! simple_math {
    ($func:ident) => {
        Some(Function::new(|_c, argument: &[Value<NumericTypes>]| {
            let num = argument
                .first()
                .ok_or_else(|| EvalexprError::wrong_function_argument_amount(0, 1))?
                .as_float()?;
            Ok(Value::Float(num.$func()))
        }))
    };
    ($func:ident, 2) => {
        Some(Function::new(|_c, argument: &[Value<NumericTypes>]| {
            let a = argument
                .first()
                .ok_or_else(|| EvalexprError::wrong_function_argument_amount(0, 2))?
                .as_float()?;
            let b = argument
                .first()
                .ok_or_else(|| EvalexprError::wrong_function_argument_amount(1, 2))?
                .as_float()?;

            Ok(Value::Float(a.$func(&b)))
        }))
    };
}

fn float_is<NumericTypes: EvalexprNumericTypes, C: Context<NumericTypes = NumericTypes>>(
    func: fn(&NumericTypes::Float) -> bool,
) -> Option<Function<NumericTypes, C>> {
    Some(Function::new(
        move |_c, argument: &[Value<NumericTypes>]| {
            Ok(func(
                &argument
                    .first()
                    .ok_or_else(|| EvalexprError::wrong_function_argument_amount(0, 1))?
                    .as_float()?,
            )
            .into())
        },
    ))
}

// macro_rules! int_function {
//     ($func:ident) => {
//         Some(Function::new(|argument| {
//             let int: NumericTypes::Int = argument.as_int()?;
//             Ok(Value::Int(int.$func()))
//         }))
//     };
//     ($func:ident, 2) => {
//         Some(Function::new(|argument| {
//             let tuple = argument.as_fixed_len_tuple(2)?;
//             let (a, b): (NumericTypes::Int, NumericTypes::Int) =
//                 (tuple[0].as_int()?, tuple[1].as_int()?);
//             Ok(Value::Int(a.$func(&b)))
//         }))
//     };
// }

pub fn builtin_function<
    NumericTypes: EvalexprNumericTypes,
    C: Context<NumericTypes = NumericTypes>,
>(
    identifier: &str,
) -> Option<Function<NumericTypes, C>> {
    match identifier {
        // Log
        "math::ln" => simple_math!(ln),
        "math::log" => simple_math!(log, 2),
        "math::log2" => simple_math!(log2),
        "math::log10" => simple_math!(log10),
        // Exp
        "math::exp" => simple_math!(exp),
        "math::exp2" => simple_math!(exp2),
        // Pow
        "math::pow" => simple_math!(pow, 2),
        // Cos
        "math::cos" => simple_math!(cos),
        "math::acos" => simple_math!(acos),
        "math::cosh" => simple_math!(cosh),
        "math::acosh" => simple_math!(acosh),
        // Sin
        "math::sin" => simple_math!(sin),
        "math::asin" => simple_math!(asin),
        "math::sinh" => simple_math!(sinh),
        "math::asinh" => simple_math!(asinh),
        // Tan
        "math::tan" => simple_math!(tan),
        "math::atan" => simple_math!(atan),
        "math::tanh" => simple_math!(tanh),
        "math::atanh" => simple_math!(atanh),
        "math::atan2" => simple_math!(atan2, 2),
        // Root
        "math::sqrt" => simple_math!(sqrt),
        "math::cbrt" => simple_math!(cbrt),
        // Hypotenuse
        "math::hypot" => simple_math!(hypot, 2),
        // Rounding
        "floor" => simple_math!(floor),
        "round" => simple_math!(round),
        "ceil" => simple_math!(ceil),
        // Float special values
        "math::is_nan" => float_is(NumericTypes::Float::is_nan),
        "math::is_finite" => float_is(NumericTypes::Float::is_finite),
        "math::is_infinite" => float_is(NumericTypes::Float::is_infinite),
        "math::is_normal" => float_is(NumericTypes::Float::is_normal),
        // Absolute value
        "math::abs" => Some(Function::new(|_c, argument| match argument.first() {
            Some(Value::Float(num)) => Ok(Value::Float(
                <NumericTypes as EvalexprNumericTypes>::Float::abs(num),
            )),
            // Value::Int(num) => Ok(Value::Int(
            //     <NumericTypes as EvalexprNumericTypes>::Int::abs(num)?,
            // )),
            Some(a) => Err(EvalexprError::expected_float(a.clone())),
            None => Err(EvalexprError::wrong_operator_argument_amount(0, 1)),
        })),
        // // Other
        // "typeof" => Some(Function::new(move |_c, argument| {
        //     Ok(match argument.first() {
        //         Value::String(_) => "string",
        //         Value::Float(_) => "float",
        //         // Value::Int(_) => "int",
        //         Value::Boolean(_) => "boolean",
        //         Value::Tuple(_) => "tuple",
        //         Value::Empty => "empty",
        //     }
        //     .into())
        // })),
        "min" => Some(Function::new(|_c, arguments| {
            // let arguments = argument.as_tuple_ref()?;
            let mut min_float = NumericTypes::Float::MAX;
            debug_assert!(min_float.is_infinite());

            for argument in arguments {
                if let Value::Float(float) = argument {
                    min_float = min_float.min(float);
                } else {
                    return Err(EvalexprError::expected_float(argument.clone()));
                }
            }

            Ok(Value::Float(min_float))
        })),
        "max" => Some(Function::new(|_c, arguments| {
            let mut max_float = NumericTypes::Float::MIN;
            debug_assert!(max_float.is_infinite());

            for argument in arguments {
                if let Value::Float(float) = argument {
                    max_float = max_float.max(float);
                } else {
                    return Err(EvalexprError::expected_float(argument.clone()));
                }
            }

            // if (NumericTypes::int_as_float(&max_int)) > max_float {
            //     Ok(Value::Int(max_int))
            // } else {
            Ok(Value::Float(max_float))
            // }
        })),
        "if" => Some(Function::new(|_c, arguments| {
            expect_function_argument_amount(arguments.len(), 3)?;
            let result_index = if arguments[0].as_boolean()? { 1 } else { 2 };
            Ok(arguments[result_index].clone())
        })),
        "contains" => Some(Function::new(move |_c, arguments| {
            expect_function_argument_amount(arguments.len(), 2)?;
            if let (Value::Tuple(a), b) = (&arguments[0].clone(), &arguments[1].clone()) {
                if let Value::Float(_) | Value::Boolean(_) = b {
                    Ok(a.contains(b).into())
                } else {
                    Err(EvalexprError::type_error(
                        b.clone(),
                        vec![
                            // ValueType::String,
                            // ValueType::Int,
                            ValueType::Float,
                            ValueType::Boolean,
                        ],
                    ))
                }
            } else {
                Err(EvalexprError::expected_tuple(arguments[0].clone()))
            }
        })),
        "contains_any" => Some(Function::new(move |_c, arguments| {
            expect_function_argument_amount(arguments.len(), 2)?;
            if let (Value::Tuple(a), b) = (&arguments[0].clone(), &arguments[1].clone()) {
                if let Value::Tuple(b) = b {
                    let mut contains = false;
                    for value in b {
                        if let //Value::String(_)
                        // | Value::Int(_) | 
                          Value::Float(_)
                        | Value::Boolean(_) = value
                        {
                            if a.contains(value) {
                                contains = true;
                            }
                        } else {
                            return Err(EvalexprError::type_error(
                                value.clone(),
                                vec![
                                    // ValueType::String,
                                    // ValueType::Int,
                                    ValueType::Float,
                                    ValueType::Boolean,
                                ],
                            ));
                        }
                    }
                    Ok(contains.into())
                } else {
                    Err(EvalexprError::expected_tuple(b.clone()))
                }
            } else {
                Err(EvalexprError::expected_tuple(arguments[0].clone()))
            }
        })),
        "len" => Some(Function::new(|_c, arguments| {
            expect_function_argument_amount(arguments.len(), 1)?;
            // if let Ok(subject) = arguments[0].as_str() {
            //     Ok(Value::Float(NumericTypes::int_as_float(
            //         &NumericTypes::Int::from_usize(subject.len())?,
            //     )))
            // } else 
              if let Ok(subject) = arguments[0].as_tuple_ref() {
                Ok(Value::Float(NumericTypes::int_as_float(
                    &NumericTypes::Int::from_usize(subject.len())?,
                )))
            } else {
                Err(EvalexprError::type_error(
                    arguments[0].clone(),
                    vec![ValueType::Tuple],
                ))
            }
        })),
        // String functions
        // #[cfg(feature = "regex")]
        // "str::regex_matches" => Some(Function::new(|arguments| {
        //     expect_function_argument_amount(arguments.len(), 2)?;

        //     let subject = arguments[0].as_string()?;
        //     let re_str = arguments[1].as_string()?;
        //     match Regex::new(&re_str) {
        //         Ok(re) => Ok(Value::Boolean(re.is_match(&subject))),
        //         Err(err) => Err(EvalexprError::invalid_regex(
        //             re_str.to_string(),
        //             format!("{}", err),
        //         )),
        //     }
        // })),
        // #[cfg(feature = "regex")]
        // "str::regex_replace" => Some(Function::new(|arguments| {
        //     expect_function_argument_amount(arguments.len(), 3)?;

        //     let subject = arguments[0].as_str()?;
        //     let re_str = arguments[1].as_str()?;
        //     let repl = arguments[2].as_str()?;
        //     match Regex::new(&re_str) {
        //         Ok(re) => Ok(Value::String(
        //             re.replace_all(&subject, repl.as_str()).to_string(),
        //         )),
        //         Err(err) => Err(EvalexprError::invalid_regex(
        //             re_str.to_string(),
        //             format!("{}", err),
        //         )),
        //     }
        // })),
        // "str::to_lowercase" => Some(Function::new(|_c, arguments| {
        //     expect_function_argument_amount(arguments.len(), 1)?;

        //     let subject = arguments[0].as_str()?;
        //     Ok(Value::from(subject.to_lowercase()))
        // })),
        // "str::to_uppercase" => Some(Function::new(|_c, arguments| {
        //     expect_function_argument_amount(arguments.len(), 1)?;
        //     let subject = arguments[0].as_str()?;
        //     Ok(Value::from(subject.to_uppercase()))
        // })),
        // "str::trim" => Some(Function::new(|_c, arguments| {
        //     expect_function_argument_amount(arguments.len(), 1)?;
        //     let subject = arguments[0].as_str()?;
        //     Ok(Value::from(subject.trim()))
        // })),
        // "str::from" => Some(Function::new(|_c, arguments| {
        //     expect_function_argument_amount(arguments.len(), 1)?;
        //     Ok(Value::String(arguments[0].str_from()))
        // })),
        // "str::substring" => Some(Function::new(|argument| {
        //     let args = argument.as_ranged_len_tuple(2..=3)?;
        //     let subject = args[0].as_string()?;
        //     let start: NumericTypes::Int = args[1].as_int()?;
        //     let start = start
        //         .into_usize()
        //         .map_err(|_| EvalexprError::OutOfBoundsAccess)?;
        //     let end = if let Some(end) = args.get(2) {
        //         let end: NumericTypes::Int = end.as_int()?;
        //         end.into_usize()
        //             .map_err(|_| EvalexprError::OutOfBoundsAccess)?
        //     } else {
        //         subject.len()
        //     };
        //     if start > end || end > subject.len() {
        //         return Err(EvalexprError::OutOfBoundsAccess);
        //     }
        //     Ok(Value::from(&subject[start..end]))
        // })),
        #[cfg(feature = "rand")]
        "random" => Some(Function::new(|argument| {
            argument.as_empty()?;
            Ok(Value::Float(NumericTypes::Float::random()?))
        })),
        // Bitwise operators
        // "bitand" => int_function!(bitand, 2),
        // "bitor" => int_function!(bitor, 2),
        // "bitxor" => int_function!(bitxor, 2),
        // "bitnot" => int_function!(bitnot),
        // "shl" => int_function!(bit_shift_left, 2),
        // "shr" => int_function!(bit_shift_right, 2),
        _ => None,
    }
}
