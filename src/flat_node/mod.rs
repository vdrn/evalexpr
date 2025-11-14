use smallvec::SmallVec;

use crate::{
    Context, ContextWithMutableVariables, DefaultNumericTypes, EMPTY_VALUE, EmptyType, EvalexprError, EvalexprFloat, EvalexprNumericTypes, EvalexprResult, F32NumericTypes, HashMapContext, IStr, Node, Operator, TupleType, Value, error::{EvalexprResultValue, expect_operator_argument_amount}, function::builtin::builtin_function
};

#[cold]
fn cold() {}
pub fn unlikely(x: bool) -> bool {
    if x {
        cold()
    }
    x
}

#[derive(Debug, Clone, PartialEq)]
// NOTE: while repr(C) costs us 8 bytes, it generates much nicer match in `eval_priv`
#[repr(C)]
pub enum FlatOperator<NumericTypes: EvalexprNumericTypes = DefaultNumericTypes> {
    // Arithmetic operators
    Add,
    Sub,
    Neg,
    Mul,
    Div,
    Mod,
    Exp,

    // Comparison operators
    Eq,
    Neq,
    Gt,
    Lt,
    Geq,
    Leq,

    // Logical operators
    And,
    Or,
    Not,

    // Assignment operators (may not be used in immutable context)
    Assign,
    AddAssign,
    SubAssign,
    MulAssign,
    DivAssign,
    ModAssign,
    ExpAssign,
    AndAssign,
    OrAssign,

    // Variable-length operators with explicit length
    /// Construct tuple from top `len` stack values
    Tuple {
        len: u32,
    },
    /// Execute `len` expressions, keep only the last result
    Chain {
        len: u32,
    },
    /// Call function with `len` arguments from stack
    FunctionCall {
        identifier: IStr,
        len: u32,
    },

    // Constants and variables
    /// Push constant value onto stack
    PushConst {
        value: Value<NumericTypes>,
    },
    /// Read variable and push onto stack
    ReadVar {
        identifier: IStr,
    },
    /// Write to variable (pops value from stack)
    WriteVar {
        identifier: IStr,
    },
}

/// Flat compiled node - linear sequence of operations
#[derive(Debug, Clone, PartialEq)]
pub struct FlatNode<NumericTypes: EvalexprNumericTypes = DefaultNumericTypes> {
    ops: Vec<FlatOperator<NumericTypes>>,
}

/// Helper type for stacks
pub type Stack<T> = Vec<Value<T>>;

/// Helper function to extract exactly one child node
fn extract_one_node<NumericTypes: EvalexprNumericTypes>(
    mut children: Vec<Node<NumericTypes>>,
) -> EvalexprResult<Node<NumericTypes>, NumericTypes> {
    expect_operator_argument_amount(children.len(), 1)?;
    Ok(children.pop().unwrap())
}

/// Helper function to extract exactly two child nodes
fn extract_two_nodes<NumericTypes: EvalexprNumericTypes>(
    mut children: Vec<Node<NumericTypes>>,
) -> EvalexprResult<[Node<NumericTypes>; 2], NumericTypes> {
    expect_operator_argument_amount(children.len(), 2)?;
    let b = children.pop().unwrap();
    let a = children.pop().unwrap();
    Ok([a, b])
}

fn into_u32<NumericTypes: EvalexprNumericTypes>(value: usize) -> EvalexprResult<u32, NumericTypes> {
    value.try_into().map_err(|_| EvalexprError::CustomMessage(format!("Length of tuples cannot exceed u32::MAX")))
}
/// Recursively compile a Node tree into flat operations
/// This validates the tree structure during compilation (like try_into for CompiledNode)
fn compile_to_flat_inner<NumericTypes: EvalexprNumericTypes>(
    node: Node<NumericTypes>,
    ops: &mut Vec<FlatOperator<NumericTypes>>,
) -> EvalexprResult<(), NumericTypes> {
    use Operator::*;

    match node.operator {
        RootNode => {
            if node.children.len() > 1 {
                return Err(EvalexprError::wrong_operator_argument_amount(
                    node.children.len(),
                    1,
                ));
            }

            if let Some(child) = node.children.into_iter().next() {
                compile_to_flat_inner(child, ops)?;
            } else {
                // Empty expression
                ops.push(FlatOperator::PushConst {
                    value: Value::Empty,
                });
            }
        },

        // Binary operators - compile left child, right child, then operation
        Add => {
            let [a, b] = extract_two_nodes(node.children)?;
            compile_to_flat_inner(a, ops)?;
            compile_to_flat_inner(b, ops)?;
            ops.push(FlatOperator::Add);
        },
        Sub => {
            let [a, b] = extract_two_nodes(node.children)?;
            compile_to_flat_inner(a, ops)?;
            compile_to_flat_inner(b, ops)?;
            ops.push(FlatOperator::Sub);
        },
        Mul => {
            let [a, b] = extract_two_nodes(node.children)?;
            compile_to_flat_inner(a, ops)?;
            compile_to_flat_inner(b, ops)?;
            ops.push(FlatOperator::Mul);
        },
        Div => {
            let [a, b] = extract_two_nodes(node.children)?;
            compile_to_flat_inner(a, ops)?;
            compile_to_flat_inner(b, ops)?;
            ops.push(FlatOperator::Div);
        },
        Mod => {
            let [a, b] = extract_two_nodes(node.children)?;
            compile_to_flat_inner(a, ops)?;
            compile_to_flat_inner(b, ops)?;
            ops.push(FlatOperator::Mod);
        },
        Exp => {
            let [a, b] = extract_two_nodes(node.children)?;
            compile_to_flat_inner(a, ops)?;
            compile_to_flat_inner(b, ops)?;
            ops.push(FlatOperator::Exp);
        },

        // Unary operators
        Neg => {
            let child = extract_one_node(node.children)?;
            compile_to_flat_inner(child, ops)?;
            ops.push(FlatOperator::Neg);
        },
        Not => {
            let child = extract_one_node(node.children)?;
            compile_to_flat_inner(child, ops)?;
            ops.push(FlatOperator::Not);
        },

        // Comparison operators
        Eq => {
            let [a, b] = extract_two_nodes(node.children)?;
            compile_to_flat_inner(a, ops)?;
            compile_to_flat_inner(b, ops)?;
            ops.push(FlatOperator::Eq);
        },
        Neq => {
            let [a, b] = extract_two_nodes(node.children)?;
            compile_to_flat_inner(a, ops)?;
            compile_to_flat_inner(b, ops)?;
            ops.push(FlatOperator::Neq);
        },
        Gt => {
            let [a, b] = extract_two_nodes(node.children)?;
            compile_to_flat_inner(a, ops)?;
            compile_to_flat_inner(b, ops)?;
            ops.push(FlatOperator::Gt);
        },
        Lt => {
            let [a, b] = extract_two_nodes(node.children)?;
            compile_to_flat_inner(a, ops)?;
            compile_to_flat_inner(b, ops)?;
            ops.push(FlatOperator::Lt);
        },
        Geq => {
            let [a, b] = extract_two_nodes(node.children)?;
            compile_to_flat_inner(a, ops)?;
            compile_to_flat_inner(b, ops)?;
            ops.push(FlatOperator::Geq);
        },
        Leq => {
            let [a, b] = extract_two_nodes(node.children)?;
            compile_to_flat_inner(a, ops)?;
            compile_to_flat_inner(b, ops)?;
            ops.push(FlatOperator::Leq);
        },

        // Logical operators
        And => {
            let [a, b] = extract_two_nodes(node.children)?;
            compile_to_flat_inner(a, ops)?;
            compile_to_flat_inner(b, ops)?;
            ops.push(FlatOperator::And);
        },
        Or => {
            let [a, b] = extract_two_nodes(node.children)?;
            compile_to_flat_inner(a, ops)?;
            compile_to_flat_inner(b, ops)?;
            ops.push(FlatOperator::Or);
        },

        // Assignment operators
        Assign => {
            let [a, b] = extract_two_nodes(node.children)?;
            compile_to_flat_inner(b, ops)?; // Value first
            compile_to_flat_inner(a, ops)?; // Variable second (should emit WriteVar)
            ops.push(FlatOperator::Assign);
        },
        AddAssign => {
            let [a, b] = extract_two_nodes(node.children)?;
            compile_to_flat_inner(b, ops)?;
            compile_to_flat_inner(a, ops)?;
            ops.push(FlatOperator::AddAssign);
        },
        SubAssign => {
            let [a, b] = extract_two_nodes(node.children)?;
            compile_to_flat_inner(b, ops)?;
            compile_to_flat_inner(a, ops)?;
            ops.push(FlatOperator::SubAssign);
        },
        MulAssign => {
            let [a, b] = extract_two_nodes(node.children)?;
            compile_to_flat_inner(b, ops)?;
            compile_to_flat_inner(a, ops)?;
            ops.push(FlatOperator::MulAssign);
        },
        DivAssign => {
            let [a, b] = extract_two_nodes(node.children)?;
            compile_to_flat_inner(b, ops)?;
            compile_to_flat_inner(a, ops)?;
            ops.push(FlatOperator::DivAssign);
        },
        ModAssign => {
            let [a, b] = extract_two_nodes(node.children)?;
            compile_to_flat_inner(b, ops)?;
            compile_to_flat_inner(a, ops)?;
            ops.push(FlatOperator::ModAssign);
        },
        ExpAssign => {
            let [a, b] = extract_two_nodes(node.children)?;
            compile_to_flat_inner(b, ops)?;
            compile_to_flat_inner(a, ops)?;
            ops.push(FlatOperator::ExpAssign);
        },
        AndAssign => {
            let [a, b] = extract_two_nodes(node.children)?;
            compile_to_flat_inner(b, ops)?;
            compile_to_flat_inner(a, ops)?;
            ops.push(FlatOperator::AndAssign);
        },
        OrAssign => {
            let [a, b] = extract_two_nodes(node.children)?;
            compile_to_flat_inner(b, ops)?;
            compile_to_flat_inner(a, ops)?;
            ops.push(FlatOperator::OrAssign);
        },

        // Variable-length operators
        Tuple => {
            let len = into_u32(node.children.len())?;
            for child in node.children {
                compile_to_flat_inner(child, ops)?;
            }
            ops.push(FlatOperator::Tuple { len });
        },
        Chain => {
            let len = into_u32(node.children.len())?;
            for child in node.children {
                compile_to_flat_inner(child, ops)?;
            }
            ops.push(FlatOperator::Chain { len });
        },

        // Leaf nodes
        Const { value } => {
            ops.push(FlatOperator::PushConst { value });
        },
        VariableIdentifierRead { identifier } => {
            ops.push(FlatOperator::ReadVar { identifier });
        },
        VariableIdentifierWrite { identifier } => {
            ops.push(FlatOperator::WriteVar { identifier });
        },
        FunctionIdentifier { identifier } => {
            let len = into_u32(node.children.len())?;
            for child in node.children {
                compile_to_flat_inner(child, ops)?;
            }
            ops.push(FlatOperator::FunctionCall { identifier, len });
        },
    }

    Ok(())
}
/// Convert Node directly to FlatNode (similar to Node -> CompiledNode)
pub fn compile_to_flat<NumericTypes: EvalexprNumericTypes>(
    node: Node<NumericTypes>,
) -> EvalexprResult<FlatNode<NumericTypes>, NumericTypes> {
    let mut ops = Vec::new();
    compile_to_flat_inner(node, &mut ops)?;
    Ok(FlatNode { ops })
}

fn pop_unchecked<T: EvalexprNumericTypes>(stack: &mut Stack<T>) -> Value<T> {
    debug_assert!(!stack.is_empty());
    unsafe { stack.pop().unwrap_unchecked() }
}
impl<NumericTypes: EvalexprNumericTypes> FlatNode<NumericTypes> {
    /// Evaluate the flat node using a stack-based approach
    pub fn eval_priv<C: Context<NumericTypes = NumericTypes>>(
        &self,
        stack: &mut Stack<NumericTypes>,
        context: &C,
        override_vars: &impl Fn(IStr) -> Option<Value<NumericTypes>>,
    ) -> EvalexprResultValue<NumericTypes> {
        use FlatOperator::*;

        for op in &self.ops {
            match op {
                // Binary arithmetic operators
                Add => {
                    let b = pop_unchecked(stack).as_float()?;
                    let a = pop_unchecked(stack).as_float()?;
                    stack.push(Value::Float(a + b));
                },
                Sub => {
                    let b = pop_unchecked(stack).as_float()?;
                    let a = pop_unchecked(stack).as_float()?;
                    stack.push(Value::Float(a - b));
                },
                Mul => {
                    let b = pop_unchecked(stack).as_float()?;
                    let a = pop_unchecked(stack).as_float()?;
                    stack.push(Value::Float(a * b));
                },
                Div => {
                    let b = pop_unchecked(stack).as_float()?;
                    let a = pop_unchecked(stack).as_float()?;
                    stack.push(Value::Float(a / b));
                },
                Mod => {
                    let b = pop_unchecked(stack).as_float()?;
                    let a = pop_unchecked(stack).as_float()?;
                    stack.push(Value::Float(a % b));
                },
                Exp => {
                    let b = pop_unchecked(stack).as_float()?;
                    let a = pop_unchecked(stack).as_float()?;
                    stack.push(Value::Float(a.pow(&b)));
                },

                // Unary operators
                Neg => {
                    let a = pop_unchecked(stack).as_float()?;
                    stack.push(Value::Float(-a));
                },
                Not => {
                    let a = pop_unchecked(stack).as_boolean()?;
                    stack.push(Value::Boolean(!a));
                },

                // Comparison operators
                Eq => {
                    let b = pop_unchecked(stack);
                    let a = pop_unchecked(stack);
                    stack.push(Value::Boolean(a == b));
                },
                Neq => {
                    let b = pop_unchecked(stack);
                    let a = pop_unchecked(stack);
                    stack.push(Value::Boolean(a != b));
                },
                Gt => {
                    let b = pop_unchecked(stack).as_float()?;
                    let a = pop_unchecked(stack).as_float()?;
                    stack.push(Value::Boolean(a > b));
                },
                Lt => {
                    let b = pop_unchecked(stack).as_float()?;
                    let a = pop_unchecked(stack).as_float()?;
                    stack.push(Value::Boolean(a < b));
                },
                Geq => {
                    let b = pop_unchecked(stack).as_float()?;
                    let a = pop_unchecked(stack).as_float()?;
                    stack.push(Value::Boolean(a >= b));
                },
                Leq => {
                    let b = pop_unchecked(stack).as_float()?;
                    let a = pop_unchecked(stack).as_float()?;
                    stack.push(Value::Boolean(a <= b));
                },

                // Logical operators
                And => {
                    let b = pop_unchecked(stack).as_boolean()?;
                    let a = pop_unchecked(stack).as_boolean()?;
                    stack.push(Value::Boolean(a && b));
                },
                Or => {
                    let b = pop_unchecked(stack).as_boolean()?;
                    let a = pop_unchecked(stack).as_boolean()?;
                    stack.push(Value::Boolean(a || b));
                },

                // Assignment operators
                Assign | AddAssign | SubAssign | MulAssign | DivAssign | ModAssign | ExpAssign
                | AndAssign | OrAssign => {
                    return Err(EvalexprError::ContextNotMutable);
                },

                // Variable-length operators
                Tuple { len } => {
                    // Special case: 2-element tuple with floats becomes Float2
                    if *len == 2 {
                        let b = pop_unchecked(stack);
                        let a = pop_unchecked(stack);

                        if a.is_float() && b.is_float() {
                            stack.push(Value::Float2(a.as_float()?, b.as_float()?));
                        } else {
                            stack.push(Value::Tuple(vec![a, b]));
                        }
                    } else {
                        let start_idx = stack.len() - *len as usize;
                        let values = stack.drain(start_idx..).collect();
                        stack.push(Value::Tuple(values));
                    }
                },
                Chain { len } => {
                    debug_assert!(
                        *len > 0,
                        "Chain with 0 length should be caught at compile time"
                    );

                    // Keep only the last value, discard the rest
                    let start_idx = stack.len() - *len as usize;
                    stack.drain(start_idx..stack.len() - 1);
                },
                FunctionCall { identifier, len } => {
                    let start_idx = stack.len() - *len as usize;
                    let arguments: SmallVec<[Value<NumericTypes>; 4]> =
                        stack.drain(start_idx..).collect();

                    let result = match context.call_function(stack, context, *identifier, &arguments)
                    {
                        Err(EvalexprError::FunctionIdentifierNotFound(_))
                            if !context.are_builtin_functions_disabled() =>
                        {
                            if let Some(builtin_function) = builtin_function(identifier) {
                                builtin_function.call(stack, context, &arguments)?
                            } else {
                                return Err(EvalexprError::FunctionIdentifierNotFound(
                                    identifier.to_string()
                                ));
                            }
                        },
                        Ok(val) => val,
                        Err(e) => return Err(e),
                    };

                    stack.push(result);
                },

                // Constants and variables
                PushConst { value } => {
                    stack.push(value.clone());
                },
                ReadVar { identifier } => {
                    let value = if let Some(val) = override_vars(*identifier) {
                        val
                    } else if let Some(val) = context.get_value(*identifier).cloned() {
                        val
                    } else {
                        // Try as zero-argument function
                        match context.call_function(stack, context, *identifier, &[Value::Empty]) {
                            Err(EvalexprError::FunctionIdentifierNotFound(_))
                                if !context.are_builtin_functions_disabled() =>
                            {
                                return Err(EvalexprError::VariableIdentifierNotFound(
                                    identifier.to_string()
                                ));
                            },
                            Ok(val) => val,
                            Err(e) => return Err(e),
                        }
                    };
                    stack.push(value);
                },
                WriteVar { .. } => {
                    // Not implemented in immutable context
                    return Err(EvalexprError::ContextNotMutable);
                },
            }
        }

        // Return the top value or Empty if stack is empty
        Ok(stack.pop().unwrap_or(Value::Empty))
    }
    /// Evaluates the operator with the given arguments and mutable context.
    fn eval_mut_priv<C: ContextWithMutableVariables + Context<NumericTypes = NumericTypes>>(
        &self,
        stack: &mut Stack<NumericTypes>,
        context: &mut C,
    ) -> EvalexprResultValue<C::NumericTypes> {
        todo!()
        // use crate::operator::Operator::*;
        // match self {
        //     Assign => {
        //         expect_operator_argument_amount(arguments.len(), 2)?;
        //         let target = arguments[0].as_str()?;
        //         context.set_value(target, arguments[1].clone())?;

        //         Ok(Value::Empty)
        //     },
        //     AddAssign | SubAssign | MulAssign | DivAssign | ModAssign | ExpAssign | AndAssign
        //     | OrAssign => {
        //         expect_operator_argument_amount(arguments.len(), 2)?;

        //         let target = arguments[0].as_str()?;
        //         let left_value = Operator::VariableIdentifierRead {
        //             identifier: target.to_string(),
        //         }
        //         .eval(&Vec::new(), context)?;
        //         let arguments = vec![left_value, arguments[1].clone()];

        //         let result = match self {
        //             AddAssign => Operator::Add.eval(&arguments, context),
        //             SubAssign => Operator::Sub.eval(&arguments, context),
        //             MulAssign => Operator::Mul.eval(&arguments, context),
        //             DivAssign => Operator::Div.eval(&arguments, context),
        //             ModAssign => Operator::Mod.eval(&arguments, context),
        //             ExpAssign => Operator::Exp.eval(&arguments, context),
        //             AndAssign => Operator::And.eval(&arguments, context),
        //             OrAssign => Operator::Or.eval(&arguments, context),
        //             _ => unreachable!(
        //                 "Forgot to add a match arm for an assign operation: {}",
        //                 self
        //             ),
        //         }?;
        //         context.set_value(target, result)?;

        //         Ok(Value::Empty)
        //     },
        //     _ => self.eval(arguments, context),
        // }
    }
    /// Evaluates the operator tree rooted at this node with empty context.
    pub fn eval(&self) -> EvalexprResultValue<NumericTypes> {
        let context = HashMapContext::<NumericTypes>::new();
        let mut stack = Stack::new();
        self.eval_priv(&mut stack, &context, &|_| None)
    }
    /// Evaluates the operator tree rooted at this node with the given context.
    ///
    /// Fails, if one of the operators in the expression tree fails.
    pub fn eval_with_context<C: Context<NumericTypes = NumericTypes>>(
        &self,
        stack: &mut Stack<NumericTypes>,
        context: &C,
    ) -> EvalexprResultValue<NumericTypes> {
        self.eval_priv(stack, context, &|_| None)
    }
    /// Evaluates the operator tree rooted at this node with the given mutable context.
    ///
    /// Fails, if one of the operators in the expression tree fails.
    pub fn eval_with_context_mut<
        C: ContextWithMutableVariables + Context<NumericTypes = NumericTypes>,
    >(
        &self,
        stack: &mut Stack<NumericTypes>,
        context: &mut C,
    ) -> EvalexprResultValue<NumericTypes> {
        self.eval_mut_priv(stack, context)
    }

    /// Evaluates the operator tree rooted at this node into a float with an the given context.
    ///
    /// Fails, if one of the operators in the expression tree fails.
    pub fn eval_float_with_context<C: Context<NumericTypes = NumericTypes>>(
        &self,
        stack: &mut Stack<NumericTypes>,
        context: &C,
    ) -> EvalexprResult<<NumericTypes as EvalexprNumericTypes>::Float, NumericTypes> {
        match self.eval_with_context(stack, context) {
            Ok(Value::Float(float)) => Ok(float),
            Ok(value) => Err(EvalexprError::expected_float(value)),
            Err(error) => Err(error),
        }
    }
    /// Evaluates the operator tree rooted at this node with the given context and override vars
    ///
    /// Fails, if one of the operators in the expression tree fails.
    pub fn eval_with_context_and_override<C: Context<NumericTypes = NumericTypes>>(
        &self,
        stack: &mut Stack<NumericTypes>,
        context: &C,
        override_vars: impl Fn(IStr) -> Option<Value<NumericTypes>>,
    ) -> EvalexprResultValue<NumericTypes> {
        self.eval_priv(stack, context, &override_vars)
    }

    /// Evaluates the operator tree rooted at this node into a float with an the given context.
    /// If the result of the expression is an integer, it is silently converted into a float.
    ///
    /// Fails, if one of the operators in the expression tree fails.
    pub fn eval_float_with_context_and_override<C: Context<NumericTypes = NumericTypes>>(
        &self,
        stack: &mut Stack<NumericTypes>,
        context: &C,
        override_vars: impl Fn(IStr) -> Option<Value<NumericTypes>>,
    ) -> EvalexprResult<<NumericTypes as EvalexprNumericTypes>::Float, NumericTypes> {
        match self.eval_with_context_and_override(stack, context, override_vars) {
            Ok(Value::Float(float)) => Ok(float),
            Ok(value) => Err(EvalexprError::expected_float(value)),
            Err(error) => Err(error),
        }
    }

    /// Evaluates the operator tree rooted at this node into a boolean with an the given context.
    ///
    /// Fails, if one of the operators in the expression tree fails.
    pub fn eval_boolean_with_context<C: Context<NumericTypes = NumericTypes>>(
        &self,
        stack: &mut Stack<NumericTypes>,
        context: &C,
    ) -> EvalexprResult<bool, NumericTypes> {
        match self.eval_with_context(stack, context) {
            Ok(Value::Boolean(boolean)) => Ok(boolean),
            Ok(value) => Err(EvalexprError::expected_boolean(value)),
            Err(error) => Err(error),
        }
    }

    /// Evaluates the operator tree rooted at this node into a tuple with an the given context.
    ///
    /// Fails, if one of the operators in the expression tree fails.
    pub fn eval_tuple_with_context<C: Context<NumericTypes = NumericTypes>>(
        &self,
        stack: &mut Stack<NumericTypes>,
        context: &C,
    ) -> EvalexprResult<TupleType<NumericTypes>, NumericTypes> {
        match self.eval_with_context(stack, context) {
            Ok(Value::Tuple(tuple)) => Ok(tuple),
            Ok(value) => Err(EvalexprError::expected_tuple(value)),
            Err(error) => Err(error),
        }
    }

    /// Evaluates the operator tree rooted at this node into an empty value with an the given context.
    ///
    /// Fails, if one of the operators in the expression tree fails.
    pub fn eval_empty_with_context<C: Context<NumericTypes = NumericTypes>>(
        &self,
        stack: &mut Stack<NumericTypes>,
        context: &C,
    ) -> EvalexprResult<EmptyType, NumericTypes> {
        match self.eval_with_context(stack, context) {
            Ok(Value::Empty) => Ok(EMPTY_VALUE),
            Ok(value) => Err(EvalexprError::expected_empty(value)),
            Err(error) => Err(error),
        }
    }

    /// Evaluates the operator tree rooted at this node into a float with an the given mutable context.
    ///
    /// Fails, if one of the operators in the expression tree fails.
    pub fn eval_float_with_context_mut<
        C: ContextWithMutableVariables + Context<NumericTypes = NumericTypes>,
    >(
        &self,
        stack: &mut Stack<NumericTypes>,
        context: &mut C,
    ) -> EvalexprResult<<NumericTypes as EvalexprNumericTypes>::Float, NumericTypes> {
        match self.eval_with_context_mut(stack, context) {
            Ok(Value::Float(float)) => Ok(float),
            Ok(value) => Err(EvalexprError::expected_float(value)),
            Err(error) => Err(error),
        }
    }

    /// Evaluates the operator tree rooted at this node into a boolean with an the given mutable context.
    ///
    /// Fails, if one of the operators in the expression tree fails.
    pub fn eval_boolean_with_context_mut<
        C: ContextWithMutableVariables + Context<NumericTypes = NumericTypes>,
    >(
        &self,
        stack: &mut Stack<NumericTypes>,
        context: &mut C,
    ) -> EvalexprResult<bool, NumericTypes> {
        match self.eval_with_context_mut(stack, context) {
            Ok(Value::Boolean(boolean)) => Ok(boolean),
            Ok(value) => Err(EvalexprError::expected_boolean(value)),
            Err(error) => Err(error),
        }
    }

    /// Evaluates the operator tree rooted at this node into a tuple with an the given mutable context.
    ///
    /// Fails, if one of the operators in the expression tree fails.
    pub fn eval_tuple_with_context_mut<
        C: ContextWithMutableVariables + Context<NumericTypes = NumericTypes>,
    >(
        &self,
        stack: &mut Stack<NumericTypes>,
        context: &mut C,
    ) -> EvalexprResult<TupleType<NumericTypes>, NumericTypes> {
        match self.eval_with_context_mut(stack, context) {
            Ok(Value::Tuple(tuple)) => Ok(tuple),
            Ok(value) => Err(EvalexprError::expected_tuple(value)),
            Err(error) => Err(error),
        }
    }

    /// Evaluates the operator tree rooted at this node into an empty value with an the given mutable context.
    ///
    /// Fails, if one of the operators in the expression tree fails.
    pub fn eval_empty_with_context_mut<
        C: ContextWithMutableVariables + Context<NumericTypes = NumericTypes>,
    >(
        &self,
        stack: &mut Stack<NumericTypes>,
        context: &mut C,
    ) -> EvalexprResult<EmptyType, NumericTypes> {
        match self.eval_with_context_mut(stack, context) {
            Ok(Value::Empty) => Ok(EMPTY_VALUE),
            Ok(value) => Err(EvalexprError::expected_empty(value)),
            Err(error) => Err(error),
        }
    }
    /// Evaluates the operator tree rooted at this node into a float.
    ///
    /// Fails, if one of the operators in the expression tree fails.
    pub fn eval_float(
        &self,
    ) -> EvalexprResult<<NumericTypes as EvalexprNumericTypes>::Float, NumericTypes> {
        let mut stack = Stack::new();
        self.eval_float_with_context_mut(&mut stack, &mut HashMapContext::new())
    }
    /// Evaluates the operator tree rooted at this node into a boolean.
    ///
    /// Fails, if one of the operators in the expression tree fails.
    pub fn eval_boolean(&self) -> EvalexprResult<bool, NumericTypes> {
        let mut stack = Stack::new();
        self.eval_boolean_with_context_mut(&mut stack, &mut HashMapContext::new())
    }

    /// Evaluates the operator tree rooted at this node into a tuple.
    ///
    /// Fails, if one of the operators in the expression tree fails.
    pub fn eval_tuple(&self) -> EvalexprResult<TupleType<NumericTypes>, NumericTypes> {
        let mut stack = Stack::new();
        self.eval_tuple_with_context_mut(&mut stack, &mut HashMapContext::new())
    }

    /// Evaluates the operator tree rooted at this node into an empty value.
    ///
    /// Fails, if one of the operators in the expression tree fails.
    pub fn eval_empty(&self) -> EvalexprResult<EmptyType, NumericTypes> {
        let mut stack = Stack::new();
        self.eval_empty_with_context_mut(&mut stack, &mut HashMapContext::new())
    }

    /// Returns an iterator over all nodes in this tree.
    pub fn iter(&self) -> impl Iterator<Item = &FlatOperator<NumericTypes>> {
        self.ops.iter()
    }
    /// Returns an iterator over all identifiers in this expression.
    /// Each occurrence of an identifier is returned separately.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use evalexpr::*;
    ///
    /// let tree = build_operator_tree::<DefaultNumericTypes>("a + b + c * f()").unwrap(); // Do proper error handling here
    /// let mut iter = tree.iter_identifiers();
    /// assert_eq!(iter.next(), Some("a"));
    /// assert_eq!(iter.next(), Some("b"));
    /// assert_eq!(iter.next(), Some("c"));
    /// assert_eq!(iter.next(), Some("f"));
    /// assert_eq!(iter.next(), None);
    /// ```
    pub fn iter_identifiers(&self) -> impl Iterator<Item = &str> {
        self.iter().filter_map(|node| match node {
            FlatOperator::ReadVar { identifier }
            | FlatOperator::WriteVar { identifier }
            | FlatOperator::FunctionCall { identifier, .. } => Some(identifier.to_str()),
            _ => None,
        })
    }
    /// Returns an iterator over all variable identifiers in this expression.
    /// Each occurrence of a variable identifier is returned separately.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use evalexpr::*;
    ///
    /// let tree = build_operator_tree::<DefaultNumericTypes>("a + f(b + c)").unwrap(); // Do proper error handling here
    /// let mut iter = tree.iter_variable_identifiers();
    /// assert_eq!(iter.next(), Some("a"));
    /// assert_eq!(iter.next(), Some("b"));
    /// assert_eq!(iter.next(), Some("c"));
    /// assert_eq!(iter.next(), None);
    /// ```
    pub fn iter_variable_identifiers(&self) -> impl Iterator<Item = &str> {
        self.iter().filter_map(|node| match node {
            FlatOperator::ReadVar { identifier } | FlatOperator::WriteVar { identifier } => {
                Some(identifier.to_str())
            },
            _ => None,
        })
    }
    /// Returns an iterator over all read variable identifiers in this expression.
    /// Each occurrence of a variable identifier is returned separately.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use evalexpr::*;
    ///
    /// let tree = build_operator_tree::<DefaultNumericTypes>("d = a + f(b + c)").unwrap(); // Do proper error handling here
    /// let mut iter = tree.iter_read_variable_identifiers();
    /// assert_eq!(iter.next(), Some("a"));
    /// assert_eq!(iter.next(), Some("b"));
    /// assert_eq!(iter.next(), Some("c"));
    /// assert_eq!(iter.next(), None);
    /// ```
    pub fn iter_read_variable_identifiers(&self) -> impl Iterator<Item = &str> {
        self.iter().filter_map(|node| match node {
            FlatOperator::ReadVar { identifier } => Some(identifier.to_str()),
            _ => None,
        })
    }
    /// Returns an iterator over all write variable identifiers in this expression.
    /// Each occurrence of a variable identifier is returned separately.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use evalexpr::*;
    ///
    /// let tree = build_operator_tree::<DefaultNumericTypes>("d = a + f(b + c)").unwrap(); // Do proper error handling here
    /// let mut iter = tree.iter_write_variable_identifiers();
    /// assert_eq!(iter.next(), Some("d"));
    /// assert_eq!(iter.next(), None);
    /// ```
    pub fn iter_write_variable_identifiers(&self) -> impl Iterator<Item = &str> {
        self.iter().filter_map(|node| match node {
            FlatOperator::WriteVar { identifier } => Some(identifier.to_str()),
            _ => None,
        })
    }
    /// Returns an iterator over all function identifiers in this expression.
    /// Each occurrence of a function identifier is returned separately.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use evalexpr::*;
    ///
    /// let tree = build_operator_tree::<DefaultNumericTypes>("a + f(b + c)").unwrap(); // Do proper error handling here
    /// let mut iter = tree.iter_function_identifiers();
    /// assert_eq!(iter.next(), Some("f"));
    /// assert_eq!(iter.next(), None);
    /// ```
    pub fn iter_function_identifiers(&self) -> impl Iterator<Item = &str> {
        self.iter().filter_map(|node| match node {
            FlatOperator::FunctionCall { identifier, .. } => Some(identifier.to_str()),
            _ => None,
        })
    }
}
