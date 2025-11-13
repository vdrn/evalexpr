use core::slice;

use smallvec::SmallVec;

use crate::{
    error::{expect_operator_argument_amount, EvalexprResultValue},
    function::builtin::builtin_function,
    Context, ContextWithMutableVariables, DefaultNumericTypes, EmptyType, EvalexprError,
    EvalexprFloat, EvalexprNumericTypes, EvalexprResult, HashMapContext, Node, Operator, TupleType,
    Value, EMPTY_VALUE,
};

mod iter;
/// An enum that represents the Node
#[derive(Debug, PartialEq, Clone)]
pub enum CompiledNode<NumericTypes: EvalexprNumericTypes = DefaultNumericTypes> {
    /// A root node in the operator tree.
    /// Do we need this? only used for empty expressions?
    RootNode,

    /// A binary addition operator.
    Add(Box<[CompiledNode<NumericTypes>; 2]>),
    /// A binary subtraction operator.
    Sub(Box<[CompiledNode<NumericTypes>; 2]>),
    /// A unary negation operator.
    Neg(Box<CompiledNode<NumericTypes>>),
    /// A binary multiplication operator.
    Mul(Box<[CompiledNode<NumericTypes>; 2]>),
    /// A binary division operator.
    Div(Box<[CompiledNode<NumericTypes>; 2]>),
    /// A binary modulo operator.
    Mod(Box<[CompiledNode<NumericTypes>; 2]>),
    /// A binary exponentiation operator.
    Exp(Box<[CompiledNode<NumericTypes>; 2]>),

    /// A binary equality comparator.
    Eq(Box<[CompiledNode<NumericTypes>; 2]>),
    /// A binary inequality comparator.
    Neq(Box<[CompiledNode<NumericTypes>; 2]>),
    /// A binary greater-than comparator.
    Gt(Box<[CompiledNode<NumericTypes>; 2]>),
    /// A binary lower-than comparator.
    Lt(Box<[CompiledNode<NumericTypes>; 2]>),
    /// A binary greater-than-or-equal comparator.
    Geq(Box<[CompiledNode<NumericTypes>; 2]>),
    /// A binary lower-than-or-equal comparator.
    Leq(Box<[CompiledNode<NumericTypes>; 2]>),
    /// A binary logical and operator.
    And(Box<[CompiledNode<NumericTypes>; 2]>),
    /// A binary logical or operator.
    Or(Box<[CompiledNode<NumericTypes>; 2]>),
    /// A unary logical not operator.
    Not(Box<CompiledNode<NumericTypes>>),

    /// A binary assignment operator.
    Assign(Box<[CompiledNode<NumericTypes>; 2]>),
    /// A binary add-assign operator.
    AddAssign(Box<[CompiledNode<NumericTypes>; 2]>),
    /// A binary subtract-assign operator.
    SubAssign(Box<[CompiledNode<NumericTypes>; 2]>),
    /// A binary multiply-assign operator.
    MulAssign(Box<[CompiledNode<NumericTypes>; 2]>),
    /// A binary divide-assign operator.
    DivAssign(Box<[CompiledNode<NumericTypes>; 2]>),
    /// A binary modulo-assign operator.
    ModAssign(Box<[CompiledNode<NumericTypes>; 2]>),
    /// A binary exponentiate-assign operator.
    ExpAssign(Box<[CompiledNode<NumericTypes>; 2]>),
    /// A binary and-assign operator.
    AndAssign(Box<[CompiledNode<NumericTypes>; 2]>),
    /// A binary or-assign operator.
    OrAssign(Box<[CompiledNode<NumericTypes>; 2]>),

    /// An n-ary tuple constructor.
    Tuple(Box<[CompiledNode<NumericTypes>]>),
    /// An n-ary subexpression chain.
    Chain(Box<[CompiledNode<NumericTypes>]>),

    /// A constant value.
    Const {
        /** The value of the constant. */
        value: Value<NumericTypes>,
    },
    /// A write to a variable identifier.
    VariableIdentifierWrite {
        /// The identifier of the variable.
        identifier: String,
    },
    /// A read from a variable identifier.
    VariableIdentifierRead {
        /// The identifier of the variable.
        identifier: String,
    },
    /// A function identifier.
    FunctionIdentifier {
        /// The identifier of the function.
        identifier: String,
        /// Arguments of the function.
        args: Box<[CompiledNode<NumericTypes>]>,
    },
}
fn extract_n<NumericTypes: EvalexprNumericTypes>(
    node: Vec<Node<NumericTypes>>,
) -> Result<Box<[CompiledNode<NumericTypes>]>, EvalexprError<NumericTypes>> {
    let mut children: Vec<CompiledNode<NumericTypes>> = Vec::with_capacity(node.len());
    for child in node {
        children.push(child.try_into()?);
    }
    Ok(children.into_boxed_slice())
}

fn extract_one<NumericTypes: EvalexprNumericTypes>(
    mut node: Node<NumericTypes>,
) -> Result<Box<CompiledNode<NumericTypes>>, EvalexprError<NumericTypes>> {
    expect_operator_argument_amount(node.children.len(), 1)?;
    Ok(Box::new(node.children.pop().unwrap().try_into()?))
}
fn extract_a_anb_b<NumericTypes: EvalexprNumericTypes>(
    mut node: Node<NumericTypes>,
) -> Result<Box<[CompiledNode<NumericTypes>; 2]>, EvalexprError<NumericTypes>> {
    expect_operator_argument_amount(node.children.len(), 2)?;
    let b = node.children.pop().unwrap();
    let a = node.children.pop().unwrap();
    Ok(Box::new([a.try_into()?, b.try_into()?]))
}
impl<NumericTypes: EvalexprNumericTypes> TryFrom<Node<NumericTypes>>
    for CompiledNode<NumericTypes>
{
    type Error = EvalexprError<NumericTypes>;

    fn try_from(mut node: Node<NumericTypes>) -> Result<Self, Self::Error> {
        Ok(match node.operator {
            Operator::RootNode => {
                if node.children.len() > 1 {
                    return Err(EvalexprError::wrong_operator_argument_amount(
                        node.children.len(),
                        1,
                    ));
                }
                if let Some(child) = node.children.pop() {
                    child.try_into()?
                } else {
                    Self::RootNode
                }
            },
            Operator::Add => Self::Add(extract_a_anb_b(node)?),
            Operator::Sub => Self::Sub(extract_a_anb_b(node)?),
            Operator::Neg => Self::Neg(extract_one(node)?),
            Operator::Mul => Self::Mul(extract_a_anb_b(node)?),
            Operator::Div => Self::Div(extract_a_anb_b(node)?),
            Operator::Mod => Self::Mod(extract_a_anb_b(node)?),
            Operator::Exp => Self::Exp(extract_a_anb_b(node)?),
            Operator::Eq => Self::Eq(extract_a_anb_b(node)?),
            Operator::Neq => Self::Neq(extract_a_anb_b(node)?),
            Operator::Gt => Self::Gt(extract_a_anb_b(node)?),
            Operator::Lt => Self::Lt(extract_a_anb_b(node)?),
            Operator::Geq => Self::Geq(extract_a_anb_b(node)?),
            Operator::Leq => Self::Leq(extract_a_anb_b(node)?),
            Operator::And => Self::And(extract_a_anb_b(node)?),
            Operator::Or => Self::Or(extract_a_anb_b(node)?),
            Operator::Not => Self::Not(extract_one(node)?),
            Operator::Assign => Self::Assign(extract_a_anb_b(node)?),
            Operator::AddAssign => Self::AddAssign(extract_a_anb_b(node)?),
            Operator::SubAssign => Self::SubAssign(extract_a_anb_b(node)?),
            Operator::MulAssign => Self::MulAssign(extract_a_anb_b(node)?),
            Operator::DivAssign => Self::DivAssign(extract_a_anb_b(node)?),
            Operator::ModAssign => Self::ModAssign(extract_a_anb_b(node)?),
            Operator::ExpAssign => Self::ExpAssign(extract_a_anb_b(node)?),
            Operator::AndAssign => Self::AndAssign(extract_a_anb_b(node)?),
            Operator::OrAssign => Self::OrAssign(extract_a_anb_b(node)?),
            Operator::Tuple => Self::Tuple(extract_n(node.children)?),
            Operator::Chain => Self::Chain(extract_n(node.children)?),
            Operator::Const { value } => Self::Const { value },
            Operator::VariableIdentifierWrite { identifier } => {
                Self::VariableIdentifierWrite { identifier }
            },
            Operator::VariableIdentifierRead { identifier } => {
                Self::VariableIdentifierRead { identifier }
            },
            Operator::FunctionIdentifier { identifier } => {
                let args = extract_n(node.children)?;

                Self::FunctionIdentifier { identifier, args }
            },
        })
    }
}
impl<NumericTypes: EvalexprNumericTypes> CompiledNode<NumericTypes> {
    pub(crate) fn children(&self) -> &[CompiledNode<NumericTypes>] {
        match self {
            CompiledNode::RootNode => &[],
            CompiledNode::Neg(v) | CompiledNode::Not(v) => slice::from_ref(v),
            CompiledNode::Add(v)
            | CompiledNode::Sub(v)
            | CompiledNode::Mul(v)
            | CompiledNode::Div(v)
            | CompiledNode::Mod(v)
            | CompiledNode::Exp(v)
            | CompiledNode::Eq(v)
            | CompiledNode::Neq(v)
            | CompiledNode::Gt(v)
            | CompiledNode::Lt(v)
            | CompiledNode::Geq(v)
            | CompiledNode::Leq(v)
            | CompiledNode::And(v)
            | CompiledNode::Or(v)
            | CompiledNode::Assign(v)
            | CompiledNode::AddAssign(v)
            | CompiledNode::SubAssign(v)
            | CompiledNode::MulAssign(v)
            | CompiledNode::DivAssign(v)
            | CompiledNode::ModAssign(v)
            | CompiledNode::ExpAssign(v)
            | CompiledNode::AndAssign(v)
            | CompiledNode::OrAssign(v) => v.as_slice(),
            CompiledNode::Tuple(compiled_nodes) | CompiledNode::Chain(compiled_nodes) => {
                compiled_nodes
            },
            CompiledNode::Const { .. }
            | CompiledNode::VariableIdentifierWrite { .. }
            | CompiledNode::VariableIdentifierRead { .. } => &[],
            CompiledNode::FunctionIdentifier {  args,.. } => args,
        }
    }
    pub(crate) fn children_mut(&mut self) -> &mut [CompiledNode<NumericTypes>] {
        match self {
            CompiledNode::RootNode => &mut [],

            CompiledNode::Neg(v) | CompiledNode::Not(v) => slice::from_mut(v),
            CompiledNode::Add(v)
            | CompiledNode::Sub(v)
            | CompiledNode::Mul(v)
            | CompiledNode::Div(v)
            | CompiledNode::Mod(v)
            | CompiledNode::Exp(v)
            | CompiledNode::Eq(v)
            | CompiledNode::Neq(v)
            | CompiledNode::Gt(v)
            | CompiledNode::Lt(v)
            | CompiledNode::Geq(v)
            | CompiledNode::Leq(v)
            | CompiledNode::And(v)
            | CompiledNode::Or(v)
            | CompiledNode::Assign(v)
            | CompiledNode::AddAssign(v)
            | CompiledNode::SubAssign(v)
            | CompiledNode::MulAssign(v)
            | CompiledNode::DivAssign(v)
            | CompiledNode::ModAssign(v)
            | CompiledNode::ExpAssign(v)
            | CompiledNode::AndAssign(v)
            | CompiledNode::OrAssign(v) => v.as_mut_slice(),
            CompiledNode::Tuple(compiled_nodes) | CompiledNode::Chain(compiled_nodes) => {
                compiled_nodes
            },
            CompiledNode::Const { .. }
            | CompiledNode::VariableIdentifierWrite { .. }
            | CompiledNode::VariableIdentifierRead { .. } => &mut [],
            CompiledNode::FunctionIdentifier {  args,.. } => args,
        }
    }
    /// Evaluates the node with the given context.
    fn eval_priv<C: Context<NumericTypes = NumericTypes>>(
        &self,
        context: &C,
        override_vars: &impl Fn(&str) -> Option<Value<NumericTypes>>,
        override_fns: &impl Fn(
            &str,
            &[CompiledNode<NumericTypes>],
        ) -> Option<EvalexprResultValue<NumericTypes>>,
    ) -> EvalexprResultValue<NumericTypes> {
        use CompiledNode::*;
        match self {
            RootNode => Ok(Value::Empty),
            Add(args) => {
                // expect_float_or_string(&arguments[0])?;
                // expect_float_or_string(&arguments[1])?;
                let a = args[0]
                    .eval_priv(context, override_vars, override_fns)?
                    .as_float()?;
                let b = args[1]
                    .eval_priv(context, override_vars, override_fns)?
                    .as_float()?;

                Ok(Value::Float(a + b))
            },
            Sub(args) => {
                let a = args[0]
                    .eval_priv(context, override_vars, override_fns)?
                    .as_float()?;
                let b = args[1]
                    .eval_priv(context, override_vars, override_fns)?
                    .as_float()?;

                Ok(Value::Float(a - b))
            },
            Neg(arg) => {
                let a = arg
                    .eval_priv(context, override_vars, override_fns)?
                    .as_float()?;

                Ok(Value::Float(-a))
            },
            Mul(args) => {
                let a = args[0]
                    .eval_priv(context, override_vars, override_fns)?
                    .as_float()?;
                let b = args[1]
                    .eval_priv(context, override_vars, override_fns)?
                    .as_float()?;

                Ok(Value::Float(a * b))
            },
            Div(args) => {
                let a = args[0]
                    .eval_priv(context, override_vars, override_fns)?
                    .as_float()?;
                let b = args[1]
                    .eval_priv(context, override_vars, override_fns)?
                    .as_float()?;

                Ok(Value::Float(a / b))
            },
            Mod(args) => {
                let a = args[0]
                    .eval_priv(context, override_vars, override_fns)?
                    .as_float()?;
                let b = args[1]
                    .eval_priv(context, override_vars, override_fns)?
                    .as_float()?;

                Ok(Value::Float(a % b))
            },
            Exp(args) => {
                let a = args[0]
                    .eval_priv(context, override_vars, override_fns)?
                    .as_float()?;
                let b = args[1]
                    .eval_priv(context, override_vars, override_fns)?
                    .as_float()?;

                Ok(Value::Float(a.pow(&b)))
            },
            Eq(args) => {
                let a = args[0].eval_priv(context, override_vars, override_fns)?;
                let b = args[1].eval_priv(context, override_vars, override_fns)?;

                Ok(Value::Boolean(a == b))
            },
            Neq(args) => {
                let a = args[0].eval_priv(context, override_vars, override_fns)?;
                let b = args[1].eval_priv(context, override_vars, override_fns)?;

                Ok(Value::Boolean(a != b))
            },
            Gt(args) => {
                let a = args[0]
                    .eval_priv(context, override_vars, override_fns)?
                    .as_float()?;
                let b = args[1]
                    .eval_priv(context, override_vars, override_fns)?
                    .as_float()?;
                Ok(Value::Boolean(a > b))
            },
            Lt(args) => {
                let a = args[0]
                    .eval_priv(context, override_vars, override_fns)?
                    .as_float()?;
                let b = args[1]
                    .eval_priv(context, override_vars, override_fns)?
                    .as_float()?;
                Ok(Value::Boolean(a < b))
            },
            Geq(args) => {
                let a = args[0]
                    .eval_priv(context, override_vars, override_fns)?
                    .as_float()?;
                let b = args[1]
                    .eval_priv(context, override_vars, override_fns)?
                    .as_float()?;
                Ok(Value::Boolean(a >= b))
            },
            Leq(args) => {
                let a = args[0]
                    .eval_priv(context, override_vars, override_fns)?
                    .as_float()?;
                let b = args[1]
                    .eval_priv(context, override_vars, override_fns)?
                    .as_float()?;
                Ok(Value::Boolean(a <= b))
            },
            And(args) => {
                let a = args[0]
                    .eval_priv(context, override_vars, override_fns)?
                    .as_boolean()?;
                let b = args[1]
                    .eval_priv(context, override_vars, override_fns)?
                    .as_boolean()?;

                Ok(Value::Boolean(a && b))
            },
            Or(args) => {
                let a = args[0]
                    .eval_priv(context, override_vars, override_fns)?
                    .as_boolean()?;
                let b = args[1]
                    .eval_priv(context, override_vars, override_fns)?
                    .as_boolean()?;

                Ok(Value::Boolean(a || b))
            },
            Not(arg) => {
                let a = arg
                    .eval_priv(context, override_vars, override_fns)?
                    .as_boolean()?;

                Ok(Value::Boolean(!a))
            },
            Assign(_) | AddAssign(_) | SubAssign(_) | MulAssign(_) | DivAssign(_)
            | ModAssign(_) | ExpAssign(_) | AndAssign(_) | OrAssign(_) => {
                Err(EvalexprError::ContextNotMutable)
            },

            Tuple(args) => {
                if args.len() == 2 {
                    let a = args[0].eval_priv(context, override_vars, override_fns)?;
                    let b = args[1].eval_priv(context, override_vars, override_fns)?;
                    if a.is_float() && b.is_float() {
                        Ok(Value::Float2(a.as_float()?, b.as_float()?))
                    } else {
                        Ok(Value::Tuple(vec![a, b]))
                    }
                } else {
                    let mut values = Vec::with_capacity(args.len());
                    for arg in args {
                        values.push(arg.eval_priv(context, override_vars, override_fns)?);
                    }
                    Ok(Value::Tuple(values))
                }
            },
            Chain(args) => {
                if args.is_empty() {
                    return Err(EvalexprError::wrong_operator_argument_amount(0, 1));
                }
                let last = args.last().unwrap();
                let value = last.eval_priv(context, override_vars, override_fns)?;
                Ok(value)
            },
            Const { value } => Ok(value.clone()),
            VariableIdentifierWrite { identifier } => {
                todo!()
            },
            VariableIdentifierRead { identifier } => {
                if let Some(value) = override_vars(identifier) {
                    Ok(value)
                } else if let Some(value) = context.get_value(identifier).cloned() {
                    Ok(value)
                } else {
                    match context.call_function(context, identifier, &[Value::Empty]) {
                        Err(EvalexprError::FunctionIdentifierNotFound(_))
                            if !context.are_builtin_functions_disabled() =>
                        {
                            Err(EvalexprError::VariableIdentifierNotFound(
                                identifier.clone(),
                            ))
                        },
                        result => result,
                    }
                }
            },
            FunctionIdentifier { identifier, args } => {
                // expect_operator_argument_amount(arguments.len(), 1)?;
                // let arguments = &arguments[0];
                if let Some(value) = override_fns(identifier, args) {
                    return value;
                }
                let mut arguments: SmallVec<[Value<NumericTypes>; 4]> =
                    SmallVec::with_capacity(args.len());
                for arg in args {
                    arguments.push(arg.eval_priv(context, override_vars, override_fns)?);
                }

                match context.call_function(context, identifier, &arguments) {
                    Err(EvalexprError::FunctionIdentifierNotFound(_))
                        if !context.are_builtin_functions_disabled() =>
                    {
                        if let Some(builtin_function) = builtin_function(identifier) {
                            builtin_function.call(context, &arguments)
                        } else {
                            Err(EvalexprError::FunctionIdentifierNotFound(
                                identifier.clone(),
                            ))
                        }
                    },
                    result => result,
                }
            },
        }
    }

    /// Evaluates the operator with the given arguments and mutable context.
    fn eval_mut_priv<C: ContextWithMutableVariables + Context<NumericTypes = NumericTypes>>(
        &self,
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
        self.eval_priv(&context, &|_| None, &|_, _| None)
    }
    /// Evaluates the operator tree rooted at this node with the given context.
    ///
    /// Fails, if one of the operators in the expression tree fails.
    pub fn eval_with_context<C: Context<NumericTypes = NumericTypes>>(
        &self,
        context: &C,
    ) -> EvalexprResultValue<NumericTypes> {
        self.eval_priv(context, &|_| None, &|_, _| None)
    }
    /// Evaluates the operator tree rooted at this node with the given mutable context.
    ///
    /// Fails, if one of the operators in the expression tree fails.
    pub fn eval_with_context_mut<
        C: ContextWithMutableVariables + Context<NumericTypes = NumericTypes>,
    >(
        &self,
        context: &mut C,
    ) -> EvalexprResultValue<NumericTypes> {
        self.eval_mut_priv(context)
    }

    /// Evaluates the operator tree rooted at this node into a float with an the given context.
    ///
    /// Fails, if one of the operators in the expression tree fails.
    pub fn eval_float_with_context<C: Context<NumericTypes = NumericTypes>>(
        &self,
        context: &C,
    ) -> EvalexprResult<<NumericTypes as EvalexprNumericTypes>::Float, NumericTypes> {
        match self.eval_with_context(context) {
            Ok(Value::Float(float)) => Ok(float),
            Ok(value) => Err(EvalexprError::expected_float(value)),
            Err(error) => Err(error),
        }
    }
    /// Evaluates the operator tree rooted at this node with the given context.
    ///
    /// Fails, if one of the operators in the expression tree fails.
    pub fn eval_with_context_and_x<C: Context<NumericTypes = NumericTypes>>(
        &self,
        context: &C,
        x: &Value<NumericTypes>,
        step: NumericTypes::Float,
    ) -> EvalexprResultValue<NumericTypes> {
        self.eval_priv(
            context,
            &|identifier| {
                if identifier == "x" {
                    Some(x.clone())
                } else {
                    None
                }
            },
            &|identifier, args| {
                if identifier == "d" {
                    #[inline(always)]
                    fn inner<T: EvalexprNumericTypes, C: Context<NumericTypes = T>>(
                        context: &C,
                        args: &[CompiledNode<T>],
                        x: &Value<T>,
                        step: T::Float,
                    ) -> EvalexprResultValue<T> {
                        let x2 = Value::Float(x.as_float()? + step);

                        let expr = args.first().ok_or_else(|| {
                            EvalexprError::CustomMessage(
                                "Derivative needs 1 argument: the expression".to_string(),
                            )
                        })?;
                        let y2 = expr
                            .eval_with_context_and_x(context, &x2, step)?
                            .as_float()?;
                        let y1 = expr.eval_with_context_and_x(context, x, step)?.as_float()?;

                        Ok(Value::Float((y2 - y1) / step))
                    }
                    Some(inner(context, args, x, step))
                } else {
                    None
                }
            },
        )
    }
    /// Evaluates the operator tree rooted at this node with the given context.
    ///
    /// Fails, if one of the operators in the expression tree fails.
    pub fn eval_with_context_and_y<C: Context<NumericTypes = NumericTypes>>(
        &self,
        context: &C,
        y: &Value<NumericTypes>,
        step: NumericTypes::Float,
    ) -> EvalexprResultValue<NumericTypes> {
        self.eval_priv(
            context,
            &|identifier| {
                if identifier == "y" {
                    Some(y.clone())
                } else {
                    None
                }
            },
            &|identifier, args| {
                if identifier == "d" {
                    #[inline(always)]
                    fn inner<T: EvalexprNumericTypes, C: Context<NumericTypes = T>>(
                        context: &C,
                        args: &[CompiledNode<T>],
                        y: &Value<T>,
                        step: T::Float,
                    ) -> EvalexprResultValue<T> {
                        let y2 = Value::Float(y.as_float()? + step);

                        let expr = args.first().ok_or_else(|| {
                            EvalexprError::CustomMessage(
                                "Derivative needs 1 argument: the expression".to_string(),
                            )
                        })?;
                        let x2 = expr
                            .eval_with_context_and_y(context, &y2, step)?
                            .as_float()?;
                        let x1 = expr.eval_with_context_and_y(context, y, step)?.as_float()?;

                        Ok(Value::Float((x2 - x1) / step))
                    }
                    Some(inner(context, args, y, step))
                } else {
                    None
                }
            },
        )
    }

    /// Evaluates the operator tree rooted at this node with the given context.
    ///
    /// Fails, if one of the operators in the expression tree fails.
    pub fn eval_with_context_and_xy_and_z<C: Context<NumericTypes = NumericTypes>>(
        &self,
        context: &C,
        x: &Value<NumericTypes>,
        y: &Value<NumericTypes>,
        zx: &Value<NumericTypes>,
        zy: &Value<NumericTypes>,
    ) -> EvalexprResultValue<NumericTypes> {
        self.eval_priv(
            context,
            &|identifier| {
                if identifier == "x" {
                    Some(x.clone())
                } else if identifier == "y" {
                    Some(y.clone())
                } else if identifier == "zx" {
                    Some(zx.clone())
                } else if identifier == "zy" {
                    Some(zy.clone())
                } else {
                    None
                }
            },
            &|_, _| None,
        )
    }

    /// Evaluates the operator tree rooted at this node into a float with an the given context.
    /// If the result of the expression is an integer, it is silently converted into a float.
    ///
    /// Fails, if one of the operators in the expression tree fails.
    pub fn eval_float_with_context_and_x<C: Context<NumericTypes = NumericTypes>>(
        &self,
        context: &C,
        x: &Value<NumericTypes>,
        step: NumericTypes::Float,
    ) -> EvalexprResult<<NumericTypes as EvalexprNumericTypes>::Float, NumericTypes> {
        match self.eval_with_context_and_x(context, x, step) {
            Ok(Value::Float(float)) => Ok(float),
            Ok(value) => Err(EvalexprError::expected_float(value)),
            Err(error) => Err(error),
        }
    }
    /// Evaluates the operator tree rooted at this node into a float with an the given context.
    /// If the result of the expression is an integer, it is silently converted into a float.
    ///
    /// Fails, if one of the operators in the expression tree fails.
    pub fn eval_float_with_context_and_y<C: Context<NumericTypes = NumericTypes>>(
        &self,
        context: &C,
        y: &Value<NumericTypes>,
        step: NumericTypes::Float,
    ) -> EvalexprResult<<NumericTypes as EvalexprNumericTypes>::Float, NumericTypes> {
        match self.eval_with_context_and_y(context, y, step) {
            Ok(Value::Float(float)) => Ok(float),
            Ok(value) => Err(EvalexprError::expected_float(value)),
            Err(error) => Err(error),
        }
    }

    /// Evaluates the operator tree rooted at this node into a float with an the given context.
    /// If the result of the expression is an integer, it is silently converted into a float.
    ///
    /// Fails, if one of the operators in the expression tree fails.
    pub fn eval_float_with_context_and_xy_and_z<C: Context<NumericTypes = NumericTypes>>(
        &self,
        context: &C,
        x: &Value<NumericTypes>,
        y: &Value<NumericTypes>,
        zx: &Value<NumericTypes>,
        zy: &Value<NumericTypes>,
    ) -> EvalexprResult<<NumericTypes as EvalexprNumericTypes>::Float, NumericTypes> {
        match self.eval_with_context_and_xy_and_z(context, x, y, zx, zy) {
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
        context: &C,
    ) -> EvalexprResult<bool, NumericTypes> {
        match self.eval_with_context(context) {
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
        context: &C,
    ) -> EvalexprResult<TupleType<NumericTypes>, NumericTypes> {
        match self.eval_with_context(context) {
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
        context: &C,
    ) -> EvalexprResult<EmptyType, NumericTypes> {
        match self.eval_with_context(context) {
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
        context: &mut C,
    ) -> EvalexprResult<<NumericTypes as EvalexprNumericTypes>::Float, NumericTypes> {
        match self.eval_with_context_mut(context) {
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
        context: &mut C,
    ) -> EvalexprResult<bool, NumericTypes> {
        match self.eval_with_context_mut(context) {
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
        context: &mut C,
    ) -> EvalexprResult<TupleType<NumericTypes>, NumericTypes> {
        match self.eval_with_context_mut(context) {
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
        context: &mut C,
    ) -> EvalexprResult<EmptyType, NumericTypes> {
        match self.eval_with_context_mut(context) {
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
        self.eval_float_with_context_mut(&mut HashMapContext::new())
    }
    /// Evaluates the operator tree rooted at this node into a boolean.
    ///
    /// Fails, if one of the operators in the expression tree fails.
    pub fn eval_boolean(&self) -> EvalexprResult<bool, NumericTypes> {
        self.eval_boolean_with_context_mut(&mut HashMapContext::new())
    }

    /// Evaluates the operator tree rooted at this node into a tuple.
    ///
    /// Fails, if one of the operators in the expression tree fails.
    pub fn eval_tuple(&self) -> EvalexprResult<TupleType<NumericTypes>, NumericTypes> {
        self.eval_tuple_with_context_mut(&mut HashMapContext::new())
    }

    /// Evaluates the operator tree rooted at this node into an empty value.
    ///
    /// Fails, if one of the operators in the expression tree fails.
    pub fn eval_empty(&self) -> EvalexprResult<EmptyType, NumericTypes> {
        self.eval_empty_with_context_mut(&mut HashMapContext::new())
    }
}

impl<NumericTypes: EvalexprNumericTypes> CompiledNode<NumericTypes> {
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
            CompiledNode::VariableIdentifierWrite { identifier }
            | CompiledNode::VariableIdentifierRead { identifier }
            | CompiledNode::FunctionIdentifier { identifier, .. } => Some(identifier.as_str()),
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
            CompiledNode::VariableIdentifierWrite { identifier }
            | CompiledNode::VariableIdentifierRead { identifier } => Some(identifier.as_str()),
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
            CompiledNode::VariableIdentifierRead { identifier } => Some(identifier.as_str()),
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
            CompiledNode::VariableIdentifierWrite { identifier } => Some(identifier.as_str()),
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
            CompiledNode::FunctionIdentifier { identifier, .. } => Some(identifier.as_str()),
            _ => None,
        })
    }
}
