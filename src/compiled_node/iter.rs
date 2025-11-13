use crate::{compiled_node::CompiledNode, EvalexprNumericTypes};
use std::slice::{Iter, IterMut};

/// An iterator that traverses an operator tree in pre-order.
pub struct NodeIter<'a, NumericTypes: EvalexprNumericTypes> {
    node: Option<&'a CompiledNode<NumericTypes>>,
    stack: Vec<Iter<'a, CompiledNode<NumericTypes>>>,
}

impl<'a, NumericTypes: EvalexprNumericTypes> NodeIter<'a, NumericTypes> {
    fn new(node: &'a CompiledNode<NumericTypes>) -> Self {
        NodeIter {
            node: Some(node),
            stack: vec![node.children().iter()],
        }
    }
}

impl<'a, NumericTypes: EvalexprNumericTypes> Iterator for NodeIter<'a, NumericTypes> {
    type Item = &'a CompiledNode<NumericTypes>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(node) = self.node.take() {
            return Some(node);
        }
        loop {
            let mut result = None;

            if let Some(last) = self.stack.last_mut() {
                if let Some(next) = last.next() {
                    result = Some(next);
                } else {
                    // Can not fail because we just borrowed last.
                    // We just checked that the iterator is empty, so we can safely discard it.
                    let _ = self.stack.pop().unwrap();
                }
            } else {
                return None;
            }

            if let Some(result) = result {
                self.stack.push(result.children().iter());
                return Some(result);
            }
        }
    }
}

impl<NumericTypes: EvalexprNumericTypes> CompiledNode<NumericTypes> {
    /// Returns an iterator over all nodes in this tree.
    pub fn iter(&self) -> impl Iterator<Item = &CompiledNode<NumericTypes>> {
        NodeIter::new(self)
    }

}
