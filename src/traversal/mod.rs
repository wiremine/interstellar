use std::marker::PhantomData;

use crate::graph::Graph;
use crate::storage::{Edge, Vertex};

pub struct Traversal<S, E, T> {
    pub source: S,
    pub _phantom: PhantomData<(E, T)>,
}

#[derive(Clone)]
pub struct Traverser<E> {
    pub element: E,
}

#[derive(Clone, Default)]
pub struct Path;

pub struct GraphTraversalSource<'g> {
    pub graph: &'g Graph,
}

impl<'g> GraphTraversalSource<'g> {
    pub fn v(self) -> Traversal<Self, Vertex, Traverser<Vertex>> {
        Traversal {
            source: self,
            _phantom: PhantomData,
        }
    }

    pub fn e(self) -> Traversal<Self, Edge, Traverser<Edge>> {
        Traversal {
            source: self,
            _phantom: PhantomData,
        }
    }
}

pub mod p {}
pub mod __ {}
