use std::any::Any;
use std::collections::HashMap;
use std::marker::PhantomData;

use smallvec::SmallVec;

use crate::graph::GraphSnapshot;
use crate::storage::{Edge, GraphStorage, Vertex};
use crate::value::Value;

pub struct Traversal<S, E, T> {
    pub source: S,
    pub _phantom: PhantomData<(E, T)>,
}

pub struct Traverser<E> {
    pub element: E,
    pub path: Path,
    pub loops: u32,
    pub sack: Option<Box<dyn Any + Send>>, // not clonable by design
    pub bulk: u64,
}

impl<E: Clone> Clone for Traverser<E> {
    fn clone(&self) -> Self {
        Traverser {
            element: self.element.clone(),
            path: self.path.clone(),
            loops: self.loops,
            sack: None, // omit sack clone; can be extended later
            bulk: self.bulk,
        }
    }
}

#[derive(Clone, Default)]
pub struct Path {
    pub objects: Vec<PathElement>,
    pub labels: HashMap<String, Vec<usize>>,
}

#[derive(Clone)]
pub struct PathElement {
    pub value: Value,
    pub labels: SmallVec<[String; 2]>,
}

pub struct GraphTraversalSource<'s> {
    pub(crate) snapshot: &'s GraphSnapshot<'s>,
}

impl<'s> GraphTraversalSource<'s> {
    fn storage(&self) -> &dyn GraphStorage {
        self.snapshot.graph.storage.as_ref()
    }

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
