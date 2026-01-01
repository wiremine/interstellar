use std::collections::HashMap;

pub struct StringInterner {
    forward: HashMap<String, u32>,
    reverse: HashMap<u32, String>,
    next_id: u32,
}

impl StringInterner {
    pub fn new() -> Self {
        StringInterner {
            forward: HashMap::new(),
            reverse: HashMap::new(),
            next_id: 0,
        }
    }

    pub fn intern(&mut self, value: &str) -> u32 {
        if let Some(id) = self.forward.get(value) {
            return *id;
        }
        let id = self.next_id;
        self.forward.insert(value.to_owned(), id);
        self.reverse.insert(id, value.to_owned());
        self.next_id = self
            .next_id
            .checked_add(1)
            .expect("string interner id overflow");
        id
    }

    pub fn resolve(&self, id: u32) -> Option<&str> {
        self.reverse.get(&id).map(|s| s.as_str())
    }

    /// Look up a string's ID without interning (read-only)
    /// Returns None if the string has not been interned
    pub fn lookup(&self, value: &str) -> Option<u32> {
        self.forward.get(value).copied()
    }

    /// Number of interned strings
    pub fn len(&self) -> usize {
        self.forward.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.forward.is_empty()
    }
}

impl Default for StringInterner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interns_and_resolves_strings() {
        let mut interner = StringInterner::new();
        let first = interner.intern("person");
        let second = interner.intern("person");
        let third = interner.intern("knows");

        assert_eq!(first, second);
        assert_ne!(first, third);
        assert_eq!(interner.resolve(first), Some("person"));
        assert_eq!(interner.resolve(third), Some("knows"));
    }

    #[test]
    fn lookup_without_interning() {
        let mut interner = StringInterner::new();
        interner.intern("exists");

        assert_eq!(interner.lookup("exists"), Some(0));
        assert_eq!(interner.lookup("missing"), None);
    }

    #[test]
    fn len_and_is_empty() {
        let mut interner = StringInterner::new();
        assert!(interner.is_empty());
        assert_eq!(interner.len(), 0);

        interner.intern("one");
        interner.intern("two");
        interner.intern("one"); // duplicate

        assert!(!interner.is_empty());
        assert_eq!(interner.len(), 2);
    }
}
