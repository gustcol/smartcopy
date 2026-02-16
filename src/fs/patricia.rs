//! Slab-based Patricia tree for memory-efficient path deduplication
//!
//! Uses a compact radix tree (Patricia trie) to store file paths with shared
//! prefix compression. Paths like `/data/project/src/a.rs` and
//! `/data/project/src/b.rs` share the prefix nodes, reducing memory usage
//! compared to a HashSet<String>.
//!
//! Ported from Andromeda's deduplication approach to give SmartCopy
//! memory-efficient path storage for million-file manifests.

use std::collections::HashMap;

/// Initial slab capacity for node allocation.
const DEFAULT_SLAB_CAPACITY: usize = 32 * 1024;

/// Index into the node slab.
type NodeIdx = u32;

/// A single node in the Patricia trie.
struct PatriciaNode {
    /// The label (edge bytes) leading to this node.
    label: Vec<u8>,
    /// Children keyed by their first byte for O(1) dispatch.
    children: HashMap<u8, NodeIdx>,
    /// Whether this node represents a complete inserted key.
    is_terminal: bool,
}

/// Slab-based Patricia tree for byte-string keys.
///
/// Stores paths with shared-prefix compression. All nodes live in a
/// contiguous `Vec` (the slab), avoiding per-node heap allocations and
/// improving cache locality.
pub struct PatriciaTree {
    nodes: Vec<PatriciaNode>,
    len: usize,
}

impl PatriciaTree {
    /// Create a new empty Patricia tree with default slab capacity.
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_SLAB_CAPACITY)
    }

    /// Create a new empty Patricia tree with the given slab capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        let mut nodes = Vec::with_capacity(capacity);
        // Root node (index 0) with empty label.
        nodes.push(PatriciaNode {
            label: Vec::new(),
            children: HashMap::new(),
            is_terminal: false,
        });
        Self { nodes, len: 0 }
    }

    /// Allocate a new node in the slab and return its index.
    fn alloc_node(&mut self, label: Vec<u8>, is_terminal: bool) -> NodeIdx {
        let idx = self.nodes.len() as NodeIdx;
        self.nodes.push(PatriciaNode {
            label,
            children: HashMap::new(),
            is_terminal,
        });
        idx
    }

    /// Compute the common prefix length between two byte slices.
    /// Uses 8-byte-at-a-time comparison for speed on long paths.
    fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
        let min_len = a.len().min(b.len());

        // Fast path: compare 8 bytes at a time
        let chunks = min_len / 8;
        for i in 0..chunks {
            let off = i * 8;
            let va = u64::from_ne_bytes(a[off..off + 8].try_into().unwrap());
            let vb = u64::from_ne_bytes(b[off..off + 8].try_into().unwrap());
            if va != vb {
                // Find exact mismatch byte within this u64
                let diff = va ^ vb;
                // On little-endian the trailing zeros give the first differing byte;
                // on big-endian, leading zeros do. Use to_le() to normalise.
                let byte_offset = (diff.to_le().trailing_zeros() / 8) as usize;
                return off + byte_offset;
            }
        }

        // Tail: compare remaining bytes one by one
        let tail_start = chunks * 8;
        for i in tail_start..min_len {
            if a[i] != b[i] {
                return i;
            }
        }

        min_len
    }

    /// Insert a key into the trie. Returns `true` if the key was newly inserted,
    /// `false` if it was already present.
    pub fn insert(&mut self, key: &[u8]) -> bool {
        let mut node_idx: NodeIdx = 0; // start at root
        let mut remaining = key;

        loop {
            if remaining.is_empty() {
                let node = &mut self.nodes[node_idx as usize];
                if node.is_terminal {
                    return false; // already present
                }
                node.is_terminal = true;
                self.len += 1;
                return true;
            }

            let first_byte = remaining[0];

            // Check if there's a child starting with this byte
            let child_idx = self.nodes[node_idx as usize].children.get(&first_byte).copied();

            match child_idx {
                None => {
                    // No child with this prefix byte -> create a new leaf
                    let new_idx = self.alloc_node(remaining.to_vec(), true);
                    self.nodes[node_idx as usize].children.insert(first_byte, new_idx);
                    self.len += 1;
                    return true;
                }
                Some(child) => {
                    let child_label_len = self.nodes[child as usize].label.len();
                    let prefix_len = Self::common_prefix_len(
                        &self.nodes[child as usize].label,
                        remaining,
                    );

                    if prefix_len == child_label_len {
                        // Full match of child label -> descend
                        remaining = &remaining[prefix_len..];
                        node_idx = child;
                        continue;
                    }

                    // Partial match -> split the child node
                    // Create an intermediate node for the shared prefix
                    let shared_prefix = self.nodes[child as usize].label[..prefix_len].to_vec();
                    let child_suffix = self.nodes[child as usize].label[prefix_len..].to_vec();
                    let remaining_suffix = &remaining[prefix_len..];

                    // The intermediate node takes the shared prefix
                    let mid_idx = self.alloc_node(shared_prefix, remaining_suffix.is_empty());

                    // Update the old child: its label becomes the suffix after the split
                    let child_suffix_first = child_suffix[0];
                    self.nodes[child as usize].label = child_suffix;

                    // Wire the old child under the new intermediate node
                    self.nodes[mid_idx as usize].children.insert(child_suffix_first, child);

                    // Replace parent's pointer to old child with the intermediate
                    self.nodes[node_idx as usize].children.insert(first_byte, mid_idx);

                    if remaining_suffix.is_empty() {
                        // The key ends exactly at the split point
                        self.len += 1;
                        return true;
                    }

                    // Create a new leaf for the remaining suffix
                    let leaf_idx = self.alloc_node(remaining_suffix.to_vec(), true);
                    self.nodes[mid_idx as usize]
                        .children
                        .insert(remaining_suffix[0], leaf_idx);

                    self.len += 1;
                    return true;
                }
            }
        }
    }

    /// Insert a string key.
    pub fn insert_str(&mut self, key: &str) -> bool {
        self.insert(key.as_bytes())
    }

    /// Check if a key exists in the trie.
    pub fn contains(&self, key: &[u8]) -> bool {
        let mut node_idx: NodeIdx = 0;
        let mut remaining = key;

        loop {
            if remaining.is_empty() {
                return self.nodes[node_idx as usize].is_terminal;
            }

            let first_byte = remaining[0];
            match self.nodes[node_idx as usize].children.get(&first_byte) {
                None => return false,
                Some(&child) => {
                    let label = &self.nodes[child as usize].label;
                    let prefix_len = Self::common_prefix_len(label, remaining);
                    if prefix_len < label.len() {
                        return false;
                    }
                    remaining = &remaining[prefix_len..];
                    node_idx = child;
                }
            }
        }
    }

    /// Check if a string key exists.
    pub fn contains_str(&self, key: &str) -> bool {
        self.contains(key.as_bytes())
    }

    /// Return the number of inserted keys.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Return whether the trie is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Return the number of allocated slab nodes (for diagnostics).
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Iterate over all keys in the trie in lexicographic order.
    /// Uses a reusable String buffer to avoid per-key allocations.
    pub fn iter(&self) -> PatriciaIter<'_> {
        PatriciaIter {
            tree: self,
            stack: vec![(0, 0)],
            buffer: Vec::new(),
        }
    }
}

impl Default for PatriciaTree {
    fn default() -> Self {
        Self::new()
    }
}

/// Iterator over all keys in the Patricia tree.
pub struct PatriciaIter<'a> {
    tree: &'a PatriciaTree,
    /// Stack of (node_idx, depth_to_restore_buffer_to) for DFS
    stack: Vec<(NodeIdx, usize)>,
    /// Reusable buffer for building the current key
    buffer: Vec<u8>,
}

impl<'a> Iterator for PatriciaIter<'a> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((node_idx, restore_len)) = self.stack.pop() {
            let node = &self.tree.nodes[node_idx as usize];

            // Restore buffer to the appropriate depth
            self.buffer.truncate(restore_len);
            self.buffer.extend_from_slice(&node.label);

            // Push children in reverse order so smallest byte comes out first
            let mut child_keys: Vec<u8> = node.children.keys().copied().collect();
            child_keys.sort_unstable();
            for &byte in child_keys.iter().rev() {
                let child_idx = node.children[&byte];
                self.stack.push((child_idx, self.buffer.len()));
            }

            if node.is_terminal {
                return Some(self.buffer.clone());
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_contains() {
        let mut tree = PatriciaTree::new();

        assert!(tree.insert_str("/data/project/src/a.rs"));
        assert!(tree.insert_str("/data/project/src/b.rs"));
        assert!(tree.insert_str("/data/project/Cargo.toml"));
        assert!(tree.insert_str("/other/path"));

        assert!(tree.contains_str("/data/project/src/a.rs"));
        assert!(tree.contains_str("/data/project/src/b.rs"));
        assert!(tree.contains_str("/data/project/Cargo.toml"));
        assert!(tree.contains_str("/other/path"));

        assert!(!tree.contains_str("/data/project/src"));
        assert!(!tree.contains_str("/data/project/src/c.rs"));
        assert!(!tree.contains_str("/nonexistent"));

        assert_eq!(tree.len(), 4);
    }

    #[test]
    fn test_duplicate_insert() {
        let mut tree = PatriciaTree::new();

        assert!(tree.insert_str("hello"));
        assert!(!tree.insert_str("hello")); // duplicate
        assert_eq!(tree.len(), 1);
    }

    #[test]
    fn test_prefix_key() {
        let mut tree = PatriciaTree::new();

        assert!(tree.insert_str("/a/b/c"));
        assert!(tree.insert_str("/a/b"));
        assert!(tree.insert_str("/a"));

        assert!(tree.contains_str("/a"));
        assert!(tree.contains_str("/a/b"));
        assert!(tree.contains_str("/a/b/c"));
        assert_eq!(tree.len(), 3);
    }

    #[test]
    fn test_iteration_sorted() {
        let mut tree = PatriciaTree::new();
        tree.insert_str("banana");
        tree.insert_str("apple");
        tree.insert_str("cherry");
        tree.insert_str("avocado");

        let keys: Vec<String> = tree
            .iter()
            .map(|b| String::from_utf8(b).unwrap())
            .collect();

        assert_eq!(keys, vec!["apple", "avocado", "banana", "cherry"]);
    }

    #[test]
    fn test_empty_tree() {
        let tree = PatriciaTree::new();
        assert!(tree.is_empty());
        assert_eq!(tree.len(), 0);
        assert!(!tree.contains_str("anything"));
        assert_eq!(tree.iter().count(), 0);
    }

    #[test]
    fn test_large_dataset() {
        let mut tree = PatriciaTree::new();

        for i in 0..10_000 {
            let path = format!("/data/project/module{}/file{}.rs", i % 100, i);
            tree.insert_str(&path);
        }

        assert_eq!(tree.len(), 10_000);

        // Verify a sample
        assert!(tree.contains_str("/data/project/module0/file0.rs"));
        assert!(tree.contains_str("/data/project/module99/file9999.rs"));
        assert!(!tree.contains_str("/data/project/module0/file99999.rs"));

        // Verify iteration count
        assert_eq!(tree.iter().count(), 10_000);
    }

    #[test]
    fn test_common_prefix_len_fast_path() {
        // Test the 8-byte-at-a-time comparison
        let a = b"/data/project/really/long/path/to/some/file.rs";
        let b = b"/data/project/really/long/path/to/other/file.rs";
        let prefix = PatriciaTree::common_prefix_len(a, b);
        assert_eq!(&a[..prefix], &b[..prefix]);
        assert_eq!(prefix, 34); // diverges at "some" vs "other"
    }

    #[test]
    fn test_single_byte_keys() {
        let mut tree = PatriciaTree::new();
        tree.insert(b"a");
        tree.insert(b"b");
        tree.insert(b"c");

        assert!(tree.contains(b"a"));
        assert!(tree.contains(b"b"));
        assert!(tree.contains(b"c"));
        assert!(!tree.contains(b"d"));
        assert_eq!(tree.len(), 3);
    }
}
