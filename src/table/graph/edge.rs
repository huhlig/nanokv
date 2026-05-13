//
// Copyright 2025-2026 Hans W. Uhlig. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

//! Edge storage format and data structures for graph tables.

use crate::types::KeyBuf;

/// Represents an edge in the graph with all its properties.
#[derive(Clone, Debug, PartialEq)]
pub struct Edge {
    /// Unique edge identifier
    pub edge_id: KeyBuf,
    /// Source vertex
    pub source: KeyBuf,
    /// Edge label/type
    pub label: KeyBuf,
    /// Target vertex
    pub target: KeyBuf,
    /// Optional weight (for weighted graphs)
    pub weight: Option<f64>,
}

impl Edge {
    /// Create a new edge.
    pub fn new(
        edge_id: KeyBuf,
        source: KeyBuf,
        label: KeyBuf,
        target: KeyBuf,
        weight: Option<f64>,
    ) -> Self {
        Self {
            edge_id,
            source,
            label,
            target,
            weight,
        }
    }

    /// Create an unweighted edge.
    pub fn unweighted(edge_id: KeyBuf, source: KeyBuf, label: KeyBuf, target: KeyBuf) -> Self {
        Self::new(edge_id, source, label, target, None)
    }

    /// Create a weighted edge.
    pub fn weighted(
        edge_id: KeyBuf,
        source: KeyBuf,
        label: KeyBuf,
        target: KeyBuf,
        weight: f64,
    ) -> Self {
        Self::new(edge_id, source, label, target, Some(weight))
    }
}

/// Adjacency list for a vertex, storing all outgoing or incoming edges.
#[derive(Clone, Debug, Default)]
pub struct AdjacencyList {
    /// Edges organized by label for efficient filtering
    edges_by_label: std::collections::HashMap<KeyBuf, Vec<Edge>>,
    /// All edges in insertion order
    all_edges: Vec<Edge>,
}

impl AdjacencyList {
    /// Create a new empty adjacency list.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an edge to the adjacency list.
    pub fn add_edge(&mut self, edge: Edge) {
        let label = edge.label.clone();
        self.edges_by_label
            .entry(label)
            .or_insert_with(Vec::new)
            .push(edge.clone());
        self.all_edges.push(edge);
    }

    /// Remove an edge by edge_id.
    pub fn remove_edge(&mut self, edge_id: &[u8]) -> bool {
        // Find and remove from all_edges
        if let Some(pos) = self.all_edges.iter().position(|e| e.edge_id.0 == edge_id) {
            let edge = self.all_edges.remove(pos);
            
            // Remove from edges_by_label
            if let Some(edges) = self.edges_by_label.get_mut(&edge.label) {
                edges.retain(|e| e.edge_id.0 != edge_id);
                if edges.is_empty() {
                    self.edges_by_label.remove(&edge.label);
                }
            }
            true
        } else {
            false
        }
    }

    /// Get all edges with a specific label.
    pub fn edges_with_label(&self, label: &[u8]) -> Vec<&Edge> {
        self.edges_by_label
            .get(&KeyBuf(label.to_vec()))
            .map(|edges| edges.iter().collect())
            .unwrap_or_default()
    }

    /// Get all edges (any label).
    pub fn all_edges(&self) -> &[Edge] {
        &self.all_edges
    }

    /// Get the number of edges.
    pub fn len(&self) -> usize {
        self.all_edges.len()
    }

    /// Check if the adjacency list is empty.
    pub fn is_empty(&self) -> bool {
        self.all_edges.is_empty()
    }

    /// Get all unique labels.
    pub fn labels(&self) -> Vec<&KeyBuf> {
        self.edges_by_label.keys().collect()
    }
}

/// Storage key format for graph data.
///
/// We use a composite key format to enable efficient lookups:
/// - Outgoing edges: `out:{source}:{label}:{edge_id}`
/// - Incoming edges: `in:{target}:{label}:{edge_id}`
/// - Edge data: `edge:{edge_id}`
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GraphKey {
    /// Key for outgoing edge index: out:{source}:{label}:{edge_id}
    Outgoing {
        source: KeyBuf,
        label: KeyBuf,
        edge_id: KeyBuf,
    },
    /// Key for incoming edge index: in:{target}:{label}:{edge_id}
    Incoming {
        target: KeyBuf,
        label: KeyBuf,
        edge_id: KeyBuf,
    },
    /// Key for edge data: edge:{edge_id}
    EdgeData { edge_id: KeyBuf },
}

impl GraphKey {
    /// Encode the key into bytes for storage.
    pub fn encode(&self) -> Vec<u8> {
        match self {
            GraphKey::Outgoing {
                source,
                label,
                edge_id,
            } => {
                let mut key = Vec::new();
                key.extend_from_slice(b"out:");
                key.extend_from_slice(&source.0);
                key.push(b':');
                key.extend_from_slice(&label.0);
                key.push(b':');
                key.extend_from_slice(&edge_id.0);
                key
            }
            GraphKey::Incoming {
                target,
                label,
                edge_id,
            } => {
                let mut key = Vec::new();
                key.extend_from_slice(b"in:");
                key.extend_from_slice(&target.0);
                key.push(b':');
                key.extend_from_slice(&label.0);
                key.push(b':');
                key.extend_from_slice(&edge_id.0);
                key
            }
            GraphKey::EdgeData { edge_id } => {
                let mut key = Vec::new();
                key.extend_from_slice(b"edge:");
                key.extend_from_slice(&edge_id.0);
                key
            }
        }
    }

    /// Create a prefix for scanning outgoing edges from a vertex.
    pub fn outgoing_prefix(source: &[u8], label: Option<&[u8]>) -> Vec<u8> {
        let mut prefix = Vec::new();
        prefix.extend_from_slice(b"out:");
        prefix.extend_from_slice(source);
        if let Some(label) = label {
            prefix.push(b':');
            prefix.extend_from_slice(label);
        }
        prefix
    }

    /// Create a prefix for scanning incoming edges to a vertex.
    pub fn incoming_prefix(target: &[u8], label: Option<&[u8]>) -> Vec<u8> {
        let mut prefix = Vec::new();
        prefix.extend_from_slice(b"in:");
        prefix.extend_from_slice(target);
        if let Some(label) = label {
            prefix.push(b':');
            prefix.extend_from_slice(label);
        }
        prefix
    }
}

/// Serialized edge data stored in the underlying table.
#[derive(Clone, Debug)]
pub struct EdgeData {
    pub source: Vec<u8>,
    pub label: Vec<u8>,
    pub target: Vec<u8>,
    pub weight: Option<f64>,
}

impl EdgeData {
    /// Encode edge data into bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut data = Vec::new();
        
        // Encode source length and data
        data.extend_from_slice(&(self.source.len() as u32).to_le_bytes());
        data.extend_from_slice(&self.source);
        
        // Encode label length and data
        data.extend_from_slice(&(self.label.len() as u32).to_le_bytes());
        data.extend_from_slice(&self.label);
        
        // Encode target length and data
        data.extend_from_slice(&(self.target.len() as u32).to_le_bytes());
        data.extend_from_slice(&self.target);
        
        // Encode weight (1 byte flag + 8 bytes if present)
        if let Some(weight) = self.weight {
            data.push(1);
            data.extend_from_slice(&weight.to_le_bytes());
        } else {
            data.push(0);
        }
        
        data
    }

    /// Decode edge data from bytes.
    pub fn decode(data: &[u8]) -> Result<Self, String> {
        let mut offset = 0;

        // Decode source
        if data.len() < offset + 4 {
            return Err("Invalid edge data: too short for source length".to_string());
        }
        let source_len = u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]) as usize;
        offset += 4;
        if data.len() < offset + source_len {
            return Err("Invalid edge data: too short for source".to_string());
        }
        let source = data[offset..offset + source_len].to_vec();
        offset += source_len;

        // Decode label
        if data.len() < offset + 4 {
            return Err("Invalid edge data: too short for label length".to_string());
        }
        let label_len = u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]) as usize;
        offset += 4;
        if data.len() < offset + label_len {
            return Err("Invalid edge data: too short for label".to_string());
        }
        let label = data[offset..offset + label_len].to_vec();
        offset += label_len;

        // Decode target
        if data.len() < offset + 4 {
            return Err("Invalid edge data: too short for target length".to_string());
        }
        let target_len = u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]) as usize;
        offset += 4;
        if data.len() < offset + target_len {
            return Err("Invalid edge data: too short for target".to_string());
        }
        let target = data[offset..offset + target_len].to_vec();
        offset += target_len;

        // Decode weight
        if data.len() < offset + 1 {
            return Err("Invalid edge data: too short for weight flag".to_string());
        }
        let has_weight = data[offset] != 0;
        offset += 1;
        let weight = if has_weight {
            if data.len() < offset + 8 {
                return Err("Invalid edge data: too short for weight".to_string());
            }
            let weight_bytes = [
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ];
            Some(f64::from_le_bytes(weight_bytes))
        } else {
            None
        };

        Ok(Self {
            source,
            label,
            target,
            weight,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_edge_creation() {
        let edge = Edge::unweighted(
            KeyBuf(b"e1".to_vec()),
            KeyBuf(b"v1".to_vec()),
            KeyBuf(b"knows".to_vec()),
            KeyBuf(b"v2".to_vec()),
        );
        assert_eq!(edge.edge_id.0, b"e1");
        assert_eq!(edge.source.0, b"v1");
        assert_eq!(edge.label.0, b"knows");
        assert_eq!(edge.target.0, b"v2");
        assert_eq!(edge.weight, None);

        let weighted_edge = Edge::weighted(
            KeyBuf(b"e2".to_vec()),
            KeyBuf(b"v1".to_vec()),
            KeyBuf(b"likes".to_vec()),
            KeyBuf(b"v3".to_vec()),
            0.8,
        );
        assert_eq!(weighted_edge.weight, Some(0.8));
    }

    #[test]
    fn test_adjacency_list() {
        let mut adj = AdjacencyList::new();
        
        let edge1 = Edge::unweighted(
            KeyBuf(b"e1".to_vec()),
            KeyBuf(b"v1".to_vec()),
            KeyBuf(b"knows".to_vec()),
            KeyBuf(b"v2".to_vec()),
        );
        let edge2 = Edge::unweighted(
            KeyBuf(b"e2".to_vec()),
            KeyBuf(b"v1".to_vec()),
            KeyBuf(b"knows".to_vec()),
            KeyBuf(b"v3".to_vec()),
        );
        let edge3 = Edge::unweighted(
            KeyBuf(b"e3".to_vec()),
            KeyBuf(b"v1".to_vec()),
            KeyBuf(b"likes".to_vec()),
            KeyBuf(b"v4".to_vec()),
        );

        adj.add_edge(edge1);
        adj.add_edge(edge2);
        adj.add_edge(edge3);

        assert_eq!(adj.len(), 3);
        assert_eq!(adj.edges_with_label(b"knows").len(), 2);
        assert_eq!(adj.edges_with_label(b"likes").len(), 1);
        assert_eq!(adj.labels().len(), 2);

        assert!(adj.remove_edge(b"e1"));
        assert_eq!(adj.len(), 2);
        assert_eq!(adj.edges_with_label(b"knows").len(), 1);
    }

    #[test]
    fn test_graph_key_encoding() {
        let key = GraphKey::Outgoing {
            source: KeyBuf(b"v1".to_vec()),
            label: KeyBuf(b"knows".to_vec()),
            edge_id: KeyBuf(b"e1".to_vec()),
        };
        let encoded = key.encode();
        assert!(encoded.starts_with(b"out:"));

        let key2 = GraphKey::Incoming {
            target: KeyBuf(b"v2".to_vec()),
            label: KeyBuf(b"knows".to_vec()),
            edge_id: KeyBuf(b"e1".to_vec()),
        };
        let encoded2 = key2.encode();
        assert!(encoded2.starts_with(b"in:"));

        let key3 = GraphKey::EdgeData {
            edge_id: KeyBuf(b"e1".to_vec()),
        };
        let encoded3 = key3.encode();
        assert!(encoded3.starts_with(b"edge:"));
    }

    #[test]
    fn test_edge_data_encoding() {
        let data = EdgeData {
            source: b"v1".to_vec(),
            label: b"knows".to_vec(),
            target: b"v2".to_vec(),
            weight: Some(0.5),
        };

        let encoded = data.encode();
        let decoded = EdgeData::decode(&encoded).unwrap();

        assert_eq!(decoded.source, b"v1");
        assert_eq!(decoded.label, b"knows");
        assert_eq!(decoded.target, b"v2");
        assert_eq!(decoded.weight, Some(0.5));
    }

    #[test]
    fn test_edge_data_encoding_unweighted() {
        let data = EdgeData {
            source: b"v1".to_vec(),
            label: b"knows".to_vec(),
            target: b"v2".to_vec(),
            weight: None,
        };

        let encoded = data.encode();
        let decoded = EdgeData::decode(&encoded).unwrap();

        assert_eq!(decoded.source, b"v1");
        assert_eq!(decoded.label, b"knows");
        assert_eq!(decoded.target, b"v2");
        assert_eq!(decoded.weight, None);
    }
}

// Made with Bob