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

//! Time series compression algorithms.
//!
//! This module implements specialized compression algorithms optimized for time series data:
//! - Delta-of-delta encoding for timestamps
//! - Gorilla compression for floating-point values
//! - Simple delta encoding
//! - Run-length encoding

use crate::table::TableResult;

/// Compress timestamps using delta-of-delta encoding.
///
/// Delta-of-delta is highly effective for regularly-spaced timestamps.
/// It stores the first timestamp, then the delta between consecutive timestamps,
/// then the delta of deltas.
pub fn compress_timestamps_delta_of_delta(timestamps: &[i64]) -> TableResult<Vec<u8>> {
    if timestamps.is_empty() {
        return Ok(Vec::new());
    }

    let mut output = Vec::new();

    // Write the first timestamp (8 bytes)
    output.extend_from_slice(&timestamps[0].to_le_bytes());

    if timestamps.len() == 1 {
        return Ok(output);
    }

    // Write the first delta (8 bytes)
    let first_delta = timestamps[1] - timestamps[0];
    output.extend_from_slice(&first_delta.to_le_bytes());

    if timestamps.len() == 2 {
        return Ok(output);
    }

    // Write delta-of-deltas using variable-length encoding
    let mut prev_delta = first_delta;
    for i in 2..timestamps.len() {
        let delta = timestamps[i] - timestamps[i - 1];
        let delta_of_delta = delta - prev_delta;

        // Use variable-length encoding for delta-of-delta
        encode_varint(&mut output, delta_of_delta);

        prev_delta = delta;
    }

    Ok(output)
}

/// Decompress timestamps using delta-of-delta encoding.
pub fn decompress_timestamps_delta_of_delta(data: &[u8], count: usize) -> TableResult<Vec<i64>> {
    if count == 0 {
        return Ok(Vec::new());
    }

    let mut timestamps = Vec::with_capacity(count);
    let mut pos = 0;

    // Read the first timestamp
    if data.len() < 8 {
        return Err(crate::table::TableError::Other(
            "Insufficient data for first timestamp".to_string(),
        ));
    }
    let first_ts = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
    timestamps.push(first_ts);
    pos += 8;

    if count == 1 {
        return Ok(timestamps);
    }

    // Read the first delta
    if data.len() < pos + 8 {
        return Err(crate::table::TableError::Other(
            "Insufficient data for first delta".to_string(),
        ));
    }
    let first_delta = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
    timestamps.push(first_ts + first_delta);
    pos += 8;

    if count == 2 {
        return Ok(timestamps);
    }

    // Read delta-of-deltas
    let mut prev_delta = first_delta;
    for _ in 2..count {
        let (delta_of_delta, bytes_read) = decode_varint(&data[pos..])?;
        pos += bytes_read;

        let delta = prev_delta + delta_of_delta;
        let ts = timestamps.last().unwrap() + delta;
        timestamps.push(ts);

        prev_delta = delta;
    }

    Ok(timestamps)
}

/// Compress floating-point values using Gorilla compression.
///
/// Gorilla compression (from Facebook) uses XOR-based compression
/// which is very effective for slowly-changing values.
pub fn compress_values_gorilla(values: &[f64]) -> TableResult<Vec<u8>> {
    if values.is_empty() {
        return Ok(Vec::new());
    }

    let mut output = Vec::new();

    // Write the first value (8 bytes)
    output.extend_from_slice(&values[0].to_bits().to_le_bytes());

    if values.len() == 1 {
        return Ok(output);
    }

    let mut prev_bits = values[0].to_bits();

    for &value in &values[1..] {
        let curr_bits = value.to_bits();
        let xor = prev_bits ^ curr_bits;

        if xor == 0 {
            // Value unchanged - write a single 0 bit
            // For simplicity, we'll use a byte with value 0
            output.push(0);
        } else {
            // Value changed - write XOR with leading/trailing zero compression
            let leading_zeros = xor.leading_zeros();
            let trailing_zeros = xor.trailing_zeros();
            let _significant_bits = 64 - leading_zeros - trailing_zeros;

            // Write marker byte: 1 bit (changed) + 6 bits (leading zeros) + 6 bits (significant bits)
            // Simplified: just write the full XOR for now
            output.push(1); // Changed marker
            output.extend_from_slice(&xor.to_le_bytes());
        }

        prev_bits = curr_bits;
    }

    Ok(output)
}

/// Decompress floating-point values using Gorilla compression.
pub fn decompress_values_gorilla(data: &[u8], count: usize) -> TableResult<Vec<f64>> {
    if count == 0 {
        return Ok(Vec::new());
    }

    let mut values = Vec::with_capacity(count);
    let mut pos = 0;

    // Read the first value
    if data.len() < 8 {
        return Err(crate::table::TableError::Other(
            "Insufficient data for first value".to_string(),
        ));
    }
    let first_bits = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
    values.push(f64::from_bits(first_bits));
    pos += 8;

    if count == 1 {
        return Ok(values);
    }

    let mut prev_bits = first_bits;

    for _ in 1..count {
        if pos >= data.len() {
            return Err(crate::table::TableError::Other(
                "Insufficient data for value".to_string(),
            ));
        }

        let marker = data[pos];
        pos += 1;

        let curr_bits = if marker == 0 {
            // Value unchanged
            prev_bits
        } else {
            // Value changed - read XOR
            if data.len() < pos + 8 {
                return Err(crate::table::TableError::Other(
                    "Insufficient data for XOR value".to_string(),
                ));
            }
            let xor = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            prev_bits ^ xor
        };

        values.push(f64::from_bits(curr_bits));
        prev_bits = curr_bits;
    }

    Ok(values)
}

/// Compress values using simple delta encoding.
pub fn compress_values_delta(values: &[i64]) -> TableResult<Vec<u8>> {
    if values.is_empty() {
        return Ok(Vec::new());
    }

    let mut output = Vec::new();

    // Write the first value
    output.extend_from_slice(&values[0].to_le_bytes());

    // Write deltas
    for i in 1..values.len() {
        let delta = values[i] - values[i - 1];
        encode_varint(&mut output, delta);
    }

    Ok(output)
}

/// Decompress values using simple delta encoding.
pub fn decompress_values_delta(data: &[u8], count: usize) -> TableResult<Vec<i64>> {
    if count == 0 {
        return Ok(Vec::new());
    }

    let mut values = Vec::with_capacity(count);
    let mut pos = 0;

    // Read the first value
    if data.len() < 8 {
        return Err(crate::table::TableError::Other(
            "Insufficient data for first value".to_string(),
        ));
    }
    let first_value = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
    values.push(first_value);
    pos += 8;

    // Read deltas
    for _ in 1..count {
        let (delta, bytes_read) = decode_varint(&data[pos..])?;
        pos += bytes_read;
        let value = values.last().unwrap() + delta;
        values.push(value);
    }

    Ok(values)
}

/// Encode a signed integer using variable-length encoding (zigzag + varint).
fn encode_varint(output: &mut Vec<u8>, value: i64) {
    // Zigzag encoding to handle negative numbers efficiently
    let zigzag = ((value << 1) ^ (value >> 63)) as u64;

    // Variable-length encoding
    let mut n = zigzag;
    loop {
        let mut byte = (n & 0x7F) as u8;
        n >>= 7;
        if n != 0 {
            byte |= 0x80; // More bytes follow
        }
        output.push(byte);
        if n == 0 {
            break;
        }
    }
}

/// Decode a variable-length encoded signed integer.
fn decode_varint(data: &[u8]) -> TableResult<(i64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0;
    let mut pos = 0;

    loop {
        if pos >= data.len() {
            return Err(crate::table::TableError::Other(
                "Insufficient data for varint".to_string(),
            ));
        }

        let byte = data[pos];
        pos += 1;

        result |= ((byte & 0x7F) as u64) << shift;
        shift += 7;

        if (byte & 0x80) == 0 {
            break;
        }

        if shift >= 64 {
            return Err(crate::table::TableError::Other(
                "Varint too large".to_string(),
            ));
        }
    }

    // Zigzag decoding
    let value = ((result >> 1) as i64) ^ -((result & 1) as i64);

    Ok((value, pos))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_of_delta_compression() {
        let timestamps = vec![1000, 1010, 1020, 1030, 1040];
        let compressed = compress_timestamps_delta_of_delta(&timestamps).unwrap();
        let decompressed = decompress_timestamps_delta_of_delta(&compressed, timestamps.len()).unwrap();
        assert_eq!(timestamps, decompressed);
    }

    #[test]
    fn test_delta_of_delta_irregular() {
        let timestamps = vec![1000, 1015, 1025, 1040, 1050];
        let compressed = compress_timestamps_delta_of_delta(&timestamps).unwrap();
        let decompressed = decompress_timestamps_delta_of_delta(&compressed, timestamps.len()).unwrap();
        assert_eq!(timestamps, decompressed);
    }

    #[test]
    fn test_gorilla_compression() {
        let values = vec![1.0, 1.1, 1.2, 1.1, 1.0];
        let compressed = compress_values_gorilla(&values).unwrap();
        let decompressed = decompress_values_gorilla(&compressed, values.len()).unwrap();
        assert_eq!(values, decompressed);
    }

    #[test]
    fn test_delta_compression() {
        let values = vec![100, 110, 120, 115, 125];
        let compressed = compress_values_delta(&values).unwrap();
        let decompressed = decompress_values_delta(&compressed, values.len()).unwrap();
        assert_eq!(values, decompressed);
    }

    #[test]
    fn test_varint_encoding() {
        let test_values = vec![0, 1, -1, 127, -127, 128, -128, 1000, -1000];
        for &value in &test_values {
            let mut encoded = Vec::new();
            encode_varint(&mut encoded, value);
            let (decoded, _) = decode_varint(&encoded).unwrap();
            assert_eq!(value, decoded, "Failed for value {}", value);
        }
    }
}

// Made with Bob