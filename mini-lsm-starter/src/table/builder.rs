#![allow(unused_variables)] // TODO: remove this lint after implementing this mod
#![allow(dead_code)] // TODO: remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use xxhash_rust::xxh32::Xxh32;
use anyhow::Result;
use bytes::BufMut;

use super::bloom::Bloom;
use super::{BlockMeta, FileObject, SsTable};
use crate::block::BlockBuilder;
use crate::key::{KeySlice, KeyVec};
use crate::lsm_storage::BlockCache;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    // BlockBuilder instance to build the current block.
    builder: BlockBuilder,
    // First key in the current block.
    first_key: KeyVec,
    // Last key in the current block.
    last_key: KeyVec,
    // Data buffer storing all encoded blocks.
    data: Vec<u8>,
    // Metadata for each block (e.g., offset, keys).
    pub(crate) meta: Vec<BlockMeta>,
    // Target size of each block.
    block_size: usize,
    // Hashes of all keys, used for building Bloom filter.
    key_hashes: Vec<u32>,
    // Maximum timestamp of all keys.
    max_ts: u64,
}

impl SsTableBuilder {
    /// Create a builder based on the target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            data: Vec::new(),
            meta: Vec::new(),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            block_size,
            builder: BlockBuilder::new(block_size),
            key_hashes: Vec::new(),
            max_ts: 0,
        }
    }

    /// Adds a key-value pair to the SSTable.
    ///
    /// # Arguments
    /// * `key`: The key to add.
    /// * `value`: The value associated with the key.
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        // If the first key is empty, set it to the current key.
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        }

        // Generate timestamp
        let timestamp = generate_timestamp();
        println!("Timestamp: {}", timestamp);

        // Update max timestamp if necessary
        if timestamp > self.max_ts {
            self.max_ts = timestamp;
        }

        // Generate and store the hash of the key using Xxh32
        let mut hasher = Xxh32::new(0);
        hasher.update(key.into_inner());
        let hash = hasher.digest();
        println!("Key hash: {}", hash);
        self.key_hashes.push(hash);

        // Try to add the key-value pair to the current block.
        if !self.builder.add(key, value) {
            // If the current block is full, finish the block and start a new one.
            self.finish_block();
            assert!(self.builder.add(key, value));
            self.first_key.set_from_slice(key);
        }

        // Update last key to the current key
        self.last_key.set_from_slice(key);

        // Define the function to generate a timestamp
        fn generate_timestamp() -> u64 {
            // Get current system time
            let now = SystemTime::now();

            // Calculate duration since UNIX epoch
            let duration = now.duration_since(UNIX_EPOCH).expect("Time went backwards");

            // Return the number of seconds as u64
            duration.as_secs()
        }
    }

    /// Get the estimated size of the SSTable.
    ///
    /// # Returns
    /// * The size of the current encoded data.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Finishes the current block and encodes it into the data buffer.
    fn finish_block(&mut self) {
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let encoded_block = builder.build().encode();
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: std::mem::take(&mut self.first_key).into_key_bytes(),
            last_key: std::mem::take(&mut self.last_key).into_key_bytes(),
        });
        let checksum = crc32fast::hash(&encoded_block);
        self.data.extend(encoded_block);
        self.data.put_u32(checksum);
    }

    /// Builds the SSTable and writes it to the given path.
    ///
    /// # Arguments
    /// * `id`: Identifier for the SSTable.
    /// * `block_cache`: Optional cache for blocks.
    /// * `path`: Path to the file where the SSTable will be saved.
    ///
    /// # Returns
    /// * A result containing the constructed `SsTable` if successful.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // Finish the last block
        self.finish_block();

        // Take ownership of the data buffer and calculate metadata offset
        let mut buf = self.data;
        let meta_offset = buf.len();

        // Encode block metadata and append it to the buffer
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        buf.put_u32(meta_offset as u32); // Record the offset of the metadata

        // Build the Bloom filter from the key hashes
        let bloom = Bloom::build_from_key_hashes(
            &self.key_hashes,
            Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01),
        );

        // Record the offset for the Bloom filter
        let bloom_offset = buf.len();
        bloom.encode(&mut buf);
        buf.put_u32(bloom_offset as u32); // Record the Bloom filter offset

     

        // Create the FileObject and write the buffer to disk
        let file = FileObject::create(path.as_ref(), buf)?;

        // Return the constructed SsTable with all relevant metadata
        Ok(SsTable {
            id,
            file,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.last().unwrap().last_key.clone(),
            block_meta: self.meta,
            block_meta_offset: meta_offset, // Offset where block metadata starts
            block_cache,
            bloom: Some(bloom),
            max_ts: self.max_ts, // Use the latest timestamp tracked
        })
    }

    /// Builds the Bloom filter from key hashes.
    ///
    /// # Returns
    /// * A Bloom filter instance.
    fn build_bloom_filter(&self) -> Bloom {
        Bloom::build_from_key_hashes(
            &self.key_hashes,
            Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01),
        )
    }

    /// Builds the SSTable for testing purposes.
    ///
    /// # Arguments
    /// * `path`: Path to the file where the SSTable will be saved.
    ///
    /// # Returns
    /// * A result containing the constructed `SsTable` if successful.
    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
