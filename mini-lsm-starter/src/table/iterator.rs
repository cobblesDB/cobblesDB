use std::sync::Arc;
use anyhow::{Result, Context};

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,          // The SSTable this iterator is iterating over
    blk_iter: BlockIterator,      // The current block iterator
    blk_idx: usize,               // The index of the current block
}

impl SsTableIterator {
    /// Initializes the first data block iterator.
    fn initialize_first_block(table: &Arc<SsTable>) -> Result<(usize, BlockIterator)> {
        // Read the first block and create an iterator for it
        let block = table.read_block_cached(0)
            .context("Failed to read the first block")?;
        let first_block = BlockIterator::create_and_seek_to_first(block);
        Ok((0, first_block))
    }

    /// Creates a new iterator and seeks to the first key-value pair.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        // Initialize the first block and create the iterator
        let (blk_idx, blk_iter) = Self::initialize_first_block(&table)?;
        Ok(Self {
            table,
            blk_iter,
            blk_idx,
        })
    }

    /// Seeks to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        // Reinitialize the first block and update the iterator
        let (blk_idx, blk_iter) = Self::initialize_first_block(&self.table)?;
        self.blk_idx = blk_idx;
        self.blk_iter = blk_iter;
        Ok(())
    }

    /// Initializes the block iterator starting from a given key.
    fn initialize_key_block(table: &Arc<SsTable>, key: KeySlice) -> Result<(usize, BlockIterator)> {
        // Find the block that may contain the key and create an iterator for it
        let mut blk_idx = table.find_block_idx(key);
        let block = table.read_block_cached(blk_idx)
            .context(format!("Failed to read block at index {}", blk_idx))?;
        let mut blk_iter = BlockIterator::create_and_seek_to_key(block, key);

        // If the key is not found, move to the next block
        if !blk_iter.is_valid() {
            blk_idx = blk_idx.checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("Block index overflow"))?;
            if blk_idx < table.num_of_blocks() {
                let next_block = table.read_block_cached(blk_idx)
                    .context(format!("Failed to read block at index {}", blk_idx))?;
                blk_iter = BlockIterator::create_and_seek_to_first(next_block);
            }
        }

        Ok((blk_idx, blk_iter))
    }

    /// Creates a new iterator and seeks to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        // Initialize the block starting from the given key and create the iterator
        let (blk_idx, blk_iter) = Self::initialize_key_block(&table, key)?;
        Ok(Self {
            blk_idx,
            table,
            blk_iter,
        })
    }

    /// Seeks to the first key-value pair which >= `key`.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        // Reinitialize the block starting from the given key and update the iterator
        let (blk_idx, blk_iter) = Self::initialize_key_block(&self.table, key)?;
        self.blk_idx = blk_idx;
        self.blk_iter = blk_iter;
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Returns the key held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Returns the value held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Returns whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Moves to the next key in the block.
    ///
    /// After moving to the next key, if the current block iterator is not valid,
    /// advances to the next block iterator if available.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.blk_iter.is_valid() {
            self.blk_idx += 1;
            if self.blk_idx < self.table.num_of_blocks() {
                self.blk_iter = BlockIterator::create_and_seek_to_first(
                    self.table.read_block_cached(self.blk_idx)?,
                );
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
