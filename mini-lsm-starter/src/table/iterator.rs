use std::sync::Arc;
use anyhow::{Result, Context};

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
/// SSTable 内容的迭代器
pub struct SsTableIterator {
    table: Arc<SsTable>,          // The SSTable this iterator is iterating over // 该迭代器正在迭代的 SSTable
    blk_iter: BlockIterator,      // The current block iterator // 当前的块迭代器
    blk_idx: usize,               // The index of the current block // 当前块的索引
}

impl SsTableIterator {
    /// Initializes the first data block iterator.
    /// 初始化第一个数据块迭代器
    fn initialize_first_block(table: &Arc<SsTable>) -> Result<(usize, BlockIterator)> {
        // Read the first block and create an iterator for it
        // 读取第一个块并为其创建迭代器
        let block = table.read_block_cached(0)
            .context("Failed to read the first block")?;
        let first_block = BlockIterator::create_and_seek_to_first(block);
        Ok((0, first_block))
    }

    /// Creates a new iterator and seeks to the first key-value pair.
    /// 创建一个新的迭代器并定位到第一个键值对
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        // Initialize the first block and create the iterator
        // 初始化第一个块并创建迭代器
        let (blk_idx, blk_iter) = Self::initialize_first_block(&table)?;
        Ok(Self {
            table,
            blk_iter,
            blk_idx,
        })
    }

    /// Seeks to the first key-value pair in the first data block.
    /// 定位到第一个数据块中的第一个键值对
    pub fn seek_to_first(&mut self) -> Result<()> {
        // Reinitialize the first block and update the iterator
        // 重新初始化第一个块并更新迭代器
        let (blk_idx, blk_iter) = Self::initialize_first_block(&self.table)?;
        self.blk_idx = blk_idx;
        self.blk_iter = blk_iter;
        Ok(())
    }

    /// Initializes the block iterator starting from a given key.
    /// 初始化从给定键开始的块迭代器
    fn initialize_key_block(table: &Arc<SsTable>, key: KeySlice) -> Result<(usize, BlockIterator)> {
        // Find the block that may contain the key and create an iterator for it
        // 找到可能包含键的块并为其创建迭代器
        let mut blk_idx = table.find_block_idx(key);
        let block = table.read_block_cached(blk_idx)
            .context(format!("Failed to read block at index {}", blk_idx))?;
        let mut blk_iter = BlockIterator::create_and_seek_to_key(block, key);

        // If the key is not found, move to the next block
        // 如果未找到键，则移动到下一个块
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
    /// 创建一个新的迭代器并定位到第一个大于等于 `key` 的键值对
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        // Initialize the block starting from the given key and create the iterator
        // 初始化从给定键开始的块并创建迭代器
        let (blk_idx, blk_iter) = Self::initialize_key_block(&table, key)?;
        Ok(Self {
            blk_idx,
            table,
            blk_iter,
        })
    }

    /// Seeks to the first key-value pair which >= `key`.
    /// 定位到第一个大于等于 `key` 的键值对
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        // Reinitialize the block starting from the given key and update the iterator
        // 重新初始化从给定键开始的块并更新迭代器
        let (blk_idx, blk_iter) = Self::initialize_key_block(&self.table, key)?;
        self.blk_idx = blk_idx;
        self.blk_iter = blk_iter;
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Returns the key held by the underlying block iterator.
    /// 返回底层块迭代器持有的键
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Returns the value held by the underlying block iterator.
    /// 返回底层块迭代器持有的值
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Returns whether the current block iterator is valid or not.
    /// 返回当前块迭代器是否有效
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Moves to the next key in the block.
    /// 移动到块中的下一个键
    ///
    /// After moving to the next key, if the current block iterator is not valid,
    /// advances to the next block iterator if available.
    /// 移动到下一个键后，如果当前块迭代器无效，则移到下一个可用的块迭代器。
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
}
