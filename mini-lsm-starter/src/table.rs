pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::io::{Cursor, Read}; 

use anyhow::{anyhow, bail, Result};
use byteorder::{BigEndian, ReadBytesExt};

pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

/// Metadata structure for a data block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta into a buffer.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let mut estimated_size = std::mem::size_of::<u32>();
        for meta in block_meta {
            estimated_size += std::mem::size_of::<u32>(); // Offset size
            estimated_size += std::mem::size_of::<u16>(); // First key length size
            estimated_size += meta.first_key.len(); // First key data size
            estimated_size += std::mem::size_of::<u16>(); // Last key length size
            estimated_size += meta.last_key.len(); // Last key data size
        }
        estimated_size += std::mem::size_of::<u32>(); // Checksum size

        buf.reserve(estimated_size); // Reserve space for performance

        let original_len = buf.len();
        buf.put_u32(block_meta.len() as u32); // Number of entries

        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.len() as u16);
            buf.put_slice(meta.first_key.raw_ref());
            buf.put_u16(meta.last_key.len() as u16);
            buf.put_slice(meta.last_key.raw_ref());
        }

        // Calculate and append CRC32 checksum
        let checksum = crc32fast::hash(&buf[original_len + 4..]); // Exclude the checksum itself
        buf.put_u32(checksum);

        assert_eq!(estimated_size, buf.len() - original_len);
    }

    /// Decode block meta from a buffer.
    /// 
    pub fn decode_block_meta(mut buf: &[u8]) -> Result<Vec<BlockMeta>> {
        let mut block_meta = Vec::new();
        let num = buf.get_u32() as usize;
        let checksum = crc32fast::hash(&buf[..buf.remaining() - 4]);
        for _ in 0..num {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key = KeyBytes::from_bytes(buf.copy_to_bytes(first_key_len));
            let last_key_len: usize = buf.get_u16() as usize;
            let last_key = KeyBytes::from_bytes(buf.copy_to_bytes(last_key_len));
            block_meta.push(BlockMeta {
                offset,
                first_key,
                last_key,
            });
        }
        if buf.get_u32() != checksum {
            bail!("meta checksum mismatched");
        }

        Ok(block_meta)
    }
}

/// A file object wrapper.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    /// Read data from the file at a given offset and length.
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    /// Get the size of the file in bytes.
    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object and write data to the disk.
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    /// Open an existing file object.
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// Structure representing an SSTable.
pub struct SsTable {
    /// The underlying file object.
    pub(crate) file: FileObject,
    /// Metadata blocks containing information about data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// Offset indicating the start point of metadata blocks in `file`.
    pub(crate) block_meta_offset: usize,
    /// Identifier for the SSTable.
    id: usize,
    /// Optional block cache to improve read performance.
    block_cache: Option<Arc<BlockCache>>,
    /// First key present in the SSTable.
    first_key: KeyBytes,
    /// Last key present in the SSTable.
    last_key: KeyBytes,
    /// Optional Bloom filter for quick existence checks.
    pub(crate) bloom: Option<Bloom>,
    /// Maximum timestamp stored in the SSTable.
    max_ts: u64,
}

impl SsTable {
    /// Open an SSTable for testing purposes.
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open an SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let len = file.size(); // Get the length of the file
        println!("File length: {}", len);
        
    
        // Read the last 4 bytes of the file to get the bloom filter offset
        let raw_bloom_offset = file.read(len - 4, 4)?;
        let bloom_offset = (&raw_bloom_offset[..]).get_u32() as u64;
        println!("Raw bloom offset bytes: {:?}", raw_bloom_offset);
        println!("Bloom filter offset: {}", bloom_offset);
    
        // Ensure the bloom filter offset is within the file length range
        if bloom_offset >= len {
            bail!("Bloom filter offset is out of file length range. Offset: {}, File length: {}", bloom_offset, len);
        }
    
        // Calculate the length of the bloom filter data
        let bloom_filter_len = len - 4 - bloom_offset;
        if bloom_filter_len == 0 {
            bail!("Bloom filter length is zero"); // Bail out if the bloom filter length is zero
        }
    
        // Read the bloom filter data from the file
        let raw_bloom = file.read(bloom_offset, bloom_filter_len)?;
        let bloom_filter = Bloom::decode(&raw_bloom)?;
    
        // Read the 4 bytes preceding the bloom filter to get the block metadata offset
        let raw_meta_offset = file.read(bloom_offset - 4, 4)?;
        let block_meta_offset = (&raw_meta_offset[..]).get_u32() as u64;
        println!("Raw block meta offset bytes: {:?}", raw_meta_offset);
        println!("Block metadata offset: {}", block_meta_offset);
    
        // Ensure the block metadata offset is valid
        if block_meta_offset >= bloom_offset {
            bail!("Block metadata offset is invalid. Offset: {}, Bloom filter offset: {}", block_meta_offset, bloom_offset);
        }
    
        // Calculate the length of the block metadata
        let block_meta_len = bloom_offset - 4 - block_meta_offset;
        let raw_meta = file.read(block_meta_offset, block_meta_len)?;
        let block_meta = BlockMeta::decode_block_meta(&raw_meta)?;
    
        Ok(Self {
            file,
            first_key: block_meta.first().unwrap().first_key.clone(),
            last_key: block_meta.last().unwrap().last_key.clone(),
            block_meta,
            block_meta_offset: block_meta_offset as usize,
            id,
            block_cache,
            bloom: Some(bloom_filter),
            max_ts: 0,
        })
    }
    

    /// Create a mock SSTable with only first key and last key metadata.
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a data block from the disk.
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if block_idx >= self.block_meta.len() {
            bail!("block index out of bounds: {}", block_idx);
        }
    
        if let Some(ref block_cache) = self.block_cache {
            let blk = block_cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow!("{}", e))?;
            Ok(blk)
        } else {
            self.read_block(block_idx)
        }
    }
    
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        if block_idx >= self.block_meta.len() {
            bail!("block index out of bounds: {}", block_idx);
        }
    
        let offset = self.block_meta[block_idx].offset;
        let offset_end = self
            .block_meta
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |x| x.offset);
        let block_len = offset_end - offset - 4;
    
        let block_data_with_chksum: Vec<u8> = self
            .file
            .read(offset as u64, (offset_end - offset) as u64)?;
        let block_data = &block_data_with_chksum[..block_len];
        let checksum = (&block_data_with_chksum[block_len..]).get_u32();
    
        if checksum != crc32fast::hash(block_data) {
            bail!("block checksum mismatched");
        }
    
        Ok(Arc::new(Block::decode(block_data)))
    }

    /// Find the block index that may contain a given `key`.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        self.block_meta
            .partition_point(|meta| meta.first_key.as_key_slice() <= key)
            .saturating_sub(1)
    }

    /// Get the number of data blocks in the SSTable.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    /// Get the first key in the SSTable.
    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    /// Get the last key in the SSTable.
    pub fn last_key(&self) ->&KeyBytes {
    &self.last_key
    }

    /// Get the size of the SSTable in bytes.
    pub fn table_size(&self) -> u64 {
        self.file.size()
    }

    /// Get the ID of the SSTable.
    pub fn sst_id(&self) -> usize {
        self.id
    }

    /// Get the maximum timestamp stored in the SSTable.
    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
