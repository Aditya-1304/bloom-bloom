use xxhash_rust::xxh3::xxh3_128_with_seed;

const BLOCK_WORDS: usize = 8;
const WORD_BITS: usize = 64;
const BLOCK_BITS: usize = BLOCK_WORDS * WORD_BITS;
const BLOCK_BYTES: usize = BLOCK_WORDS * std::mem::size_of::<u64>();

const BLOOM_MAGIC: [u8; 8] = *b"BLMFILT1";
const BLOOM_VERSION: u32 = 1;
pub const BLOOM_HASH_SEED: u64 = 0xD6E8_FD9A_2C4B_1A37;
const BLOCK_INDEX_BITS: u32 = 9;
const BLOCK_MASK: u64 = (BLOCK_BITS as u64) - 1;
const HEADER_LEN: usize = 8 + 4 + 8 + 8 + 4;

/// Maximum number of hash probes supported by one filter.
pub const MAX_HASHES: u32 = 64;

/// Maximum serialized size of a filter, including its format header.
///
/// The 64 MiB cap bounds memory use for construction, decoding, and
/// serialization while still supporting filters for tens of millions of keys
/// at common false-positive targets. Applications may enforce a smaller limit.
pub const MAX_FILTER_BYTES: usize = 64 * 1024 * 1024;

/// Stable compatibility identity for the serialized Bloom encoding.
///
/// This descriptor identifies the format version and hash seed needed by
/// storage metadata. It intentionally does not expose the private block
/// layout, probing implementation, or caller-specific key transformation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BloomFormatDescriptor {
    /// Version of the serialized Bloom filter encoding.
    pub format_version: u32,
    /// Seed used by the stable key hashing contract.
    pub hash_seed: u64,
}

/// Returns the stable compatibility identity emitted in the V1 header.
pub const fn format_descriptor() -> BloomFormatDescriptor {
    BloomFormatDescriptor {
        format_version: BLOOM_VERSION,
        hash_seed: BLOOM_HASH_SEED,
    }
}

#[repr(align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Block {
    words: [u64; BLOCK_WORDS],
}

impl Block {
    #[inline(always)]
    fn empty() -> Self {
        Self {
            words: [0; BLOCK_WORDS],
        }
    }

    #[inline(always)]
    fn set(&mut self, bit_index: usize) -> bool {
        let word_index = bit_index >> 6;
        let bit_offset = bit_index & 63;
        let mask = 1u64 << bit_offset;

        let was_set = self.words[word_index] & mask != 0;
        self.words[word_index] |= mask;
        was_set
    }

    #[inline(always)]
    fn check(&self, bit_index: usize) -> bool {
        let word_index = bit_index >> 6;
        let bit_offset = bit_index & 63;
        let mask = 1u64 << bit_offset;

        self.words[word_index] & mask != 0
    }
}

/// Configuration used to size a Bloom filter for a known key count.
#[derive(Debug, Clone, Copy)]
pub struct BloomConfig {
    pub expected_items: usize,
    pub false_positive_rate: f64,
}

/// A typed failure encountered while validating or building a Bloom filter.
///
/// Use this error from fallible constructors and serialization APIs to
/// distinguish invalid configuration, resource limits, arithmetic overflow,
/// and allocation failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BloomBuildError {
    ZeroExpectedItems,
    InvalidFalsePositiveRate,
    InvalidNumBits,
    InvalidNumHashes,
    TooManyHashes {
        requested: u32,
        max: u32,
    },
    FilterTooLarge {
        requested_bytes: usize,
        max_bytes: usize,
    },
    SizeOverflow,
    AllocationFailed,
}

impl std::fmt::Display for BloomBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroExpectedItems => f.write_str("expected item count must be greater than zero"),
            Self::InvalidFalsePositiveRate => {
                f.write_str("false-positive rate must be finite and between zero and one")
            }
            Self::InvalidNumBits => f.write_str("Bloom filter bit count must be greater than zero"),
            Self::InvalidNumHashes => {
                f.write_str("Bloom filter hash count must be greater than zero")
            }
            Self::TooManyHashes { requested, max } => {
                write!(
                    f,
                    "Bloom filter requests {requested} hashes; maximum is {max}"
                )
            }
            Self::FilterTooLarge {
                requested_bytes,
                max_bytes,
            } => write!(
                f,
                "Bloom filter requires {requested_bytes} bytes; maximum is {max_bytes}"
            ),
            Self::SizeOverflow => f.write_str("Bloom filter size calculation overflowed"),
            Self::AllocationFailed => f.write_str("Bloom filter memory allocation failed"),
        }
    }
}

impl std::error::Error for BloomBuildError {}

#[derive(Debug, Clone, Copy)]
struct LookupPlan {
    block_index: usize,
    bit_hash: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BloomFilter {
    blocks: Vec<Block>,
    num_hashes: u32,
}

impl BloomFilter {
    /// Builds a filter from an explicit bit count and hash count.
    ///
    /// This convenience API panics for invalid or oversized input. Use
    /// [`BloomFilter::try_with_num_bits`] when values are untrusted or come
    /// from configuration.
    pub fn with_num_bits(num_bits: usize, num_hashes: u32) -> Self {
        Self::try_with_num_bits(num_bits, num_hashes)
            .expect("valid trusted Bloom filter sizing and available memory")
    }

    /// Fallibly builds a filter from an explicit bit count and hash count.
    pub fn try_with_num_bits(num_bits: usize, num_hashes: u32) -> Result<Self, BloomBuildError> {
        if num_bits == 0 {
            return Err(BloomBuildError::InvalidNumBits);
        }
        validate_num_hashes(num_hashes)?;

        let num_blocks = checked_div_ceil(num_bits, BLOCK_BITS)
            .ok_or(BloomBuildError::SizeOverflow)?
            .max(1);
        checked_filter_layout(num_blocks)?;

        allocate_filter(num_blocks, num_hashes)
    }

    /// Builds a filter sized for the requested expected item count and
    /// false-positive target.
    ///
    /// This convenience API panics for invalid or oversized input. Use
    /// [`BloomFilter::try_with_false_positive_rate`] for untrusted or
    /// operator-provided values.
    pub fn with_false_positive_rate(expected_items: usize, false_positive_rate: f64) -> Self {
        Self::try_with_false_positive_rate(expected_items, false_positive_rate)
            .expect("valid trusted Bloom filter configuration and available memory")
    }

    /// Fallibly builds a filter sized for an expected item count and target
    /// false-positive rate.
    pub fn try_with_false_positive_rate(
        expected_items: usize,
        false_positive_rate: f64,
    ) -> Result<Self, BloomBuildError> {
        validate_config(expected_items, false_positive_rate)?;

        let requested_bits = checked_optimal_num_bits(expected_items, false_positive_rate)?;
        let mut num_blocks = checked_div_ceil(requested_bits, BLOCK_BITS)
            .ok_or(BloomBuildError::SizeOverflow)?
            .max(1);

        loop {
            let (actual_bits, _, _) = checked_filter_layout(num_blocks)?;
            let num_hashes = checked_optimal_num_hashes(actual_bits, expected_items)?;
            validate_num_hashes(num_hashes)?;

            let expected_fp =
                expected_block_false_positive_rate(num_blocks, num_hashes, expected_items);

            if expected_fp <= false_positive_rate {
                return allocate_filter(num_blocks, num_hashes);
            }

            num_blocks = num_blocks
                .checked_add(1)
                .ok_or(BloomBuildError::SizeOverflow)?;
        }
    }

    /// Fallibly builds a filter from a configuration object.
    pub fn try_from_config(config: BloomConfig) -> Result<Self, BloomBuildError> {
        Self::try_with_false_positive_rate(config.expected_items, config.false_positive_rate)
    }

    pub fn num_blocks(&self) -> usize {
        self.blocks.len()
    }

    pub fn num_bits(&self) -> usize {
        self.blocks.len() * BLOCK_BITS
    }

    pub fn num_hashes(&self) -> u32 {
        self.num_hashes
    }

    pub fn expected_density(&self, inserted_items: usize) -> f64 {
        expected_density(self.num_bits(), self.num_hashes(), inserted_items)
    }

    pub fn expected_false_positive_rate(&self, inserted_items: usize) -> f64 {
        expected_block_false_positive_rate(self.num_blocks(), self.num_hashes(), inserted_items)
    }

    pub fn insert_key(&mut self, key: &[u8]) -> bool {
        let hash = xxh3_128_with_seed(key, BLOOM_HASH_SEED);

        let block_hash = hash as u64;
        let mut bit_hash = (hash >> 64) as u64;

        let block_index = index(self.num_blocks(), block_hash);
        let block = &mut self.blocks[block_index];

        if self.num_hashes == 7 {
            let [b0, b1, b2, b3, b4, b5, b6] = block_bit_indexes_7(bit_hash);

            let mut all_set = block.set(b0);
            all_set &= block.set(b1);
            all_set &= block.set(b2);
            all_set &= block.set(b3);
            all_set &= block.set(b4);
            all_set &= block.set(b5);
            all_set &= block.set(b6);

            return all_set;
        }

        let mut previously_contained = true;

        if self.num_hashes < 7 {
            for _ in 0..self.num_hashes {
                let bit_index = take_block_bit_index(&mut bit_hash);
                previously_contained &= block.set(bit_index);
            }

            return previously_contained;
        }

        for bit_index in block_bit_indexes(bit_hash, self.num_hashes) {
            previously_contained &= block.set(bit_index);
        }

        previously_contained
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        let hash = xxh3_128_with_seed(key, BLOOM_HASH_SEED);

        let block_hash = hash as u64;
        let mut bit_hash = (hash >> 64) as u64;

        let block_index = index(self.num_blocks(), block_hash);
        let block = &self.blocks[block_index];

        if self.num_hashes == 7 {
            let [b0, b1, b2, b3, b4, b5, b6] = block_bit_indexes_7(bit_hash);

            if !block.check(b0) {
                return false;
            }
            if !block.check(b1) {
                return false;
            }
            if !block.check(b2) {
                return false;
            }
            if !block.check(b3) {
                return false;
            }
            if !block.check(b4) {
                return false;
            }
            if !block.check(b5) {
                return false;
            }
            if !block.check(b6) {
                return false;
            }

            return true;
        }

        if self.num_hashes < 7 {
            for _ in 0..self.num_hashes {
                let bit_index = take_block_bit_index(&mut bit_hash);

                if !block.check(bit_index) {
                    return false;
                }
            }

            return true;
        }

        for bit_index in block_bit_indexes(bit_hash, self.num_hashes) {
            if !block.check(bit_index) {
                return false;
            }
        }

        true
    }

    pub fn insert_str(&mut self, key: &str) -> bool {
        self.insert_key(key.as_bytes())
    }

    pub fn contains_str(&self, key: &str) -> bool {
        self.contains_key(key.as_bytes())
    }

    /// Serializes this filter using the V1 format.
    ///
    /// This convenience API panics only if the filter violates the crate's
    /// internal size invariant or memory allocation fails. Use
    /// [`BloomFilter::try_to_bytes`] in storage paths that must handle those
    /// failures explicitly.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.try_to_bytes()
            .expect("valid Bloom filter size and available serialization memory")
    }

    /// Fallibly serializes this filter using the V1 format.
    pub fn try_to_bytes(&self) -> Result<Vec<u8>, BloomBuildError> {
        let (_, _, serialized_len) = checked_filter_layout(self.num_blocks())?;
        let num_blocks =
            u64::try_from(self.num_blocks()).map_err(|_| BloomBuildError::SizeOverflow)?;
        let descriptor = format_descriptor();

        let mut out = Vec::new();
        out.try_reserve_exact(serialized_len)
            .map_err(|_| BloomBuildError::AllocationFailed)?;

        out.extend_from_slice(&BLOOM_MAGIC);
        out.extend_from_slice(&descriptor.format_version.to_le_bytes());
        out.extend_from_slice(&descriptor.hash_seed.to_le_bytes());
        out.extend_from_slice(&num_blocks.to_le_bytes());
        out.extend_from_slice(&self.num_hashes.to_le_bytes());

        for block in &self.blocks {
            for word in block.words {
                out.extend_from_slice(&word.to_le_bytes());
            }
        }

        Ok(out)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, BloomDecodeError> {
        let header_len = HEADER_LEN;
        let descriptor = format_descriptor();

        if bytes.len() > MAX_FILTER_BYTES {
            return Err(BloomDecodeError::FilterTooLarge {
                requested_bytes: bytes.len(),
                max_bytes: MAX_FILTER_BYTES,
            });
        }

        if bytes.len() < header_len {
            return Err(BloomDecodeError::TooShort);
        }

        if bytes[0..8] != BLOOM_MAGIC {
            return Err(BloomDecodeError::BadMagic);
        }

        let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        if version != descriptor.format_version {
            return Err(BloomDecodeError::UnsupportedVersion(version));
        }

        let seed = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
        if seed != descriptor.hash_seed {
            return Err(BloomDecodeError::WrongHashSeed(seed));
        }

        let num_blocks_u64 = u64::from_le_bytes(bytes[20..28].try_into().unwrap());

        let num_blocks =
            usize::try_from(num_blocks_u64).map_err(|_| BloomDecodeError::LengthOverflow)?;

        let num_hashes = u32::from_le_bytes(bytes[28..32].try_into().unwrap());

        if num_blocks == 0 {
            return Err(BloomDecodeError::InvalidNumBlocks);
        }

        if num_hashes == 0 || num_hashes > MAX_HASHES {
            return Err(BloomDecodeError::InvalidNumHashes);
        }

        let payload_len = num_blocks
            .checked_mul(BLOCK_BYTES)
            .ok_or(BloomDecodeError::LengthOverflow)?;

        let expected_len = HEADER_LEN
            .checked_add(payload_len)
            .ok_or(BloomDecodeError::LengthOverflow)?;

        if expected_len > MAX_FILTER_BYTES {
            return Err(BloomDecodeError::FilterTooLarge {
                requested_bytes: expected_len,
                max_bytes: MAX_FILTER_BYTES,
            });
        }

        if bytes.len() != expected_len {
            return Err(BloomDecodeError::LengthMismatch);
        }

        let mut blocks = Vec::new();
        blocks
            .try_reserve_exact(num_blocks)
            .map_err(|_| BloomDecodeError::AllocationFailed)?;
        let mut offset = header_len;

        for _ in 0..num_blocks {
            let mut words = [0u64; BLOCK_WORDS];

            for word in &mut words {
                *word = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
                offset += 8;
            }

            blocks.push(Block { words });
        }

        Ok(Self { blocks, num_hashes })
    }

    /// Builds from a configuration object using the trusted-input convenience
    /// constructor. Use [`BloomFilter::try_from_config`] for external input.
    pub fn from_config(config: BloomConfig) -> Self {
        Self::with_false_positive_rate(config.expected_items, config.false_positive_rate)
    }

    pub fn may_contain_key(&self, key: &[u8]) -> bool {
        self.contains_key(key)
    }

    pub fn may_contain_str(&self, key: &str) -> bool {
        self.contains_str(key)
    }

    pub fn clear(&mut self) {
        for block in &mut self.blocks {
            block.words.fill(0);
        }
    }

    pub fn byte_len(&self) -> usize {
        self.num_blocks() * BLOCK_WORDS * 8
    }

    pub fn serialized_len(&self) -> usize {
        HEADER_LEN + self.byte_len()
    }

    pub fn count_may_contain<T: AsRef<[u8]>>(&self, keys: &[T]) -> usize {
        keys.iter()
            .filter(|key| self.may_contain_key(key.as_ref()))
            .count()
    }

    #[inline(always)]
    fn lookup_plan(&self, key: &[u8]) -> LookupPlan {
        let hash = xxh3_128_with_seed(key, BLOOM_HASH_SEED);

        LookupPlan {
            block_index: index(self.num_blocks(), hash as u64),
            bit_hash: (hash >> 64) as u64,
        }
    }

    #[inline(always)]
    fn prefetch_block(&self, block_index: usize) {
        #[cfg(all(feature = "prefetch", target_arch = "x86_64"))]
        unsafe {
            use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};

            let ptr = self.blocks.as_ptr().add(block_index) as *const i8;
            _mm_prefetch(ptr, _MM_HINT_T0);
        }

        #[cfg(not(all(feature = "prefetch", target_arch = "x86_64")))]
        {
            let _ = block_index;
        }
    }

    #[inline(always)]
    fn contains_planned(&self, plan: LookupPlan) -> bool {
        let block = &self.blocks[plan.block_index];

        if self.num_hashes == 7 {
            let [b0, b1, b2, b3, b4, b5, b6] = block_bit_indexes_7(plan.bit_hash);

            return block.check(b0)
                && block.check(b1)
                && block.check(b2)
                && block.check(b3)
                && block.check(b4)
                && block.check(b5)
                && block.check(b6);
        }

        if self.num_hashes < 7 {
            let mut bit_hash = plan.bit_hash;

            for _ in 0..self.num_hashes {
                if !block.check(take_block_bit_index(&mut bit_hash)) {
                    return false;
                }
            }

            return true;
        }

        for bit_index in block_bit_indexes(plan.bit_hash, self.num_hashes) {
            if !block.check(bit_index) {
                return false;
            }
        }

        true
    }

    #[inline(always)]
    fn contains_planned_branchless(&self, plan: LookupPlan) -> bool {
        let block = &self.blocks[plan.block_index];

        if self.num_hashes == 7 {
            return check7_branchless(block, plan.bit_hash);
        }

        self.contains_planned(plan)
    }

    pub fn count_may_contain_keys_prefetch(&self, keys: &[&[u8]]) -> usize {
        const BATCH_SIZE: usize = 32;

        let mut count = 0;

        for chunk in keys.chunks(BATCH_SIZE) {
            let mut plans = [LookupPlan {
                block_index: 0,
                bit_hash: 0,
            }; BATCH_SIZE];

            for (i, key) in chunk.iter().enumerate() {
                let plan = self.lookup_plan(*key);
                self.prefetch_block(plan.block_index);
                plans[i] = plan;
            }

            for plan in plans.iter().take(chunk.len()) {
                count += self.contains_planned(*plan) as usize;
            }
        }

        count
    }

    pub fn count_may_contain_keys_prefetch_branchless(&self, keys: &[&[u8]]) -> usize {
        const BATCH_SIZE: usize = 32;

        let mut count = 0;

        for chunk in keys.chunks(BATCH_SIZE) {
            let mut plans = [LookupPlan {
                block_index: 0,
                bit_hash: 0,
            }; BATCH_SIZE];

            for (i, key) in chunk.iter().enumerate() {
                let plan = self.lookup_plan(*key);
                self.prefetch_block(plan.block_index);
                plans[i] = plan;
            }

            for plan in plans.iter().take(chunk.len()) {
                count += self.contains_planned_branchless(*plan) as usize;
            }
        }

        count
    }
}

fn validate_config(expected_items: usize, false_positive_rate: f64) -> Result<(), BloomBuildError> {
    if expected_items == 0 {
        return Err(BloomBuildError::ZeroExpectedItems);
    }

    if !false_positive_rate.is_finite() || false_positive_rate <= 0.0 || false_positive_rate >= 1.0
    {
        return Err(BloomBuildError::InvalidFalsePositiveRate);
    }

    Ok(())
}

fn validate_num_hashes(num_hashes: u32) -> Result<(), BloomBuildError> {
    if num_hashes == 0 {
        return Err(BloomBuildError::InvalidNumHashes);
    }

    if num_hashes > MAX_HASHES {
        return Err(BloomBuildError::TooManyHashes {
            requested: num_hashes,
            max: MAX_HASHES,
        });
    }

    Ok(())
}

fn checked_div_ceil(value: usize, divisor: usize) -> Option<usize> {
    if divisor == 0 {
        return None;
    }

    let quotient = value / divisor;
    let has_remainder = usize::from(value % divisor != 0);
    quotient.checked_add(has_remainder)
}

fn checked_round_up(value: usize, alignment: usize) -> Option<usize> {
    if alignment == 0 {
        return None;
    }

    let remainder = value % alignment;
    if remainder == 0 {
        Some(value)
    } else {
        value.checked_add(alignment - remainder)
    }
}

fn checked_optimal_num_bits(
    expected_items: usize,
    false_positive_rate: f64,
) -> Result<usize, BloomBuildError> {
    validate_config(expected_items, false_positive_rate)?;

    let items = expected_items as f64;
    let ln_2 = std::f64::consts::LN_2;
    let raw_bits = -(items * false_positive_rate.ln()) / (ln_2 * ln_2);
    let rounded_bits = raw_bits.ceil();

    // `usize::MAX as f64` may round upward on 64-bit targets. Rejecting that
    // boundary conservatively avoids a saturating float-to-integer cast.
    if !rounded_bits.is_finite() || rounded_bits <= 0.0 || rounded_bits >= usize::MAX as f64 {
        return Err(BloomBuildError::SizeOverflow);
    }

    let bits = rounded_bits as usize;
    checked_round_up(bits, WORD_BITS).ok_or(BloomBuildError::SizeOverflow)
}

fn checked_optimal_num_hashes(
    num_bits: usize,
    expected_items: usize,
) -> Result<u32, BloomBuildError> {
    if num_bits == 0 {
        return Err(BloomBuildError::InvalidNumBits);
    }
    if expected_items == 0 {
        return Err(BloomBuildError::ZeroExpectedItems);
    }

    let raw_hashes = (num_bits as f64 / expected_items as f64) * std::f64::consts::LN_2;
    let rounded_hashes = raw_hashes.round();
    if !rounded_hashes.is_finite() || rounded_hashes > u32::MAX as f64 {
        return Err(BloomBuildError::SizeOverflow);
    }

    Ok((rounded_hashes as u32).max(1))
}

fn checked_filter_layout(num_blocks: usize) -> Result<(usize, usize, usize), BloomBuildError> {
    if num_blocks == 0 {
        return Err(BloomBuildError::InvalidNumBits);
    }

    let actual_bits = num_blocks
        .checked_mul(BLOCK_BITS)
        .ok_or(BloomBuildError::SizeOverflow)?;
    let payload_bytes = num_blocks
        .checked_mul(BLOCK_BYTES)
        .ok_or(BloomBuildError::SizeOverflow)?;
    let serialized_bytes = HEADER_LEN
        .checked_add(payload_bytes)
        .ok_or(BloomBuildError::SizeOverflow)?;

    if serialized_bytes > MAX_FILTER_BYTES {
        return Err(BloomBuildError::FilterTooLarge {
            requested_bytes: serialized_bytes,
            max_bytes: MAX_FILTER_BYTES,
        });
    }

    Ok((actual_bits, payload_bytes, serialized_bytes))
}

fn allocate_filter(num_blocks: usize, num_hashes: u32) -> Result<BloomFilter, BloomBuildError> {
    let (_, _, _) = checked_filter_layout(num_blocks)?;
    validate_num_hashes(num_hashes)?;

    let mut blocks = Vec::new();
    blocks
        .try_reserve_exact(num_blocks)
        .map_err(|_| BloomBuildError::AllocationFailed)?;
    blocks.resize(num_blocks, Block::empty());

    Ok(BloomFilter { blocks, num_hashes })
}

fn block_bit_indexes(bit_hash: u64, num_hashes: u32) -> impl Iterator<Item = usize> {
    let mut state = bit_hash;
    let mut pool = state;
    let mut chunks_left = 7u32;

    (0..num_hashes).map(move |_| {
        if chunks_left == 0 {
            state = mix64(state);
            pool = state;
            chunks_left = 7;
        }

        let bit_index = (pool & BLOCK_MASK) as usize;

        pool >>= BLOCK_INDEX_BITS;
        chunks_left -= 1;

        bit_index
    })
}

#[inline(always)]
fn take_block_bit_index(bit_hash: &mut u64) -> usize {
    let bit_index = (*bit_hash & BLOCK_MASK) as usize;
    *bit_hash >>= BLOCK_INDEX_BITS;
    bit_index
}

#[inline(always)]
fn block_bit_indexes_7(bit_hash: u64) -> [usize; 7] {
    [
        (bit_hash & BLOCK_MASK) as usize,
        ((bit_hash >> 9) & BLOCK_MASK) as usize,
        ((bit_hash >> 18) & BLOCK_MASK) as usize,
        ((bit_hash >> 27) & BLOCK_MASK) as usize,
        ((bit_hash >> 36) & BLOCK_MASK) as usize,
        ((bit_hash >> 45) & BLOCK_MASK) as usize,
        ((bit_hash >> 54) & BLOCK_MASK) as usize,
    ]
}

#[inline(always)]
fn check7_branchless(block: &Block, bit_hash: u64) -> bool {
    let [b0, b1, b2, b3, b4, b5, b6] = block_bit_indexes_7(bit_hash);

    let c0 = block.check(b0) as u8;
    let c1 = block.check(b1) as u8;
    let c2 = block.check(b2) as u8;
    let c3 = block.check(b3) as u8;
    let c4 = block.check(b4) as u8;
    let c5 = block.check(b5) as u8;
    let c6 = block.check(b6) as u8;

    (c0 & c1 & c2 & c3 & c4 & c5 & c6) != 0
}

fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^= x >> 31;
    x
}

/// Returns the conventional Bloom bit-count estimate for trusted inputs.
///
/// # Panics
///
/// Panics if `expected_items` is zero, the false-positive rate is invalid, or
/// the resulting size cannot be represented. Use the fallible filter
/// constructors for configuration and operator-provided values.
pub fn optimal_num_bits(expected_items: usize, false_positive_rate: f64) -> usize {
    checked_optimal_num_bits(expected_items, false_positive_rate)
        .expect("valid trusted Bloom sizing inputs and representable bit count")
}

/// Returns the conventional Bloom hash-count estimate for trusted inputs.
///
/// # Panics
///
/// Panics if either input is zero or the calculated count cannot be
/// represented as `u32`. The returned mathematical optimum may exceed
/// [`MAX_HASHES`], in which case a filter constructor rejects it.
pub fn optimal_num_hashes(num_bits: usize, expected_items: usize) -> u32 {
    checked_optimal_num_hashes(num_bits, expected_items)
        .expect("valid trusted Bloom sizing inputs and representable hash count")
}

#[inline(always)]
fn index(num_bits: usize, hash: u64) -> usize {
    assert!(num_bits > 0, "num_bits must be greater than 0");

    ((hash as u128 * num_bits as u128) >> 64) as usize
}

/// Estimates bit density for trusted, nonzero filter sizing inputs.
///
/// # Panics
///
/// Panics if `num_bits` or `num_hashes` is zero.
pub fn expected_density(num_bits: usize, num_hashes: u32, inserted_items: usize) -> f64 {
    assert!(num_bits > 0, "num_bits must be greater than 0");
    assert!(num_hashes > 0, "num_hashes must be greater than 0");

    let m = num_bits as f64;
    let k = num_hashes as f64;
    let n = inserted_items as f64;

    1.0 - (-(k * n) / m).exp()
}

/// Estimates the conventional Bloom false-positive rate for trusted inputs.
///
/// # Panics
///
/// Panics if `num_bits` or `num_hashes` is zero, or if the hash count exceeds
/// [`MAX_HASHES`].
pub fn expected_false_positive_rate(
    num_bits: usize,
    num_hashes: u32,
    inserted_items: usize,
) -> f64 {
    assert!(
        num_hashes <= MAX_HASHES,
        "Bloom filter hash count is too large"
    );

    let density = expected_density(num_bits, num_hashes, inserted_items);

    density.powi(num_hashes as i32)
}

/// Estimates the block-filter false-positive rate for trusted sizing inputs.
///
/// # Panics
///
/// Panics if `num_blocks` or `num_hashes` is zero, or if the hash count is
/// greater than [`MAX_HASHES`].
pub fn expected_block_false_positive_rate(
    num_blocks: usize,
    num_hashes: u32,
    inserted_items: usize,
) -> f64 {
    assert!(num_blocks > 0, "num_blocks must be greater than 0");
    assert!(num_hashes > 0, "num_hashes must be greater than 0");
    assert!(
        num_hashes <= MAX_HASHES,
        "Bloom filter hash count is too large"
    );

    let lambda = inserted_items as f64 / num_blocks as f64;
    let hashes = num_hashes as usize;

    let miss_one_bit_per_item = (1.0 - 1.0 / BLOCK_BITS as f64).powi(num_hashes as i32);

    let mut fp = 0.0;

    for j in 0..=hashes {
        let sign = if j % 2 == 0 { 1.0 } else { -1.0 };
        let term =
            binomial(hashes, j) * (lambda * (miss_one_bit_per_item.powi(j as i32) - 1.0)).exp();

        fp += sign * term;
    }

    fp.clamp(0.0, 1.0)
}

fn binomial(n: usize, k: usize) -> f64 {
    if k > n {
        return 0.0;
    }

    let k = k.min(n - k);
    let mut result = 1.0;

    for i in 0..k {
        result *= (n - i) as f64;
        result /= (i + 1) as f64;
    }

    result
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// A typed failure encountered while decoding serialized Bloom filter bytes.
pub enum BloomDecodeError {
    TooShort,
    BadMagic,
    UnsupportedVersion(u32),
    WrongHashSeed(u64),
    InvalidNumBlocks,
    InvalidNumHashes,
    LengthMismatch,
    LengthOverflow,
    FilterTooLarge {
        requested_bytes: usize,
        max_bytes: usize,
    },
    AllocationFailed,
}

impl std::fmt::Display for BloomDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooShort => f.write_str("serialized Bloom filter is shorter than its header"),
            Self::BadMagic => f.write_str("serialized Bloom filter has invalid magic bytes"),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported Bloom filter format version {version}")
            }
            Self::WrongHashSeed(seed) => {
                write!(
                    f,
                    "serialized Bloom filter uses unsupported hash seed {seed}"
                )
            }
            Self::InvalidNumBlocks => {
                f.write_str("serialized Bloom filter has an invalid block count")
            }
            Self::InvalidNumHashes => {
                f.write_str("serialized Bloom filter has an invalid hash count")
            }
            Self::LengthMismatch => {
                f.write_str("serialized Bloom filter length does not match its header")
            }
            Self::LengthOverflow => {
                f.write_str("serialized Bloom filter length calculation overflowed")
            }
            Self::FilterTooLarge {
                requested_bytes,
                max_bytes,
            } => write!(
                f,
                "serialized Bloom filter requires {requested_bytes} bytes; maximum is {max_bytes}"
            ),
            Self::AllocationFailed => {
                f.write_str("memory allocation failed while decoding Bloom filter")
            }
        }
    }
}

impl std::error::Error for BloomDecodeError {}

#[cfg(test)]
mod tests {
    use super::*;

    // Frozen V1 bytes for a one-block filter containing `alpha` and `beta`.
    // Keep this fixture independent from runtime serialization so encoder and
    // decoder changes cannot silently redefine the compatibility baseline.
    const V1_GOLDEN_BYTES: [u8; 96] = [
        // Magic, version, hash seed, block count, and hash count.
        b'B', b'L', b'M', b'F', b'I', b'L', b'T', b'1', 1, 0, 0, 0, 0x37, 0x1a, 0x4b, 0x2c, 0x9a,
        0xfd, 0xe8, 0xd6, 1, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0,
        // Eight little-endian words in the V1 block payload.
        0, 0, 0, 0, 0, 0, 0, 0, // word 0
        0, 0, 0, 0, 32, 0, 0, 0, // word 1
        0, 0, 0, 0, 0, 0, 0, 0, // word 2
        0, 0, 0, 0, 0, 64, 32, 0, // word 3
        0, 0, 0, 0, 0, 0, 0, 0, // word 4
        0, 0, 8, 0, 0, 0, 0, 0, // word 5
        0, 0, 0, 0, 0, 0, 0, 0, // word 6
        0, 0, 32, 0, 0, 0, 1, 0, // word 7
    ];

    #[test]
    fn block_filter_rounds_up_to_full_blocks() {
        let one_bit = BloomFilter::with_num_bits(1, 3);
        let one_block_plus_one_bit = BloomFilter::with_num_bits(513, 3);

        assert_eq!(one_bit.num_blocks(), 1);
        assert_eq!(one_bit.num_bits(), 512);

        assert_eq!(one_block_plus_one_bit.num_blocks(), 2);
        assert_eq!(one_block_plus_one_bit.num_bits(), 1024);
    }

    #[test]
    fn empty_bloom_filter_contains_nothing() {
        let filter = BloomFilter::with_num_bits(1024, 3);

        assert!(!filter.contains_key(b"hello"));
        assert!(!filter.contains_key(b"rust"));
        assert!(!filter.contains_key(&12345u64.to_be_bytes()));
    }

    #[test]
    fn inserted_key_is_contained() {
        let mut filter = BloomFilter::with_num_bits(1024, 3);

        filter.insert_key(b"hello");

        assert!(filter.contains_key(b"hello"));
    }

    #[test]
    fn string_helpers_use_key_bytes() {
        let mut filter = BloomFilter::with_num_bits(1024, 3);

        filter.insert_str("rust");

        assert!(filter.contains_str("rust"));
        assert!(!filter.contains_str("zig"));
    }

    #[test]
    fn many_inserted_keys_are_contained() {
        let mut filter = BloomFilter::with_num_bits(10_000, 4);

        for value in 0..1000u64 {
            filter.insert_key(&value.to_be_bytes());
        }

        for value in 0..1000u64 {
            assert!(filter.contains_key(&value.to_be_bytes()));
        }
    }

    #[test]
    fn insert_reports_whether_all_bits_were_already_set() {
        let mut filter = BloomFilter::with_num_bits(1024, 3);

        assert!(!filter.insert_key(b"hello"));
        assert!(filter.insert_key(b"hello"));
    }

    #[test]
    fn lower_false_positive_rate_needs_more_bits() {
        let loose = optimal_num_bits(1000, 0.1);
        let strict = optimal_num_bits(1000, 0.001);

        assert!(strict > loose);
    }

    #[test]
    fn more_expected_items_need_more_bits() {
        let small = optimal_num_bits(100, 0.01);
        let large = optimal_num_bits(10_000, 0.01);

        assert!(large > small);
    }

    #[test]
    fn optimal_hashes_never_returns_zero() {
        let hashes = optimal_num_hashes(64, 1_000_000);

        assert!(hashes >= 1);
    }

    #[test]
    fn can_build_from_false_positive_rate() {
        let mut filter = BloomFilter::with_false_positive_rate(1000, 0.01);

        filter.insert_key(b"rust");

        assert!(filter.contains_key(b"rust"));
        assert!(filter.num_bits() >= 512);
        assert!(filter.num_hashes() >= 1);
    }

    #[test]
    fn block_bit_indexes_are_inside_one_block() {
        for bit_index in block_bit_indexes(123456789, 20) {
            assert!(bit_index < BLOCK_BITS);
        }
    }

    #[test]
    fn index_is_always_in_bounds() {
        let num_slots = 1000;

        for i in 0..100_000u64 {
            let hash = i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            let slot = index(num_slots, hash);

            assert!(slot < num_slots);
        }
    }

    #[test]
    fn index_handles_single_slot() {
        assert_eq!(index(1, 0), 0);
        assert_eq!(index(1, u64::MAX), 0);
        assert_eq!(index(1, 123456789), 0);
    }

    #[test]
    fn expected_density_starts_at_zero() {
        let density = expected_density(1024, 3, 0);

        assert_eq!(density, 0.0);
    }

    #[test]
    fn expected_density_increases_with_items() {
        let small = expected_density(10_000, 7, 100);
        let large = expected_density(10_000, 7, 1000);

        assert!(large > small);
    }

    #[test]
    fn expected_false_positive_rate_is_near_target() {
        let expected_items = 1000;
        let target = 0.01;

        let filter = BloomFilter::with_false_positive_rate(expected_items, target);
        let estimated = filter.expected_false_positive_rate(expected_items);

        assert!(estimated < 0.012);
    }

    #[test]
    fn measured_false_positive_rate_is_reasonable() {
        let mut filter = BloomFilter::with_false_positive_rate(1000, 0.01);

        for value in 0..1000u64 {
            filter.insert_key(&value.to_be_bytes());
        }

        let mut false_positives = 0;
        let trials = 10_000u64;

        for value in 10_000..(10_000 + trials) {
            if filter.contains_key(&value.to_be_bytes()) {
                false_positives += 1;
            }
        }

        let measured_rate = false_positives as f64 / trials as f64;

        assert!(measured_rate < 0.05);
    }

    #[test]
    fn serialization_round_trip_preserves_inserted_keys() {
        let mut filter = BloomFilter::with_false_positive_rate(1000, 0.01);

        for value in 0..1000u64 {
            filter.insert_key(&value.to_be_bytes());
        }

        let bytes = filter.to_bytes();
        let decoded = BloomFilter::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.num_bits(), filter.num_bits());
        assert_eq!(decoded.num_blocks(), filter.num_blocks());
        assert_eq!(decoded.num_hashes(), filter.num_hashes());

        for value in 0..1000u64 {
            assert!(decoded.contains_key(&value.to_be_bytes()));
        }
    }

    #[test]
    fn serialization_preserves_string_keys() {
        let mut filter = BloomFilter::with_num_bits(2048, 4);

        filter.insert_str("alpha");
        filter.insert_str("beta");
        filter.insert_str("gamma");

        let bytes = filter.to_bytes();
        let decoded = BloomFilter::from_bytes(&bytes).unwrap();

        assert!(decoded.contains_str("alpha"));
        assert!(decoded.contains_str("beta"));
        assert!(decoded.contains_str("gamma"));
        assert!(!decoded.contains_str("definitely-not-inserted"));
    }

    #[test]
    fn decode_rejects_too_short_input() {
        let err = BloomFilter::from_bytes(&[]).unwrap_err();

        assert_eq!(err, BloomDecodeError::TooShort);
    }

    #[test]
    fn decode_rejects_bad_magic() {
        let filter = BloomFilter::with_num_bits(1024, 3);
        let mut bytes = filter.to_bytes();

        bytes[0] = b'X';

        let err = BloomFilter::from_bytes(&bytes).unwrap_err();

        assert_eq!(err, BloomDecodeError::BadMagic);
    }

    #[test]
    fn decode_rejects_unsupported_version() {
        let filter = BloomFilter::with_num_bits(1024, 3);
        let mut bytes = filter.to_bytes();

        bytes[8..12].copy_from_slice(&999u32.to_le_bytes());

        let err = BloomFilter::from_bytes(&bytes).unwrap_err();

        assert_eq!(err, BloomDecodeError::UnsupportedVersion(999));
    }

    #[test]
    fn decode_rejects_wrong_hash_seed() {
        let filter = BloomFilter::with_num_bits(1024, 3);
        let mut bytes = filter.to_bytes();

        bytes[12..20].copy_from_slice(&123u64.to_le_bytes());

        let err = BloomFilter::from_bytes(&bytes).unwrap_err();

        assert_eq!(err, BloomDecodeError::WrongHashSeed(123));
    }

    #[test]
    fn decode_rejects_zero_blocks() {
        let filter = BloomFilter::with_num_bits(1024, 3);
        let mut bytes = filter.to_bytes();

        bytes[20..28].copy_from_slice(&0u64.to_le_bytes());

        let err = BloomFilter::from_bytes(&bytes).unwrap_err();

        assert_eq!(err, BloomDecodeError::InvalidNumBlocks);
    }

    #[test]
    fn decode_rejects_zero_hashes() {
        let filter = BloomFilter::with_num_bits(1024, 3);
        let mut bytes = filter.to_bytes();

        bytes[28..32].copy_from_slice(&0u32.to_le_bytes());

        let err = BloomFilter::from_bytes(&bytes).unwrap_err();

        assert_eq!(err, BloomDecodeError::InvalidNumHashes);
    }

    #[test]
    fn decode_rejects_length_mismatch() {
        let filter = BloomFilter::with_num_bits(1024, 3);
        let mut bytes = filter.to_bytes();

        bytes.pop();

        let err = BloomFilter::from_bytes(&bytes).unwrap_err();

        assert_eq!(err, BloomDecodeError::LengthMismatch);
    }

    #[test]
    fn fallible_config_rejects_invalid_item_counts_and_false_positive_rates() {
        assert_eq!(
            BloomFilter::try_from_config(BloomConfig {
                expected_items: 0,
                false_positive_rate: 0.01,
            }),
            Err(BloomBuildError::ZeroExpectedItems)
        );

        for false_positive_rate in [
            0.0,
            1.0,
            -0.01,
            1.01,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ] {
            assert_eq!(
                BloomFilter::try_from_config(BloomConfig {
                    expected_items: 100,
                    false_positive_rate,
                }),
                Err(BloomBuildError::InvalidFalsePositiveRate)
            );
        }
    }

    #[test]
    fn fallible_bit_builder_rejects_invalid_hash_counts_and_oversized_filters() {
        assert_eq!(
            BloomFilter::try_with_num_bits(0, 1),
            Err(BloomBuildError::InvalidNumBits)
        );
        assert_eq!(
            BloomFilter::try_with_num_bits(512, 0),
            Err(BloomBuildError::InvalidNumHashes)
        );
        assert_eq!(
            BloomFilter::try_with_num_bits(512, MAX_HASHES + 1),
            Err(BloomBuildError::TooManyHashes {
                requested: MAX_HASHES + 1,
                max: MAX_HASHES,
            })
        );

        let oversized_bits = MAX_FILTER_BYTES * 8;
        assert!(matches!(
            BloomFilter::try_with_num_bits(oversized_bits, 1),
            Err(BloomBuildError::FilterTooLarge { .. })
        ));
    }

    #[test]
    fn fallible_sizing_rejects_unrepresentable_requests() {
        let result = BloomFilter::try_with_false_positive_rate(usize::MAX, f64::from_bits(1));

        assert_eq!(result, Err(BloomBuildError::SizeOverflow));
    }

    #[test]
    fn fallible_config_builds_a_filter_that_survives_fallible_serialization() {
        let mut filter = BloomFilter::try_from_config(BloomConfig {
            expected_items: 128,
            false_positive_rate: 0.01,
        })
        .unwrap();
        filter.insert_key(b"configured-storage-key");

        let bytes = filter.try_to_bytes().unwrap();
        let decoded = BloomFilter::from_bytes(&bytes).unwrap();

        assert!(decoded.may_contain_key(b"configured-storage-key"));
    }

    #[test]
    fn fallible_serialization_matches_the_existing_v1_bytes() {
        let mut filter = BloomFilter::with_num_bits(1024, 3);
        filter.insert_key(b"slice-one-serialization");

        assert_eq!(filter.try_to_bytes().unwrap(), filter.to_bytes());
    }

    #[test]
    fn decode_rejects_a_declared_filter_over_the_hard_limit() {
        let mut bytes = vec![0; HEADER_LEN];
        bytes[0..8].copy_from_slice(&BLOOM_MAGIC);
        bytes[8..12].copy_from_slice(&BLOOM_VERSION.to_le_bytes());
        bytes[12..20].copy_from_slice(&BLOOM_HASH_SEED.to_le_bytes());

        let max_blocks = (MAX_FILTER_BYTES - HEADER_LEN) / (BLOCK_WORDS * 8);
        let declared_blocks = max_blocks + 1;
        bytes[20..28].copy_from_slice(&(declared_blocks as u64).to_le_bytes());
        bytes[28..32].copy_from_slice(&1u32.to_le_bytes());

        assert!(matches!(
            BloomFilter::from_bytes(&bytes),
            Err(BloomDecodeError::FilterTooLarge { .. })
        ));
    }

    #[test]
    fn public_build_and_decode_errors_implement_standard_error_traits() {
        fn assert_error<E: std::error::Error + Send + Sync + 'static>() {}

        assert_error::<BloomBuildError>();
        assert_error::<BloomDecodeError>();
    }

    #[test]
    fn public_format_descriptor_matches_v1_serialized_header() {
        let descriptor = format_descriptor();
        let filter = BloomFilter::try_with_num_bits(512, 3).unwrap();
        let bytes = filter.try_to_bytes().unwrap();

        assert_eq!(
            descriptor.format_version,
            u32::from_le_bytes(bytes[8..12].try_into().unwrap())
        );
        assert_eq!(
            descriptor.hash_seed,
            u64::from_le_bytes(bytes[12..20].try_into().unwrap())
        );
    }

    #[test]
    fn v1_encoder_matches_frozen_golden_bytes() {
        let mut filter = BloomFilter::try_with_num_bits(512, 3).unwrap();
        filter.insert_key(b"alpha");
        filter.insert_key(b"beta");

        assert_eq!(filter.try_to_bytes().unwrap(), V1_GOLDEN_BYTES);
    }

    #[test]
    fn v1_golden_fixture_decodes_without_false_negatives() {
        let descriptor = format_descriptor();
        assert_eq!(
            u32::from_le_bytes(V1_GOLDEN_BYTES[8..12].try_into().unwrap()),
            descriptor.format_version
        );
        assert_eq!(
            u64::from_le_bytes(V1_GOLDEN_BYTES[12..20].try_into().unwrap()),
            descriptor.hash_seed
        );

        let decoded = BloomFilter::from_bytes(&V1_GOLDEN_BYTES).unwrap();

        assert!(decoded.may_contain_key(b"alpha"));
        assert!(decoded.may_contain_key(b"beta"));
    }
}
