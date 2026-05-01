use xxhash_rust::xxh3::xxh3_128_with_seed;

const BLOCK_WORDS: usize = 8;
const WORD_BITS: usize = 64;
const BLOCK_BITS: usize = BLOCK_WORDS * WORD_BITS;

const BLOOM_MAGIC: [u8; 8] = *b"BLMFILT1";
const BLOOM_VERSION: u32 = 1;
pub const BLOOM_HASH_SEED: u64 = 0xD6E8_FD9A_2C4B_1A37;
const BLOCK_INDEX_BITS: u32 = 9;
const BLOCK_MASK: u64 = (BLOCK_BITS as u64) - 1;

#[repr(align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Block {
    words: [u64; BLOCK_WORDS],
}

impl Block {
    fn empty() -> Self {
        Self {
            words: [0; BLOCK_WORDS],
        }
    }

    fn set(&mut self, bit_index: usize) -> bool {
        let word_index = bit_index >> 6;
        let bit_offset = bit_index & 63;
        let mask = 1u64 << bit_offset;

        let was_set = self.words[word_index] & mask != 0;
        self.words[word_index] |= mask;
        was_set
    }

    fn check(&self, bit_index: usize) -> bool {
        let word_index = bit_index >> 6;
        let bit_offset = bit_index & 63;
        let mask = 1u64 << bit_offset;

        self.words[word_index] & mask != 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BloomFilter {
    blocks: Vec<Block>,
    num_hashes: u32,
}

impl BloomFilter {
    pub fn with_num_bits(num_bits: usize, num_hashes: u32) -> Self {
        assert!(num_bits > 0, "Bloom filter must have at least one bit");
        assert!(num_hashes > 0, "Bloom filter must use at least one hash");

        let num_blocks = num_bits.div_ceil(BLOCK_BITS).max(1);

        Self {
            blocks: vec![Block::empty(); num_blocks],
            num_hashes,
        }
    }

    pub fn with_false_positive_rate(expected_items: usize, false_positive_rate: f64) -> Self {
        let mut num_bits = optimal_num_bits(expected_items, false_positive_rate);

        loop {
            let num_blocks = num_bits.div_ceil(BLOCK_BITS).max(1);
            let actual_bits = num_blocks * BLOCK_BITS;
            let num_hashes = optimal_num_hashes(actual_bits, expected_items);

            let expected_fp =
                expected_block_false_positive_rate(num_blocks, num_hashes, expected_items);

            if expected_fp <= false_positive_rate {
                return Self::with_num_bits(actual_bits, num_hashes);
            }

            num_bits = actual_bits + BLOCK_BITS;
        }
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
        expected_false_positive_rate(self.num_bits(), self.num_hashes(), inserted_items)
    }

    pub fn insert_key(&mut self, key: &[u8]) -> bool {
        let hash = xxh3_128_with_seed(key, BLOOM_HASH_SEED);

        let block_hash = hash as u64;
        let bit_hash = (hash >> 64) as u64;

        let block_index = index(self.num_blocks(), block_hash);
        let block = &mut self.blocks[block_index];

        let mut previously_contained = true;

        for bit_index in block_bit_indexes(bit_hash, self.num_hashes) {
            previously_contained &= block.set(bit_index);
        }

        previously_contained
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        let hash = xxh3_128_with_seed(key, BLOOM_HASH_SEED);

        let block_hash = hash as u64;
        let bit_hash = (hash >> 64) as u64;

        let block_index = index(self.num_blocks(), block_hash);
        let block = &self.blocks[block_index];

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

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(32 + self.num_bits() / 8);

        out.extend_from_slice(&BLOOM_MAGIC);
        out.extend_from_slice(&BLOOM_VERSION.to_le_bytes());
        out.extend_from_slice(&BLOOM_HASH_SEED.to_le_bytes());
        out.extend_from_slice(&(self.num_blocks() as u64).to_le_bytes());
        out.extend_from_slice(&self.num_hashes.to_le_bytes());

        for block in &self.blocks {
            for word in block.words {
                out.extend_from_slice(&word.to_le_bytes());
            }
        }

        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, BloomDecodeError> {
        let header_len = 8 + 4 + 8 + 8 + 4;

        if bytes.len() < header_len {
            return Err(BloomDecodeError::TooShort);
        }

        if bytes[0..8] != BLOOM_MAGIC {
            return Err(BloomDecodeError::BadMagic);
        }

        let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        if version != BLOOM_VERSION {
            return Err(BloomDecodeError::UnsupportedVersion(version));
        }

        let seed = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
        if seed != BLOOM_HASH_SEED {
            return Err(BloomDecodeError::WrongHashSeed(seed));
        }

        let num_blocks = u64::from_le_bytes(bytes[20..28].try_into().unwrap()) as usize;
        let num_hashes = u32::from_le_bytes(bytes[28..32].try_into().unwrap());

        if num_blocks == 0 {
            return Err(BloomDecodeError::InvalidNumBlocks);
        }

        if num_hashes == 0 {
            return Err(BloomDecodeError::InvalidNumHashes);
        }

        let expected_len = header_len + num_blocks * BLOCK_WORDS * 8;
        if bytes.len() != expected_len {
            return Err(BloomDecodeError::LengthMismatch);
        }

        let mut blocks = Vec::with_capacity(num_blocks);
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

fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^= x >> 31;
    x
}

pub fn optimal_num_bits(expected_items: usize, false_positive_rate: f64) -> usize {
    assert!(expected_items > 0, "expected_items must be greater than 0");

    assert!(
        false_positive_rate > 0.0 && false_positive_rate < 1.0,
        "false_positive_rate must be between 0 and 1"
    );

    let n = expected_items as f64;
    let p = false_positive_rate;

    let ln_2 = std::f64::consts::LN_2;

    let raw_bits = -(n * p.ln()) / (ln_2 * ln_2);
    let bits = raw_bits.ceil() as usize;

    bits.div_ceil(64) * 64
}

pub fn optimal_num_hashes(num_bits: usize, expected_items: usize) -> u32 {
    assert!(num_bits > 0, "num_bits must be greater than 0");
    assert!(expected_items > 0, "expected_items must be greater than 0");

    let m = num_bits as f64;
    let n = expected_items as f64;

    let raw_hashes = (m / n) * std::f64::consts::LN_2;
    let hashes = raw_hashes.round() as u32;

    hashes.max(1)
}

fn index(num_bits: usize, hash: u64) -> usize {
    assert!(num_bits > 0, "num_bits must be greater than 0");

    ((hash as u128 * num_bits as u128) >> 64) as usize
}

pub fn expected_density(num_bits: usize, num_hashes: u32, inserted_items: usize) -> f64 {
    assert!(num_bits > 0, "num_bits must be greater than 0");
    assert!(num_hashes > 0, "num_hashes must be greater than 0");

    let m = num_bits as f64;
    let k = num_hashes as f64;
    let n = inserted_items as f64;

    1.0 - (-(k * n) / m).exp()
}

pub fn expected_false_positive_rate(
    num_bits: usize,
    num_hashes: u32,
    inserted_items: usize,
) -> f64 {
    let density = expected_density(num_bits, num_hashes, inserted_items);

    density.powi(num_hashes as i32)
}

pub fn expected_block_false_positive_rate(
    num_blocks: usize,
    num_hashes: u32,
    inserted_items: usize,
) -> f64 {
    assert!(num_blocks > 0, "num_blocks must be greater than 0");
    assert!(num_hashes > 0, "num_hashes must be greater than 0");

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
pub enum BloomDecodeError {
    TooShort,
    BadMagic,
    UnsupportedVersion(u32),
    WrongHashSeed(u64),
    InvalidNumBlocks,
    InvalidNumHashes,
    LengthMismatch,
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
