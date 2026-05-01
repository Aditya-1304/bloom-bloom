use std::sync::atomic::{AtomicU64, Ordering};
use xxhash_rust::xxh3::xxh3_128_with_seed;

// here, we store packs of 64 bits in form of Vec<u64>
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BitVec64 {
    words: Vec<u64>, // simple vector store containing 64 bits on each index

    // number of bits we will need
    // this can be smaller than words.len() * 64
    num_bits: usize,
}

#[derive(Debug)]
pub struct AtomicBitVec64 {
    words: Vec<AtomicU64>,
    num_bits: usize,
}

impl AtomicBitVec64 {
    pub fn new(num_bits: usize) -> Self {
        assert!(num_bits > 0, "bit vector must have at least one bit");

        let num_words = num_bits.div_ceil(64);

        let words = (0..num_words).map(|_| AtomicU64::new(0)).collect();

        Self { words, num_bits }
    }

    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    pub fn set(&self, index: usize) -> bool {
        assert!(index < self.num_bits, "bit index out of bounds");

        let word_index = index >> 6;
        let bit_offset = index & 63;
        let mask = 1u64 << bit_offset;
        let old_word = self.words[word_index].fetch_or(mask, Ordering::Relaxed);

        old_word & mask != 0
    }

    pub fn check(&self, index: usize) -> bool {
        assert!(index < self.num_bits, "bit index out of bounds");

        let word_index = index >> 6;
        let bit_offset = index & 63;
        let mask = 1u64 << bit_offset;

        let word = self.words[word_index].load(Ordering::Relaxed);

        word & mask != 0
    }
}

#[derive(Debug)]
pub struct AtomicBloomFilter {
    bits: AtomicBitVec64,
    num_hashes: u32,
}

impl AtomicBloomFilter {
    pub fn with_num_bits(num_bits: usize, num_hashes: u32) -> Self {
        assert!(num_hashes > 0, "Bloom filter must use at least one hash");

        Self {
            bits: AtomicBitVec64::new(num_bits),
            num_hashes,
        }
    }

    pub fn with_false_positive_rate(expected_items: usize, false_positive_rate: f64) -> Self {
        let num_bits = optimal_num_bits(expected_items, false_positive_rate);
        let num_hashes = optimal_num_hashes(num_bits, expected_items);

        Self::with_num_bits(num_bits, num_hashes)
    }

    pub fn num_bits(&self) -> usize {
        self.bits.num_bits()
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

    pub fn insert_key(&self, key: &[u8]) -> bool {
        let mut previously_contained = true;

        let (h1, h2) = base_hashes(key);

        for i in 0..self.num_hashes {
            let hash = nth_hash(h1, h2, i as u64);
            let bit_index = index(self.num_bits(), hash);

            previously_contained &= self.bits.set(bit_index);
        }

        previously_contained
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        let (h1, h2) = base_hashes(key);

        for i in 0..self.num_hashes {
            let hash = nth_hash(h1, h2, i as u64);
            let bit_index = index(self.num_bits(), hash);

            if !self.bits.check(bit_index) {
                return false;
            }
        }

        true
    }

    pub fn insert_str(&self, key: &str) -> bool {
        self.insert_key(key.as_bytes())
    }

    pub fn contains_str(&self, key: &str) -> bool {
        self.contains_key(key.as_bytes())
    }
}

impl BitVec64 {
    pub fn new(num_bits: usize) -> Self {
        // zero size bloom filter is not a real thing lil bro
        assert!(num_bits > 0, "must atleast have one bit");

        // we need to store words so we will divide by 64 bits rounding up
        // example:
        // 1 bit -> 1 word
        // 64 bits -> 1 word
        // 65 bits -> 2 words
        let num_words = num_bits.div_ceil(64);

        // this creates a vec of num_words with value of 0, every bit is 0 too automatically
        Self {
            words: vec![0; num_words],
            num_bits,
        }
    }

    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    // returns :
    // false if bit was previously 0
    // true if bit was already 1
    pub fn set_bit_to_1(&mut self, index: usize) -> bool {
        assert!(index < self.num_bits, "bit index out of bounds");

        // this will convert the logical bit index to word index for u64
        // bit 0..63 lives in word 0
        // bit 64..127 lives in word 1
        let word_index = index >> 6; // same as index / 64

        // this finds the bit position inside word
        let bit_offset = index & 63; // same as index % 64

        // mask with one bit set
        let mask = 1u64 << bit_offset;

        // simple check to see if bit was already 1
        let was_set = self.words[word_index] & mask != 0;

        // set target to 1
        // |= this is bitwise OR
        self.words[word_index] |= mask;

        was_set
    }

    pub fn check(&self, index: usize) -> bool {
        assert!(index < self.num_bits, "bit index out of bounds");

        let word_index = index >> 6;

        let bit_offset = index & 63;

        let mask = 1u64 << bit_offset;

        self.words[word_index] & mask != 0
    }
}

pub struct BloomFilter {
    bits: BitVec64,
    num_hashes: u32,
}

impl BloomFilter {
    pub fn with_num_bits(num_bits: usize, num_hashes: u32) -> Self {
        assert!(num_hashes > 0, "bloom filter should atleast use one hash");

        Self {
            bits: BitVec64::new(num_bits),
            num_hashes,
        }
    }

    pub fn num_bits(&self) -> usize {
        self.bits.num_bits()
    }

    pub fn num_hashes(&self) -> u32 {
        self.num_hashes
    }

    pub fn insert_key(&mut self, key: &[u8]) -> bool {
        let mut previously_contained = true;

        let (h1, h2) = base_hashes(key);

        for i in 0..self.num_hashes {
            let hash = nth_hash(h1, h2, i as u64);
            let bit_index = index(self.num_bits(), hash);

            previously_contained &= self.bits.set_bit_to_1(bit_index);
        }

        previously_contained
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        let (h1, h2) = base_hashes(key);

        for i in 0..self.num_hashes {
            let hash = nth_hash(h1, h2, i as u64);
            let bit_index = index(self.num_bits(), hash);

            if !self.bits.check(bit_index) {
                return false;
            }
        }

        true
    }

    pub fn with_false_positive_rate(expected_items: usize, false_positive_rate: f64) -> Self {
        let num_bits = optimal_num_bits(expected_items, false_positive_rate);

        let num_hashes = optimal_num_hashes(num_bits, expected_items);

        Self::with_num_bits(num_bits, num_hashes)
    }

    pub fn expected_density(&self, inserted_items: usize) -> f64 {
        expected_density(self.num_bits(), self.num_hashes(), inserted_items)
    }

    pub fn expected_false_positive_rate(&self, inserted_items: usize) -> f64 {
        expected_false_positive_rate(self.num_bits(), self.num_hashes(), inserted_items)
    }

    pub fn insert_str(&mut self, key: &str) -> bool {
        self.insert_key(key.as_bytes())
    }

    pub fn contains_str(&self, key: &str) -> bool {
        self.contains_key(key.as_bytes())
    }
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

const BLOOM_HASH_SEED: u64 = 0xD6E8_FD9A_2C4B_1A37;

fn base_hashes(key: &[u8]) -> (u64, u64) {
    let hash = xxh3_128_with_seed(key, BLOOM_HASH_SEED);

    let h1 = hash as u64;
    let h2 = (hash >> 64) as u64;

    (h1, h2 | 1)
}

fn nth_hash(h1: u64, h2: u64, i: u64) -> u64 {
    h1.wrapping_add(i.wrapping_mul(h2))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_bitvec_starts_empty() {
        let bits = BitVec64::new(128);

        assert!(!bits.check(0));
        assert!(!bits.check(63));
        assert!(!bits.check(64));
        assert!(!bits.check(127));
    }

    #[test]
    fn set_marks_a_bit() {
        let mut bits = BitVec64::new(128);

        assert!(!bits.check(42));
        assert!(!bits.set_bit_to_1(42));
        assert!(bits.check(42));
    }

    #[test]
    fn set_returns_whether_bit_was_already_set() {
        let mut bits = BitVec64::new(128);

        assert!(!bits.set_bit_to_1(9));
        assert!(bits.set_bit_to_1(9));
    }

    #[test]
    fn bits_cross_word_boundaries() {
        let mut bits = BitVec64::new(128);

        bits.set_bit_to_1(63);
        bits.set_bit_to_1(64);

        assert!(bits.check(63));
        assert!(bits.check(64));
        assert!(!bits.check(62));
        assert!(!bits.check(65));
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
        assert!(filter.num_bits() >= 64);
        assert!(filter.num_hashes() >= 1);
    }

    #[test]
    fn nth_hash_is_deterministic() {
        let h1 = 10;
        let h2 = 7;

        assert_eq!(nth_hash(h1, h2, 0), 10);
        assert_eq!(nth_hash(h1, h2, 1), 17);
        assert_eq!(nth_hash(h1, h2, 2), 24);
    }

    #[test]
    fn index_is_always_in_bounds() {
        let num_bits = 1000;

        for i in 0..100_000u64 {
            let hash = i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            let bit_index = index(num_bits, hash);

            assert!(bit_index < num_bits);
        }
    }

    #[test]
    fn index_handles_tiny_filters() {
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

        assert!(measured_rate < 0.03);
    }

    #[test]
    fn atomic_bitvec_can_set_and_check_bits() {
        let bits = AtomicBitVec64::new(128);

        assert!(!bits.check(64));
        assert!(!bits.set(64));
        assert!(bits.check(64));
        assert!(bits.set(64));
    }

    #[test]
    fn atomic_bloom_filter_contains_inserted_keys() {
        let filter = AtomicBloomFilter::with_false_positive_rate(1000, 0.01);

        for value in 0..1000u64 {
            filter.insert_key(&value.to_be_bytes());
        }

        for value in 0..1000u64 {
            assert!(filter.contains_key(&value.to_be_bytes()));
        }
    }

    #[test]
    fn atomic_bloom_filter_insert_takes_shared_reference() {
        let filter = AtomicBloomFilter::with_num_bits(1024, 3);

        let shared_ref = &filter;

        shared_ref.insert_key(b"rust");

        assert!(shared_ref.contains_key(b"rust"));
    }

    #[test]
    fn atomic_bloom_filter_supports_parallel_inserts() {
        use std::sync::Arc;
        use std::thread;

        let filter = Arc::new(AtomicBloomFilter::with_false_positive_rate(4000, 0.01));

        let mut handles = Vec::new();

        for thread_id in 0..4u64 {
            let filter = Arc::clone(&filter);

            let handle = thread::spawn(move || {
                let start = thread_id * 1000;
                let end = start + 1000;

                for value in start..end {
                    filter.insert_key(&value.to_be_bytes());
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        for value in 0..4000u64 {
            assert!(filter.contains_key(&value.to_be_bytes()));
        }
    }
}
