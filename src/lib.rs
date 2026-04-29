use std::hash::{DefaultHasher, Hash, Hasher};

// here, we store packs of 64 bits in form of Vec<u64>
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BitVec64 {
    words: Vec<u64>, // simple vector store containing 64 bits on each index

    // number of bits we will need
    // this can be smaller than words.len() * 64
    num_bits: usize,
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

    pub fn insert<T: Hash + ?Sized>(&mut self, value: &T) -> bool {
        let mut previously_contained = true;

        for seed in 0..self.num_hashes {
            let hash = hash_with_seed(value, seed as u64);

            let index = (hash as usize) % self.num_bits();

            let was_set = self.bits.set_bit_to_1(index);

            previously_contained &= was_set;
        }

        previously_contained
    }

    pub fn contains<T: Hash + ?Sized>(&self, value: &T) -> bool {
        for seed in 0..self.num_hashes {
            let hash = hash_with_seed(value, seed as u64);
            let index = (hash as usize) % self.num_bits();

            if !self.bits.check(index) {
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

fn hash_with_seed<T: Hash + ?Sized>(value: &T, seed: u64) -> u64 {
    let mut hasher = DefaultHasher::new();

    seed.hash(&mut hasher);

    value.hash(&mut hasher);

    hasher.finish()
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

        assert!(!filter.contains("hello"));
        assert!(!filter.contains("rust"));
        assert!(!filter.contains(&12345));
    }

    #[test]
    fn inserted_value_is_contained() {
        let mut filter = BloomFilter::with_num_bits(1024, 3);

        filter.insert("hello");

        assert!(filter.contains("hello"));
    }

    #[test]
    fn many_inserted_values_are_contained() {
        let mut filter = BloomFilter::with_num_bits(10_000, 4);

        for value in 0..1000 {
            filter.insert(&value);
        }

        for value in 0..1000 {
            assert!(filter.contains(&value));
        }
    }

    #[test]
    fn insert_reports_whether_all_bits_were_already_set() {
        let mut filter = BloomFilter::with_num_bits(1024, 3);

        assert!(!filter.insert("hello"));

        assert!(filter.insert("hello"));
    }
}
