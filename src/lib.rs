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
}
