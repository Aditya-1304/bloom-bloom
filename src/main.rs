use bloom_bloom::BloomFilter;
use std::time::Instant;

fn main() {
    let expected_items = 100_000usize;
    let target_fp_rate = 0.01;

    let mut filter = BloomFilter::with_false_positive_rate(expected_items, target_fp_rate);

    println!("Bloom filter config:");
    println!("  expected items: {expected_items}");
    println!("  target fp rate: {target_fp_rate}");
    println!("  num bits:       {}", filter.num_bits());
    println!("  num hashes:     {}", filter.num_hashes());
    println!();

    let start = Instant::now();

    for value in 0..expected_items as u64 {
        filter.insert(&value);
    }

    let insert_elapsed = start.elapsed();
    let start = Instant::now();

    let mut present_hits = 0usize;

    for value in 0..expected_items as u64 {
        if filter.contains(&value) {
            present_hits += 1;
        }
    }

    let present_lookup_elapsed = start.elapsed();
    let start = Instant::now();
    let mut false_positives = 0usize;
    let missing_start = 1_000_000u64;

    for value in missing_start..(missing_start + expected_items as u64) {
        if filter.contains(&value) {
            false_positives += 1;
        }
    }

    let missing_lookup_elapsed = start.elapsed();

    let measured_fp_rate = false_positives as f64 / expected_items as f64;

    let estimated_density = filter.expected_density(expected_items);
    let estimated_fp_rate = filter.expected_false_positive_rate(expected_items);

    println!("Results:");
    println!("  insert time:          {:?}", insert_elapsed);
    println!("  present lookup time:  {:?}", present_lookup_elapsed);
    println!("  missing lookup time:  {:?}", missing_lookup_elapsed);
    println!("  present hits:         {present_hits}/{expected_items}");
    println!("  false positives:      {false_positives}/{expected_items}");
    println!("  measured fp rate:     {:.4}", measured_fp_rate);
    println!("  estimated density:   {:.4}", estimated_density);
    println!("  estimated fp rate:   {:.4}", estimated_fp_rate);

    assert_eq!(present_hits, expected_items);
}
