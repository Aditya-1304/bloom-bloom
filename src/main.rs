use std::time::Instant;

use bloom_bloom::AtomicBloomFilter;
use rayon::prelude::*;

fn main() {
    let expected_items = 100_000usize;
    let target_fp_rate = 0.01;

    let filter = AtomicBloomFilter::with_false_positive_rate(expected_items, target_fp_rate);

    println!("Atomic Bloom filter config:");
    println!("  expected items:      {expected_items}");
    println!("  target fp rate:      {target_fp_rate}");
    println!("  num bits:            {}", filter.num_bits());
    println!("  num hashes:          {}", filter.num_hashes());
    println!("  rayon threads:       {}", rayon::current_num_threads());
    println!(
        "  estimated density:   {:.4}",
        filter.expected_density(expected_items)
    );
    println!(
        "  estimated fp rate:   {:.4}",
        filter.expected_false_positive_rate(expected_items)
    );
    println!();

    let start = Instant::now();

    (0..expected_items as u64)
        .into_par_iter()
        .for_each(|value| {
            let key = value.to_be_bytes();
            filter.insert_key(&key);
        });

    let insert_elapsed = start.elapsed();

    let start = Instant::now();

    let present_hits = (0..expected_items as u64)
        .into_par_iter()
        .filter(|value| {
            let key = value.to_be_bytes();
            filter.contains_key(&key)
        })
        .count();

    let present_lookup_elapsed = start.elapsed();

    let start = Instant::now();

    let missing_start = 1_000_000u64;

    let false_positives = (missing_start..missing_start + expected_items as u64)
        .into_par_iter()
        .filter(|value| {
            let key = value.to_be_bytes();
            filter.contains_key(&key)
        })
        .count();

    let missing_lookup_elapsed = start.elapsed();

    let measured_fp_rate = false_positives as f64 / expected_items as f64;

    println!("Results:");
    println!("  parallel insert time: {:?}", insert_elapsed);
    println!("  present lookup time:  {:?}", present_lookup_elapsed);
    println!("  missing lookup time:  {:?}", missing_lookup_elapsed);
    println!("  present hits:         {present_hits}/{expected_items}");
    println!("  false positives:      {false_positives}/{expected_items}");
    println!("  measured fp rate:     {:.4}", measured_fp_rate);

    assert_eq!(present_hits, expected_items);
}
