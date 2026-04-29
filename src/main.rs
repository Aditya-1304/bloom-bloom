use std::sync::Arc;
use std::thread;
use std::time::Instant;

use bloom_bloom::AtomicBloomFilter;

fn main() {
    let expected_items = 100_000usize;
    let target_fp_rate = 0.01;

    let thread_count = thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(4);

    let filter = Arc::new(AtomicBloomFilter::with_false_positive_rate(
        expected_items,
        target_fp_rate,
    ));

    println!("Atomic Bloom filter config:");
    println!("  expected items:      {expected_items}");
    println!("  target fp rate:      {target_fp_rate}");
    println!("  num bits:            {}", filter.num_bits());
    println!("  num hashes:          {}", filter.num_hashes());
    println!("  worker threads:      {thread_count}");
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
    let chunk_size = expected_items.div_ceil(thread_count);

    let mut handles = Vec::new();

    for thread_id in 0..thread_count {
        let start_value = thread_id * chunk_size;
        let end_value = ((thread_id + 1) * chunk_size).min(expected_items);

        if start_value >= end_value {
            continue;
        }

        let filter = Arc::clone(&filter);

        let handle = thread::spawn(move || {
            for value in start_value as u64..end_value as u64 {
                filter.insert(&value);
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("insert worker thread panicked");
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

    println!("Results:");
    println!("  parallel insert time: {:?}", insert_elapsed);
    println!("  present lookup time:  {:?}", present_lookup_elapsed);
    println!("  missing lookup time:  {:?}", missing_lookup_elapsed);
    println!("  present hits:         {present_hits}/{expected_items}");
    println!("  false positives:      {false_positives}/{expected_items}");
    println!("  measured fp rate:     {:.4}", measured_fp_rate);

    assert_eq!(present_hits, expected_items);
}
