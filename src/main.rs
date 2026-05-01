use std::env;
use std::time::Instant;

use bloom_bloom::BloomFilter;
use rayon::prelude::*;

fn main() {
    let expected_items = parse_expected_items();
    let target_fp_rate = 0.01;
    let missing_start = 1_000_000_000u64;

    println!("Preparing keys...");
    let prep_start = Instant::now();

    let present_keys = (0..expected_items as u64)
        .map(|value| value.to_be_bytes())
        .collect::<Vec<_>>();

    let missing_keys = (missing_start..missing_start + expected_items as u64)
        .map(|value| value.to_be_bytes())
        .collect::<Vec<_>>();

    let present_key_refs = present_keys
        .iter()
        .map(|key| key.as_slice())
        .collect::<Vec<_>>();

    let missing_key_refs = missing_keys
        .iter()
        .map(|key| key.as_slice())
        .collect::<Vec<_>>();

    let key_prep_elapsed = prep_start.elapsed();

    let mut filter = BloomFilter::with_false_positive_rate(expected_items, target_fp_rate);

    println!("SSTable Bloom filter config:");
    println!("  expected items:      {expected_items}");
    println!("  target fp rate:      {target_fp_rate}");
    println!("  num bits:            {}", filter.num_bits());
    println!("  num hashes:          {}", filter.num_hashes());
    println!("  num blocks:          {}", filter.num_blocks());
    println!("  serialized bytes:    {}", filter.serialized_len());
    println!(
        "  serialized MiB:      {:.2}",
        filter.serialized_len() as f64 / (1024.0 * 1024.0)
    );
    println!("  rayon threads:       {}", rayon::current_num_threads());
    println!("  prefetch feature:    {}", cfg!(feature = "prefetch"));
    println!(
        "  estimated density:   {:.4}",
        filter.expected_density(expected_items)
    );
    println!(
        "  estimated fp rate:   {:.4}",
        filter.expected_false_positive_rate(expected_items)
    );
    println!("  key prep time:       {:?}", key_prep_elapsed);
    println!();

    let start = Instant::now();

    for key in &present_keys {
        filter.insert_key(key);
    }

    let insert_elapsed = start.elapsed();

    let start = Instant::now();

    let encoded = filter.to_bytes();
    let loaded_filter =
        BloomFilter::from_bytes(&encoded).expect("serialized Bloom filter should decode");

    let serialize_roundtrip_elapsed = start.elapsed();

    let warmup_normal = present_key_refs
        .par_iter()
        .take(10_000.min(expected_items))
        .filter(|key| loaded_filter.may_contain_key(*key))
        .count();

    let warmup_batch = loaded_filter
        .count_may_contain_keys_prefetch(&present_key_refs[..10_000.min(expected_items)]);

    assert_eq!(warmup_normal, warmup_batch);

    let start = Instant::now();

    let present_hits_normal = present_key_refs
        .par_iter()
        .filter(|key| loaded_filter.may_contain_key(*key))
        .count();

    let present_normal_elapsed = start.elapsed();

    let start = Instant::now();

    let false_positives_normal = missing_key_refs
        .par_iter()
        .filter(|key| loaded_filter.may_contain_key(*key))
        .count();

    let missing_normal_elapsed = start.elapsed();

    let start = Instant::now();

    let present_hits_prefetch = present_key_refs
        .par_chunks(4096)
        .map(|chunk| loaded_filter.count_may_contain_keys_prefetch(chunk))
        .sum::<usize>();

    let present_prefetch_elapsed = start.elapsed();

    let start = Instant::now();

    let false_positives_prefetch = missing_key_refs
        .par_chunks(4096)
        .map(|chunk| loaded_filter.count_may_contain_keys_prefetch(chunk))
        .sum::<usize>();

    let missing_prefetch_elapsed = start.elapsed();

    assert_eq!(present_hits_normal, present_hits_prefetch);
    assert_eq!(false_positives_normal, false_positives_prefetch);

    let measured_fp_rate = false_positives_normal as f64 / expected_items as f64;

    println!("Results:");
    println!("  build insert time:           {:?}", insert_elapsed);
    println!(
        "  serialize+load time:         {:?}",
        serialize_roundtrip_elapsed
    );
    println!(
        "  normal present lookup:       {:?}",
        present_normal_elapsed
    );
    println!(
        "  normal missing lookup:       {:?}",
        missing_normal_elapsed
    );
    println!(
        "  batched present lookup:      {:?}",
        present_prefetch_elapsed
    );
    println!(
        "  batched missing lookup:      {:?}",
        missing_prefetch_elapsed
    );
    println!("  present hits:                {present_hits_normal}/{expected_items}");
    println!("  false positives:             {false_positives_normal}/{expected_items}");
    println!("  measured fp rate:            {:.4}", measured_fp_rate);

    assert_eq!(present_hits_normal, expected_items);

    let start = Instant::now();

    let present_hits_branchless = present_key_refs
        .par_chunks(4096)
        .map(|chunk| loaded_filter.count_may_contain_keys_prefetch_branchless(chunk))
        .sum::<usize>();

    let present_branchless_elapsed = start.elapsed();

    let start = Instant::now();

    let false_positives_branchless = missing_key_refs
        .par_chunks(4096)
        .map(|chunk| loaded_filter.count_may_contain_keys_prefetch_branchless(chunk))
        .sum::<usize>();

    let missing_branchless_elapsed = start.elapsed();

    assert_eq!(present_hits_normal, present_hits_branchless);
    assert_eq!(false_positives_normal, false_positives_branchless);

    println!(
        "  branchless present lookup:   {:?}",
        present_branchless_elapsed
    );
    println!(
        "  branchless missing lookup:   {:?}",
        missing_branchless_elapsed
    );
}

fn parse_expected_items() -> usize {
    env::args()
        .nth(1)
        .map(|arg| {
            arg.parse::<usize>()
                .expect("first argument must be a positive integer")
        })
        .unwrap_or(1_000_000)
}
