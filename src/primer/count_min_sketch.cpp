//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// count_min_sketch.cpp
//
// Identification: src/primer/count_min_sketch.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/count_min_sketch.h"

#include <queue>
#include <stdexcept>
#include <string>

namespace bustub {

/**
 * Constructor for the count-min sketch.
 *
 * @param width The width of the sketch matrix.
 * @param depth The depth of the sketch matrix.
 * @throws std::invalid_argument if width or depth are zero.
 */
template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(uint32_t width, uint32_t depth) : width_(width), depth_(depth) {
  /** @TODO(student) Implement this function! */

  // Validate parameters
  if (width == 0 || depth == 0) {
    throw std::invalid_argument("Width and depth must be greater than zero.");
  }
  // Initialize sketch matrix with zeros
  auto n = static_cast<size_t>(width_) * static_cast<size_t>(depth_);
  sketch_matrix_ = std::make_unique<std::atomic<uint32_t>[]>(n);

  /** @fall2025 PLEASE DO NOT MODIFY THE FOLLOWING */
  // Initialize seeded hash functions
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }
}

// Move constructor
template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(CountMinSketch &&other) noexcept
    : width_(other.width_),
      depth_(other.depth_),
      sketch_matrix_(std::move(other.sketch_matrix_)),
      items(std::move(other.items)),
      k_limit_(other.k_limit_),
      top_k_heap_(std::move(other.top_k_heap_)) {
  /** @TODO(student) Implement this function! */

  /**
   *
   * @warning 🚨🚨🚨
   * HashFunction 内部 lambda 捕获了 this。
   * 对象 move 后必须重建 hash_functions_，否则行为未定义。
   */
  hash_functions_.clear();
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }

  // Invalidate other
  other.width_ = 0;
  other.depth_ = 0;
  other.hash_functions_.clear();
  other.sketch_matrix_.reset();
  other.items.clear();
  other.k_limit_ = 0;
}

template <typename KeyType>
auto CountMinSketch<KeyType>::operator=(CountMinSketch &&other) noexcept -> CountMinSketch & {
  /** @TODO(student) Implement this function! */
  if (this != &other) {
    // Move data members
    width_ = other.width_;
    depth_ = other.depth_;

    // warning🚨🚨🚨: std::function is not moveable, so we need to copy them
    hash_functions_.clear();
    hash_functions_.reserve(depth_);
    for (size_t i = 0; i < depth_; i++) {
      hash_functions_.push_back(this->HashFunction(i));
    }

    sketch_matrix_ = std::move(other.sketch_matrix_);
    items = std::move(other.items);
    k_limit_ = other.k_limit_;
    top_k_heap_ = std::move(other.top_k_heap_);
    // Invalidate other
    other.width_ = 0;
    other.depth_ = 0;
    other.hash_functions_.clear();
    other.sketch_matrix_.reset();
    other.items.clear();
    other.k_limit_ = 0;
  }

  return *this;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Insert(const KeyType &item) {
  /** @TODO(student) Implement this function! */
  if (width_ == 0 || depth_ == 0) {
    throw std::invalid_argument("CountMinSketch is not properly initialized.");
  }

  for (size_t i = 0; i < depth_; i++) {
    const auto hash_idx = static_cast<uint32_t>(hash_functions_[i](item));
    // mutex_.lock();
    sketch_matrix_[i * width_ + hash_idx].fetch_add(1, std::memory_order_relaxed);
    // mutex_.unlock();
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    // Track unique items
    items.insert(item);

    // Update top-k tracking
    UpdateTopK(item);
  }

}

template <typename KeyType>
void CountMinSketch<KeyType>::Merge(const CountMinSketch<KeyType> &other) {
  // Check for dimension compatibility
  if (width_ != other.width_ || depth_ != other.depth_) {
    throw std::invalid_argument("Incompatible CountMinSketch dimensions for merge.");
  }
  /** @TODO(student) Implement this function! */
  // Merge sketch matrices by element-wise addition
  for (uint64_t i = 0; i < depth_ * width_; ++i) {
    sketch_matrix_[i].fetch_add(other.sketch_matrix_[i].load(std::memory_order_relaxed), std::memory_order_relaxed);
  }

  // merge unique items
  items.insert(other.items.begin(), other.items.end());

  // heap becomes stale after merge (counts changed), safest is to clear it
  top_k_heap_ = decltype(top_k_heap_)();
}

template <typename KeyType>
auto CountMinSketch<KeyType>::Count(const KeyType &item) const -> uint32_t {
  // Validate initialization
  if (width_ == 0 || depth_ == 0) {
    throw std::invalid_argument("CountMinSketch is not properly initialized.");
  }

  // Estimate count using the minimum count from all hash functions
  uint32_t min_count = UINT32_MAX;
  for (uint32_t i = 0; i < depth_; ++i) {
    const auto hash_idx = static_cast<uint32_t>(hash_functions_[i](item));
    auto v = sketch_matrix_[i * width_ + hash_idx].load(std::memory_order_relaxed);
    min_count = std::min(min_count, v);
  }

  return min_count == UINT32_MAX ? 0 : min_count;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Clear() {
  /** @TODO(student) Implement this function! */
  // Reset sketch matrix to zeros
  for (uint64_t i = 0; i < depth_ * width_; ++i) {
    sketch_matrix_[i].store(0, std::memory_order_relaxed);
  }

  // Clear unique items set
  items.clear();
  // Clear top-k min-heap
  top_k_heap_ = decltype(top_k_heap_)();
  k_limit_ = 0;
}

template <typename KeyType>
auto CountMinSketch<KeyType>::TopK(uint16_t k, const std::vector<KeyType> &candidates)
    -> std::vector<std::pair<KeyType, uint32_t>> {
  /** @TODO(student) Implement this function! */
  std::vector<std::pair<KeyType, uint32_t>> result;

  if (k == 0 || candidates.empty()) return result;

  // Set k limit if not already set
  if (k_limit_ == 0) k_limit_ = k;

  uint16_t effective_k = std::min(k, k_limit_);

  top_k_heap_ = decltype(top_k_heap_)();

  std::unordered_set<KeyType> unique_candidates(candidates.begin(), candidates.end());
  // Build min-heap of top k candidates
  for (const auto &candidate : unique_candidates) {
    uint32_t count = Count(candidate);
    top_k_heap_.emplace(std::make_pair(candidate, count));
    if (top_k_heap_.size() > effective_k) {
      top_k_heap_.pop();
    }
  }
  result.reserve(top_k_heap_.size());
  // Extract top k items from the heap
  while (!top_k_heap_.empty()) {
    result.push_back(top_k_heap_.top());
    top_k_heap_.pop();
  }
  std::reverse(result.begin(), result.end());  // To get descending order

  return result;
}

template <typename KeyType>
void CountMinSketch<KeyType>::UpdateTopK(const KeyType &item) {
  if (k_limit_ == 0) {
    return;  // No top-k tracking
  }

  // Update min-heap with the new item
  uint32_t count = Count(item);
  top_k_heap_.push(std::make_pair(item, count));
  if (top_k_heap_.size() > k_limit_) {
    top_k_heap_.pop();
  }
}

// Explicit instantiations for all types used in tests
template class CountMinSketch<std::string>;
template class CountMinSketch<int64_t>;  // For int64_t tests
template class CountMinSketch<int>;      // This covers both int and int32_t
}  // namespace bustub
