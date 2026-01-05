// :bustub-keep-private:
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.cpp
//
// Identification: src/buffer/arc_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/arc_replacer.h"
#include <optional>
#include "common/config.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new ArcReplacer, with lists initialized to be empty and target size to 0
 * @param num_frames the maximum number of frames the ArcReplacer will be required to cache
 */
ArcReplacer::ArcReplacer(size_t num_frames) : replacer_size_(num_frames) {
  curr_size_ = 0;
  mru_target_size_ = 0;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Performs the Replace operation as described by the writeup
 * that evicts from either mfu_ or mru_ into its corresponding ghost list
 * according to balancing policy.
 *
 * If you wish to refer to the original ARC paper, please note that there are
 * two changes in our implementation:
 * 1. When the size of mru_ equals the target size, we don't check
 * the last access as the paper did when deciding which list to evict from.
 * This is fine since the original decision is stated to be arbitrary.
 * 2. Entries that are not evictable are skipped. If all entries from the desired side
 * (mru_ / mfu_) are pinned, we instead try victimize the other side (mfu_ / mru_),
 * and move it to its corresponding ghost list (mfu_ghost_ / mru_ghost_).
 *
 * @return frame id of the evicted frame, or std::nullopt if cannot evict
 */
auto ArcReplacer::Evict() -> std::optional<frame_id_t> {
  std::scoped_lock<std::mutex> lock(latch_);
  if (curr_size_ == 0) {
    return std::nullopt;
  }

  // function to find victim from a list
  auto victim_from_list = [&](std::list<frame_id_t> &frame_list) -> std::optional<frame_id_t> {
    for (auto it = frame_list.rbegin(); it != frame_list.rend(); ++it) {
      frame_id_t frame_id = *it;
      auto status_it = alive_map_.find(frame_id);
      if (status_it == alive_map_.end()) {
        continue;
      }
      if (status_it->second->evictable_) {
        return frame_id;
      }
    }
    return std::nullopt;
  };

  const bool prefer_mru = mru_.size() >= mru_target_size_;
  // try to evict from preferred list
  std::optional<frame_id_t> victim_frame = prefer_mru ? victim_from_list(mru_) : victim_from_list(mfu_);
  if (!victim_frame.has_value()) {
    // try the other list
    victim_frame = prefer_mru ? victim_from_list(mfu_) : victim_from_list(mru_);
  }

  if (!victim_frame.has_value()) {
    return std::nullopt;
  }

  const frame_id_t victim_frame_id = victim_frame.value();
  auto it = alive_map_.find(victim_frame_id);
  if (it == alive_map_.end()) {
    return std::nullopt;
  }

  const std::shared_ptr<FrameStatus> frame_status = it->second;
  const page_id_t page_id = frame_status->page_id_;
  // --- Optimization: O(1) Removal using iterators ---
  auto iter_it = alive_iterators_.find(victim_frame_id);
  if (iter_it != alive_iterators_.end()) {
    if (frame_status->arc_status_ == ArcStatus::MFU) {
      mfu_.erase(iter_it->second);
      mfu_ghost_.push_front(page_id);
      ghost_iterators_[page_id] = mfu_ghost_.begin(); // Update ghost iterator
      ghost_map_[page_id] = std::make_shared<FrameStatus>(page_id, victim_frame_id, false, ArcStatus::MFU_GHOST);
    } else if (frame_status->arc_status_ == ArcStatus::MRU) {
      mru_.erase(iter_it->second);
      mru_ghost_.push_front(page_id);
      ghost_iterators_[page_id] = mru_ghost_.begin(); // Update ghost iterator
      ghost_map_[page_id] = std::make_shared<FrameStatus>(page_id, victim_frame_id, false, ArcStatus::MRU_GHOST);
    }
    alive_iterators_.erase(iter_it);
  } else {
    // Fallback (Should not be reached if map is maintained correctly)
    BUSTUB_ASSERT(false, "Iterator missing for alive frame");
  }

  if (frame_status->evictable_) {
    curr_size_--;
  }
  alive_map_.erase(it);

  return victim_frame_id;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record access to a frame, adjusting ARC bookkeeping accordingly
 * by bring the accessed page to the front of mfu_ if it exists in any of the lists
 * or the front of mru_ if it does not.
 *
 * Performs the operations EXCEPT REPLACE described in original paper, which is
 * handled by `Evict()`.
 *
 * Consider the following four cases, handle accordingly:
 * 1. Access hits mru_ or mfu_
 * 2/3. Access hits mru_ghost_ / mfu_ghost_
 * 4. Access misses all the lists
 *
 * This routine performs all changes to the four lists as preperation
 * for `Evict()` to simply find and evict a victim into ghost lists.
 *
 * Note that frame_id is used as identifier for alive pages and
 * page_id is used as identifier for the ghost pages, since page_id is
 * the unique identifier to the page after it's dead.
 * Using page_id for alive pages should be the same since it's one to one mapping,
 * but using frame_id is slightly more intuitive.
 *
 * @param frame_id id of frame that received a new access.
 * @param page_id id of page that is mapped to the frame.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) <= replacer_size_, "frame_id is invalid");
  std::scoped_lock<std::mutex> lock(latch_);

  // case 0: validate if frame_id is already alive, but mapped to different page_id
  auto it = alive_map_.find(frame_id);
  if (it != alive_map_.end() && it->second->page_id_ != page_id) {
    auto &old_frame_status = it->second;
    auto iter_it = alive_iterators_.find(frame_id);
    if (iter_it != alive_iterators_.end()) {
      if (old_frame_status->arc_status_ == ArcStatus::MRU) {
        mru_.erase(iter_it->second);
      } else {
        mfu_.erase(iter_it->second);
      }
      alive_iterators_.erase(iter_it);
    }

    if (old_frame_status->evictable_) {
      curr_size_--;
    }
    alive_map_.erase(it);
  }

  // case 1: hit in mru_ or mfu_
  it = alive_map_.find(frame_id);
  if (it != alive_map_.end()) {
    auto &frame_status = it->second;

    // Get iterator to remove efficiently
    auto iter_it = alive_iterators_.find(frame_id);
    BUSTUB_ASSERT(iter_it != alive_iterators_.end(), "Iterator missing for alive frame");
    if (frame_status->arc_status_ == ArcStatus::MFU) {
      // hit in mfu_, move to front
      mfu_.erase(iter_it->second);
      mfu_.push_front(frame_id);
      alive_iterators_[frame_id] = mfu_.begin(); // Update
    } else if (frame_status->arc_status_ == ArcStatus::MRU) {
      // hit in mru_, promote to mfu_
      mru_.erase(iter_it->second);
      mfu_.push_front(frame_id);
      frame_status->arc_status_ = ArcStatus::MFU;
      alive_iterators_[frame_id] = mfu_.begin(); // Update
    } else {
      BUSTUB_ASSERT(false, "frame is not in either mru_ or mfu_");
    }
    return;
  }

  // case 2/3: hit in ghost lists
  auto ghost_it = ghost_map_.find(page_id);
  if (ghost_it != ghost_map_.end()) {
    auto &frame_status = ghost_it->second;

    // Clean up from ghost list efficiently
    auto g_iter_it = ghost_iterators_.find(page_id);
    BUSTUB_ASSERT(g_iter_it != ghost_iterators_.end(), "Iterator missing for ghost page");

    const size_t mru_ghost_size = mru_ghost_.size();
    const size_t mfu_ghost_size = mfu_ghost_.size();

    if (frame_status->arc_status_ == ArcStatus::MRU_GHOST) {
      // case 2: hit in mru_ghost_
      // increase target size
      const size_t delta = mru_ghost_size >= mfu_ghost_size ? 1 : mfu_ghost_size / mru_ghost_size;
      mru_target_size_ = std::min(replacer_size_, mru_target_size_ + delta);
      mru_ghost_.erase(g_iter_it->second); // O(1) erase


    } else if (frame_status->arc_status_ == ArcStatus::MFU_GHOST) {
      // case 3: hit in mfu_ghost_
      // decrease target size
      const size_t delta = mfu_ghost_size >= mru_ghost_size ? 1 : mru_ghost_size / mfu_ghost_size;
      mru_target_size_ = (mru_target_size_ > delta) ? mru_target_size_ - delta : 0;

      mfu_ghost_.erase(g_iter_it->second); // O(1) erase

    } else {
      BUSTUB_ASSERT(false, "frame is not in either ghost list");
    }
    // promote to mfu_
    ghost_iterators_.erase(g_iter_it);
    ghost_map_.erase(ghost_it);
    // create new FrameStatus for alive_map_
    mfu_.push_front(frame_id);
    alive_iterators_[frame_id] = mfu_.begin(); // Track new iterator
    alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MFU);

    return;
  }

  // case 4: miss in all lists -> insert to mru_
  const size_t total_mru_size = mru_.size() + mru_ghost_.size();
  if (total_mru_size == replacer_size_) {
    page_id_t victim_page_id = mru_ghost_.back();
    mru_ghost_.pop_back();
    ghost_map_.erase(victim_page_id);
    ghost_iterators_.erase(victim_page_id);
  } else if (total_mru_size < replacer_size_) {
    const size_t total_mfu_size = mfu_.size() + mfu_ghost_.size();
    if (total_mru_size + total_mfu_size == 2 * replacer_size_) {
      page_id_t victim_page_id = mfu_ghost_.back();
      mfu_ghost_.pop_back();
      ghost_map_.erase(victim_page_id);
      ghost_iterators_.erase(victim_page_id);
    }

  } else {
    BUSTUB_ASSERT(false, "mru_ and mru_ghost_ size exceed replacer_size_");
  }

  mru_.push_front(frame_id);
  alive_iterators_[frame_id] = mru_.begin(); // Track new iterator
  alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MRU);
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);

  // validate frame_id
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) <= replacer_size_, "frame_id is invalid");
  auto it = alive_map_.find(frame_id);
  if (it == alive_map_.end()) {
    return;
  }

  auto frame_status = it->second;

  // no status change
  if (frame_status->evictable_ == set_evictable) {
    return;
  }

  // change evictable status
  if (set_evictable) {
    curr_size_++;
  } else {
    // changing from evictable to non-evictable
    BUSTUB_ASSERT(curr_size_ > 0, "replacer size is already 0");
    curr_size_--;
  }

  frame_status->evictable_ = set_evictable;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * decided by the ARC algorithm.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void ArcReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  // validate frame_id
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) <= replacer_size_, "frame_id is invalid");

  auto it = alive_map_.find(frame_id);
  if (it == alive_map_.end()) {
    return;
  }

  auto frame_status = it->second;
  BUSTUB_ASSERT(frame_status->evictable_, "cannot remove a non-evictable frame");
  // remove from corresponding list
  // Use iterator for O(1) removal
  auto iter_it = alive_iterators_.find(frame_id);
  if (iter_it != alive_iterators_.end()) {
    if (frame_status->arc_status_ == ArcStatus::MRU) {
      mru_.erase(iter_it->second);
    } else if (frame_status->arc_status_ == ArcStatus::MFU) {
      mfu_.erase(iter_it->second);
    } else {
      BUSTUB_ASSERT(false, "frame is not in either mru_ or mfu_");
    }
    alive_iterators_.erase(iter_it);
  }

  // remove from alive_map_
  alive_map_.erase(it);
  curr_size_--;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto ArcReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
