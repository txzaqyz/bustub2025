//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include <vector>
#include "common/macros.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Schedules a request for the DiskManager to execute.
 *
 * @param requests The requests to be scheduled.
 */
void DiskScheduler::Schedule(std::vector<DiskRequest> &requests) {
  for (auto &request : requests) {
    // Put the request into the queue, moving it to avoid copying
    request_queue_.Put(std::make_optional<DiskRequest>(std::move(request)));
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Background worker thread function that processes scheduled requests.
 *
 * The background thread needs to process requests while the DiskScheduler exists, i.e., this function should not
 * return until ~DiskScheduler() is called. At that point you need to make sure that the function does return.
 */
void DiskScheduler::StartWorkerThread() {
  while (true) {
    auto opt_request = request_queue_.Get();
    if (!opt_request.has_value()) {
      return;
    }
    // Process the request
    auto [is_write_, data_, page_id_, callback_] = std::move(*opt_request);
    // Execute the request and set the promise value accordingly
    try {
      if (is_write_) {
        disk_manager_->WritePage(page_id_, data_);
        callback_.set_value(true);
      } else {
        disk_manager_->ReadPage(page_id_, data_);
        callback_.set_value(true);
      }
    } catch (...) {
      try {
        // If an exception occurs, set the promise value to false
        callback_.set_value(false);
      } catch (...) {
      }
    }
  }
}

}  // namespace bustub
