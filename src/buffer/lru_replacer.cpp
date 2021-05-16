//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include "common/logger.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  track_size = 0;
  list_pointer = 0;
  manager_size = num_pages;
  lru_frame_list = std::vector<int>(num_pages, -1);
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  bool victim_found = false;
  list_mutex.lock();
  if (track_size > 0) {
    for (size_t i = 0; i <= 2 * manager_size; i++) {
      list_pointer = (list_pointer + 1) % manager_size;
      if (lru_frame_list[list_pointer] == 1) {
        lru_frame_list[list_pointer] = 0;
      } else if (lru_frame_list[list_pointer] == 0) {
        lru_frame_list[list_pointer] = -1;
        track_size--;
        *frame_id = list_pointer;
        victim_found = true;
        break;
      }
      LOG_DEBUG("Victim list_pointer: %d and lru_frame_list: %d in LRUReplacer.cpp", static_cast<int>(list_pointer),
                lru_frame_list[list_pointer]);
    }
    if (!victim_found) {
      LOG_DEBUG("Victim not found in LRUReplacer.cpp");
    }
  }
  list_mutex.unlock();
  return victim_found;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  list_mutex.lock();
  if (lru_frame_list[frame_id] >= 0) {
    lru_frame_list[frame_id] = -1;
    track_size -= 1;
  } else {
    LOG_DEBUG("Pin frame %d repeatedly in LRUReplacer.cpp", frame_id);
  }
  list_mutex.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  list_mutex.lock();
  if (lru_frame_list[frame_id] == -1) {
    lru_frame_list[frame_id] = 1;
    track_size += 1;
    LOG_DEBUG("Unpin frame %d in LRUReplacer.cpp", frame_id);
  } else {
    LOG_DEBUG("Unpin frame %d repeatedly in LRUReplacer.cpp", frame_id);
  }
  list_mutex.unlock();
}

size_t LRUReplacer::Size() { return track_size; }

}  // namespace bustub
