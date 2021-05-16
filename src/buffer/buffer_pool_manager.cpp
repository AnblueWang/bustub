//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <list>
#include <unordered_map>

#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  LOG_DEBUG(" !!Action: in buffer_pool_manager.cpp FetchPageImpl");
  Page *result = nullptr;
  if (page_id != INVALID_PAGE_ID) {
    latch_.lock();
    frame_id_t replace_id = -1;
    if (page_table_.find(page_id) == page_table_.end()) {
      // 1.1 & 1.2
      if (!free_list_.empty()) {
        replace_id = free_list_.back();
        free_list_.pop_back();
      } else {
        bool find_victim = replacer_->Victim(&replace_id);
        if (!find_victim) {
          LOG_DEBUG("Victim not found in buffer_pool_manager.cpp FetchPageImpl");
        }
      }
      if (replace_id != -1) {
        // 2.
        if (pages_[replace_id].is_dirty_) {
          disk_manager_->WritePage(pages_[replace_id].GetPageId(), pages_[replace_id].GetData());
        }
        // 3. 4. read data from page_id to pages_
        page_table_[page_id] = replace_id;
        page_table_.erase(pages_[replace_id].page_id_);
        pages_[replace_id].ResetMemory();
        disk_manager_->ReadPage(page_id, pages_[replace_id].data_);
        pages_[replace_id].page_id_ = page_id;
      }
    }
    if (page_table_.find(page_id) != page_table_.end()) {
      replacer_->Pin(page_table_[page_id]);
      LOG_DEBUG("Page %d found in buffer_pool_manager.cpp FetchPageImpl", page_id);
      result = &pages_[page_table_[page_id]];
      result->pin_count_++;
    }
    latch_.unlock();
  }
  return result;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  bool result = false;
  latch_.lock();
  if (page_table_.find(page_id) != page_table_.end()) {
    Page *desti_page = &pages_[page_table_[page_id]];
    if (desti_page->pin_count_ > 0) {
      desti_page->pin_count_--;
      desti_page->is_dirty_ = is_dirty;
      result = true;
      if (desti_page->pin_count_ == 0) {
        replacer_->Unpin(page_table_[page_id]);
        LOG_DEBUG("Page %d unpined in buffer_pool_manager.cpp UnpinPageImpl", page_id);
      }
    }
  } else {
    LOG_DEBUG("Page %d not found in buffer_pool_manager.cpp UnpinPageImpl", page_id);
  }
  latch_.unlock();
  return result;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  bool result = false;
  latch_.lock();
  if (page_id != INVALID_PAGE_ID && page_table_.find(page_id) != page_table_.end()) {
    Page *desti_page = &pages_[page_table_[page_id]];
    disk_manager_->WritePage(page_id, desti_page->data_);
    desti_page->is_dirty_ = false;
    result = true;
  }
  latch_.unlock();
  return result;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  Page *result = nullptr;
  latch_.lock();
  frame_id_t replace_id = -1;
  if (!free_list_.empty()) {
    replace_id = free_list_.back();
    free_list_.pop_back();
  } else {
    bool find_victim = replacer_->Victim(&replace_id);
    if (!find_victim) {
      LOG_DEBUG("Victim not found in buffer_pool_manager.cpp NewPageImpl");
    } else {
      page_table_.erase(pages_[replace_id].page_id_);
      LOG_DEBUG("Remove victim page %d in frame %d in buffer_pool_manager.cpp NewPageImpl", pages_[replace_id].page_id_,
                replace_id);
    }
  }
  if (replace_id != -1) {
    // 3. 4.
    *page_id = disk_manager_->AllocatePage();
    page_table_[*page_id] = replace_id;
    pages_[replace_id].ResetMemory();
    pages_[replace_id].pin_count_ = 1;
    pages_[replace_id].page_id_ = *page_id;
    result = &pages_[replace_id];
  }

  latch_.unlock();
  return result;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  bool result = false;
  latch_.lock();
  if (page_id != INVALID_PAGE_ID && page_table_.find(page_id) != page_table_.end()) {
    Page *desti_page = &pages_[page_table_[page_id]];
    if (desti_page->pin_count_ == 0) {
      free_list_.emplace_back(page_table_[page_id]);
      page_table_.erase(page_id);
      desti_page->ResetMemory();
      disk_manager_->DeallocatePage(page_id);
      result = true;
    }
  } else {
    result = true;
  }
  latch_.unlock();
  return result;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  for (auto page_pair : page_table_) {
    FlushPageImpl(page_pair.first);
  }
}

}  // namespace bustub
