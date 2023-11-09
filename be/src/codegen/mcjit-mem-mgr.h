// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#ifndef IMPALA_CODEGEN_MCJIT_MEM_MGR_H
#define IMPALA_CODEGEN_MCJIT_MEM_MGR_H

#include <llvm/ADT/SmallVector.h>
#include <llvm/ADT/StringRef.h>
#include <llvm/ExecutionEngine/RTDyldMemoryManager.h>
#include <llvm/Support/Memory.h>
#include <cstdint>
#include <string>
#include <system_error>

extern void *__dso_handle __attribute__ ((__visibility__ ("hidden")));

namespace impala {

/// Custom memory manager. It is needed for a couple of purposes.
///
/// We implement reserveAllocationSpace and pre-allocate all memory in a single block.
/// This is required for ARM where ADRP instructions have a limit of 4GB offsets. Large
/// memory systems may allocate sections further apart than this unless we pre-allocate.
///
/// We use it as a way to resolve references to __dso_handle in cross-compiled IR.
/// This uses the same approach as the legacy llvm JIT to handle __dso_handle. MCJIT
/// doesn't handle those for us: see LLVM issue 18062.
/// TODO: get rid of this by purging the cross-compiled IR of references to __dso_handle,
/// which come from global variables with destructors.
///
/// We also use it to track how much memory is allocated for compiled code.
class ImpalaMCJITMemoryManager : public llvm::RTDyldMemoryManager {
 public:
  ImpalaMCJITMemoryManager() = default;
  ImpalaMCJITMemoryManager(const ImpalaMCJITMemoryManager&) = delete;
  void operator=(const ImpalaMCJITMemoryManager&) = delete;
  ~ImpalaMCJITMemoryManager() override;

  bool needsToReserveAllocationSpace() override { return true; }

  void reserveAllocationSpace(uintptr_t CodeSize, uint32_t CodeAlign,
      uintptr_t RODataSize, uint32_t RODataAlign,
      uintptr_t RWDataSize, uint32_t RWDataAlign) override;

  virtual uint64_t getSymbolAddress(const std::string& name) override {
    if (name == "__dso_handle") return reinterpret_cast<uint64_t>(&__dso_handle);
    return llvm::RTDyldMemoryManager::getSymbolAddress(name);
  }

  virtual uint8_t* allocateCodeSection(uintptr_t size, unsigned alignment,
      unsigned section_id, llvm::StringRef section_name) override;

  virtual uint8_t* allocateDataSection(uintptr_t size, unsigned alignment,
      unsigned section_id, llvm::StringRef section_name, bool is_read_only) override;

  bool finalizeMemory(std::string *ErrMsg = nullptr) override;

  /// This method is called from finalizeMemory.
  virtual void invalidateInstructionCache();

  int64_t bytes_allocated() const;

  int64_t bytes_tracked() const { return bytes_tracked_; }

  void set_bytes_tracked(int64_t bytes_tracked);

 private:
  struct FreeMemBlock {
    // The actual block of free memory
    llvm::sys::MemoryBlock Free;
    // If there is a pending allocation from the same reservation right before
    // this block, store it's index in PendingMem, to be able to update the
    // pending region if part of this block is allocated, rather than having to
    // create a new one
    unsigned PendingPrefixIndex;
  };

  struct MemoryGroup {
    // PendingMem contains all blocks of memory (subblocks of AllocatedMem)
    // which have not yet had their permissions applied, but have been given
    // out to the user. FreeMem contains all block of memory, which have
    // neither had their permissions applied, nor been given out to the user.
    llvm::SmallVector<llvm::sys::MemoryBlock, 16> PendingMem;
    llvm::SmallVector<FreeMemBlock, 16> FreeMem;

    // All memory blocks that have been requested from the system
    llvm::SmallVector<llvm::sys::MemoryBlock, 16> AllocatedMem;

    llvm::sys::MemoryBlock Near;
  };

  uint8_t *allocateSection(MemoryGroup &MemGroup, uintptr_t Size,
                           unsigned Alignment);

  std::error_code applyMemoryGroupPermissions(MemoryGroup &MemGroup,
                                              unsigned Permissions);

  MemoryGroup CodeMem;
  MemoryGroup RWDataMem;
  MemoryGroup RODataMem;

  /// Total bytes already tracked by MemTrackers. <= 'bytes_allocated()'.
  /// Needed to release the correct amount from the MemTracker when done.
  int64_t bytes_tracked_ = 0;
};
}

#endif
