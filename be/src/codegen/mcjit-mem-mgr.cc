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
//
// Based on https://github.com/llvm/llvm-project/blob/llvmorg-5.0.1/llvm/lib/ExecutionEngine/SectionMemoryManager.cpp

#include <llvm/Support/MathExtras.h>
#include <llvm/Support/Process.h>

#include "mcjit-mem-mgr.h"
#include "common/logging.h"

namespace sys = llvm::sys;
using llvm::StringRef;

namespace impala {

static uintptr_t requiredPageSize(uintptr_t Size, uint32_t Alignment) {
  static const size_t PageSize = sys::Process::getPageSize();
  // Use the same calculation as allocateSection because we need to be able to satisfy it.
  uintptr_t RequiredSize = Alignment * ((Size + Alignment - 1)/Alignment + 1);
  // Round up to the nearest page size. Blocks must be page-aligned.
  return PageSize * ((RequiredSize + PageSize - 1)/PageSize);
}

static uintptr_t alignAddr(uintptr_t Addr, unsigned Alignment) {
  return (Addr + Alignment - 1) & ~(uintptr_t)(Alignment - 1);
}

void ImpalaMCJITMemoryManager::reserveAllocationSpace(
    uintptr_t CodeSize, uint32_t CodeAlign, uintptr_t RODataSize, uint32_t RODataAlign,
    uintptr_t RWDataSize, uint32_t RWDataAlign) {
  if (CodeSize == 0 && RODataSize == 0 && RWDataSize == 0) return;

  // Get space required for each section.
  CodeAlign = CodeAlign ? CodeAlign : 16;
  RODataAlign = RODataAlign ? RODataAlign : 16;
  RWDataAlign = RWDataAlign ? RWDataAlign : 16;
  uintptr_t RequiredCodeSize = requiredPageSize(CodeSize, CodeAlign);
  uintptr_t RequiredRODataSize = requiredPageSize(RODataSize, RODataAlign);
  uintptr_t RequiredRWDataSize = requiredPageSize(RWDataSize, RWDataAlign);
  uintptr_t RequiredSize = RequiredCodeSize + RequiredRODataSize + RequiredRWDataSize;

  std::error_code ec;
  sys::MemoryBlock MB = sys::Memory::allocateMappedMemory(RequiredSize, nullptr,
      sys::Memory::MF_READ | sys::Memory::MF_WRITE, ec);
  if (ec) {
    LOG(WARNING) << "Unable to pre-allocate JIT memory: " << ec;
    return;
  }
  // Request is page-aligned, so we should always get back exactly the request.
  DCHECK_EQ(MB.size(), RequiredSize);
  // CodeMem will arbitrarily own this MemoryBlock to handle cleanup.
  CodeMem.AllocatedMem.push_back(MB);
  uintptr_t Addr = (uintptr_t)MB.base();
  FreeMemBlock FreeMB;
  FreeMB.PendingPrefixIndex = (unsigned)-1;

  if (CodeSize > 0) {
    DCHECK_EQ(Addr, alignAddr(Addr, CodeAlign));
    FreeMB.Free = sys::MemoryBlock((void*)Addr, RequiredCodeSize);
    CodeMem.FreeMem.push_back(FreeMB);
    Addr += RequiredCodeSize;
  }

  if (RODataSize > 0) {
    DCHECK_EQ(Addr, alignAddr(Addr, RODataAlign));
    FreeMB.Free = sys::MemoryBlock((void*)Addr, RequiredRODataSize);
    RODataMem.FreeMem.push_back(FreeMB);
    Addr += RequiredRODataSize;
  }

  if (RWDataSize > 0) {
    DCHECK_EQ(Addr, alignAddr(Addr, RWDataAlign));
    FreeMB.Free = sys::MemoryBlock((void*)Addr, RequiredRWDataSize);
    RWDataMem.FreeMem.push_back(FreeMB);
  }
}

uint8_t *ImpalaMCJITMemoryManager::allocateDataSection(uintptr_t Size, unsigned Alignment,
    unsigned SectionID, StringRef SectionName, bool IsReadOnly) {
  if (IsReadOnly)
    return allocateSection(RODataMem, Size, Alignment);
  return allocateSection(RWDataMem, Size, Alignment);
}

uint8_t *ImpalaMCJITMemoryManager::allocateCodeSection(uintptr_t Size, unsigned Alignment,
    unsigned SectionID, StringRef SectionName) {
  return allocateSection(CodeMem, Size, Alignment);
}

uint8_t *ImpalaMCJITMemoryManager::allocateSection(MemoryGroup &MemGroup,
    uintptr_t Size, unsigned Alignment) {
  if (!Alignment)
    Alignment = 16;

  DCHECK(!(Alignment & (Alignment - 1)) && "Alignment must be a power of two.");

  uintptr_t RequiredSize = Alignment * ((Size + Alignment - 1)/Alignment + 1);
  uintptr_t Addr = 0;

  // Look in the list of free memory regions and use a block there if one
  // is available.
  for (FreeMemBlock &FreeMB : MemGroup.FreeMem) {
    if (FreeMB.Free.size() >= RequiredSize) {
      Addr = (uintptr_t)FreeMB.Free.base();
      uintptr_t EndOfBlock = Addr + FreeMB.Free.size();
      // Align the address.
      Addr = (Addr + Alignment - 1) & ~(uintptr_t)(Alignment - 1);

      if (FreeMB.PendingPrefixIndex == (unsigned)-1) {
        // The part of the block we're giving out to the user is now pending
        MemGroup.PendingMem.push_back(sys::MemoryBlock((void *)Addr, Size));

        // Remember this pending block, such that future allocations can just
        // modify it rather than creating a new one
        FreeMB.PendingPrefixIndex = MemGroup.PendingMem.size() - 1;
      } else {
        sys::MemoryBlock &PendingMB = MemGroup.PendingMem[FreeMB.PendingPrefixIndex];
        PendingMB = sys::MemoryBlock(PendingMB.base(), Addr + Size - (uintptr_t)PendingMB.base());
      }

      // Remember how much free space is now left in this block
      FreeMB.Free = sys::MemoryBlock((void *)(Addr + Size), EndOfBlock - Addr - Size);
      return (uint8_t*)Addr;
    }
  }

  DCHECK(!needsToReserveAllocationSpace())
      << "All memory should be pre-allocated: " << Size << "/" << Alignment;

  // No pre-allocated free block was large enough. Allocate a new memory region.
  // Note that all sections get allocated as read-write.  The permissions will
  // be updated later based on memory group.
  //
  // FIXME: It would be useful to define a default allocation size (or add
  // it as a constructor parameter) to minimize the number of allocations.
  std::error_code ec;
  sys::MemoryBlock MB = sys::Memory::allocateMappedMemory(RequiredSize,
                                                          &MemGroup.Near,
                                                          sys::Memory::MF_READ |
                                                            sys::Memory::MF_WRITE,
                                                          ec);
  if (ec) {
    // FIXME: Add error propagation to the interface.
    return nullptr;
  }

  // Save this address as the basis for our next request
  MemGroup.Near = MB;

  // https://github.com/llvm/llvm-project/commit/574713c3076c11a5677f554ab132d1324be9cb00
  // Copy the address to all the other groups, if they have not
  // been initialized.
  if (CodeMem.Near.base() == nullptr)
    CodeMem.Near = MB;
  if (RODataMem.Near.base() == nullptr)
    RODataMem.Near = MB;
  if (RWDataMem.Near.base() == nullptr)
    RWDataMem.Near = MB;

  // Remember that we allocated this memory
  MemGroup.AllocatedMem.push_back(MB);
  Addr = (uintptr_t)MB.base();
  uintptr_t EndOfBlock = Addr + MB.size();

  // Align the address.
  Addr = (Addr + Alignment - 1) & ~(uintptr_t)(Alignment - 1);

  // The part of the block we're giving out to the user is now pending
  MemGroup.PendingMem.push_back(sys::MemoryBlock((void *)Addr, Size));

  // The allocateMappedMemory may allocate much more memory than we need. In
  // this case, we store the unused memory as a free memory block.
  unsigned FreeSize = EndOfBlock-Addr-Size;
  if (FreeSize > 16) {
    FreeMemBlock FreeMB;
    FreeMB.Free = sys::MemoryBlock((void*)(Addr + Size), FreeSize);
    FreeMB.PendingPrefixIndex = (unsigned)-1;
    MemGroup.FreeMem.push_back(FreeMB);
  }

  // Return aligned address
  return (uint8_t*)Addr;
}

bool ImpalaMCJITMemoryManager::finalizeMemory(std::string *ErrMsg)
{
  // FIXME: Should in-progress permissions be reverted if an error occurs?
  std::error_code ec;

  // Make code memory executable.
  ec = applyMemoryGroupPermissions(CodeMem,
                                   sys::Memory::MF_READ | sys::Memory::MF_EXEC);
  if (ec) {
    if (ErrMsg) {
      *ErrMsg = ec.message();
    }
    return true;
  }

  // Make read-only data memory read-only.
  // https://github.com/llvm/llvm-project/commit/ba0e71432a60e1fa2da9e098cbc574a1d9b9618b
  ec = applyMemoryGroupPermissions(RODataMem, sys::Memory::MF_READ);
  if (ec) {
    if (ErrMsg) {
      *ErrMsg = ec.message();
    }
    return true;
  }

  // Read-write data memory already has the correct permissions

  // Some platforms with separate data cache and instruction cache require
  // explicit cache flush, otherwise JIT code manipulations (like resolved
  // relocations) will get to the data cache but not to the instruction cache.
  invalidateInstructionCache();

  return false;
}

static sys::MemoryBlock trimBlockToPageSize(sys::MemoryBlock M) {
  static const size_t PageSize = sys::Process::getPageSize();

  size_t StartOverlap =
      (PageSize - ((uintptr_t)M.base() % PageSize)) % PageSize;

  size_t TrimmedSize = M.size();
  TrimmedSize -= StartOverlap;
  TrimmedSize -= TrimmedSize % PageSize;

  sys::MemoryBlock Trimmed((void *)((uintptr_t)M.base() + StartOverlap), TrimmedSize);

  DCHECK(((uintptr_t)Trimmed.base() % PageSize) == 0);
  DCHECK((Trimmed.size() % PageSize) == 0);
  DCHECK(M.base() <= Trimmed.base() && Trimmed.size() <= M.size());

  return Trimmed;
}


std::error_code
ImpalaMCJITMemoryManager::applyMemoryGroupPermissions(MemoryGroup &MemGroup,
                                                  unsigned Permissions) {
  for (sys::MemoryBlock &MB : MemGroup.PendingMem)
    if (std::error_code EC = sys::Memory::protectMappedMemory(MB, Permissions))
      return EC;

  MemGroup.PendingMem.clear();

  // Now go through free blocks and trim any of them that don't span the entire
  // page because one of the pending blocks may have overlapped it.
  for (FreeMemBlock &FreeMB : MemGroup.FreeMem) {
    FreeMB.Free = trimBlockToPageSize(FreeMB.Free);
    // We cleared the PendingMem list, so all these pointers are now invalid
    FreeMB.PendingPrefixIndex = (unsigned)-1;
  }

  // Remove all blocks which are now empty
  MemGroup.FreeMem.erase(
      remove_if(MemGroup.FreeMem,
                [](FreeMemBlock &FreeMB) { return FreeMB.Free.size() == 0; }),
      MemGroup.FreeMem.end());

  return std::error_code();
}

void ImpalaMCJITMemoryManager::invalidateInstructionCache() {
  for (sys::MemoryBlock &Block : CodeMem.PendingMem)
    sys::Memory::InvalidateInstructionCache(Block.base(), Block.size());
}

int64_t ImpalaMCJITMemoryManager::bytes_allocated() const {
  int64_t total = 0;
  for (const MemoryGroup *Group : {&CodeMem, &RWDataMem, &RODataMem}) {
    for (const sys::MemoryBlock &Block : Group->AllocatedMem)
      total += Block.size();
  }
  return total;
}

void ImpalaMCJITMemoryManager::set_bytes_tracked(int64_t bytes_tracked) {
  DCHECK_LE(bytes_tracked, bytes_allocated());
  bytes_tracked_ = bytes_tracked;
}

ImpalaMCJITMemoryManager::~ImpalaMCJITMemoryManager() {
  for (MemoryGroup *Group : {&CodeMem, &RWDataMem, &RODataMem}) {
    for (sys::MemoryBlock &Block : Group->AllocatedMem)
      sys::Memory::releaseMappedMemory(Block);
  }
}

} // namespace llvm
