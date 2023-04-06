/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#ifndef _BLOCK_ALLOCATOR_H_
#define _BLOCK_ALLOCATOR_H_ 1

#include <new>
#include <iostream>
#include <string>
#include <vector>
#include <stdlib.h>
#include <stddef.h>

/*
 * The code in this file provides a simple memory allocator for each tablet.
 * This allocator has the following goals.
 *
 *  - Avoid calling the system allocator for each key,value inserted.
 *  - Avoid interleaving key values of different tablets in memory.  This
 *    avoids fragmenting memory.  Tablets slowly allocate memory over time and
 *    then free it all at once.
 *  - Support quick deallocation of a tablets memory when its minor
 *    compacted/flushed.  Want to avoid deallocating each key/value individually.
 *
 * These goals are achieved by allocating 128K blocks from the system.
 * Individual key values are allocated from these 128K blocks.  The allocator
 * keeps a list of these 128K blocks and deallocates them all when needed.
 * Large key values are allocated directly from the system.  This strategy
 * avoids interleaving key/values from different tablets in memory and supports
 * fast de-allocation.
 *
 * This allocator does not support deallocation, except in the special
 * circumstance of deallocating the last thing added.  This supports the case
 * of allocating a key to see if it already exist in the map and then
 * deallocating it when its found to exist.
 *
 * This allocator is not thread safe.
 */

struct Block {
  unsigned char *data;
  unsigned char *currentPos;
  unsigned char *end;
  unsigned char *prevPos;

  Block(uint32_t size){
    data = new unsigned char[size];
    end = data + size;
    currentPos = data;
    prevPos = NULL;
  }

  ~Block(){
  }

  void *allocate(size_t amount){
    unsigned char *nextPos = currentPos + amount;

    if(nextPos > end){
      return NULL;
    }

    prevPos = currentPos;
    currentPos = nextPos;
    return prevPos;
  }

  size_t rollback(void *p){
    if(p == prevPos){
      size_t diff = currentPos - prevPos;
      currentPos = prevPos;
      return diff;
    }else{
      std::cerr << "Tried to delete something that was not previous allocation " << p << " " << prevPos << std::endl;
      exit(-1);
    }

    return 0;
  }

  size_t getMemoryFree() {
    return end - currentPos;
  }
};

struct BigBlock {
  unsigned char *ptr;
  size_t length;

  BigBlock(unsigned char *p, size_t len):ptr(p),length(len){}
};

struct LinkedBlockAllocator {

  std::vector<Block> blocks;
  std::vector<BigBlock> bigBlocks;
  int blockSize;
  int bigBlockSize;
  int64_t memused;
  void *lastAlloc;

  LinkedBlockAllocator(int blockSize, int bigBlockSize){
    this->blockSize = blockSize;
    this->bigBlockSize = bigBlockSize;
    lastAlloc = NULL;
    memused = 0;
  }

  void *allocate(size_t amount){
    if(amount > (size_t)bigBlockSize){
      unsigned char *p = new unsigned char[amount];
      bigBlocks.push_back(BigBlock(p, amount));
      memused += sizeof(BigBlock) + amount;
      return p;
    }else{
      if(blocks.size() == 0){
        //do lazy allocation of memory, do not allocate a block until it is used
        blocks.push_back(Block(blockSize));
        memused += sizeof(Block) + blockSize;
      }

      lastAlloc = blocks.back().allocate(amount);
      if(lastAlloc == NULL){
        blocks.push_back(Block(blockSize));
        lastAlloc = blocks.back().allocate(amount);
        memused += sizeof(Block) + blockSize;
      }

      return lastAlloc;
    }
  }

  void deleteLast(void *p){
    if(p != NULL){
      if(p == lastAlloc){
        blocks.back().rollback(p);
        lastAlloc = NULL;
        return;
      }else if(!bigBlocks.empty() && bigBlocks.back().ptr == p){
        memused -= (sizeof(BigBlock) + bigBlocks.back().length);
        bigBlocks.pop_back();
        delete((unsigned char *)p);
        return;
      }
    }

    std::cerr << "Tried to delete something that was not last allocation " << p << " " << lastAlloc << std::endl;
    exit(-1);
  }

  size_t getMemoryUsed(){
    if(blocks.size() == 0)
      return memused;
    else
      return memused - blocks.back().getMemoryFree();
  }

  ~LinkedBlockAllocator(){
    //std::cout << "Deleting " << blocks.size() << " blocks, memused : " << memused << std::endl;
    std::vector<Block>::iterator iter = blocks.begin();
    while(iter != blocks.end()){
      delete [] (iter->data);
      iter++;
    }

    std::vector<BigBlock>::iterator iter2 = bigBlocks.begin();
    while(iter2 != bigBlocks.end()){
      delete [] (iter2->ptr);
      iter2++;
    }
  }

};


/**
 *  @brief  An allocator that uses global new, as per [20.4].
 *
 *  This is precisely the allocator defined in the C++ Standard.
 *    - all allocation calls operator new
 *    - all deallocation calls operator delete
 */
template<typename _Tp>
class BlockAllocator
{
  public:
    typedef size_t     size_type;
    typedef ptrdiff_t  difference_type;
    typedef _Tp*       pointer;
    typedef const _Tp* const_pointer;
    typedef _Tp&       reference;
    typedef const _Tp& const_reference;
    typedef _Tp        value_type;

    LinkedBlockAllocator *lba;

    template<typename _Tp1>
      struct rebind
      { typedef BlockAllocator<_Tp1> other; };

    BlockAllocator() throw() {
      lba = NULL;
    }

    BlockAllocator(LinkedBlockAllocator *lba) throw() {
      this->lba = lba;
    }

    BlockAllocator(const BlockAllocator& ba) throw() {
      lba = ba.lba;
    }

    template<typename _Tp1>
      BlockAllocator(const BlockAllocator<_Tp1>& ba) throw() {
        lba = ba.lba;
      }

    ~BlockAllocator() throw() { }

    pointer
      address(reference __x) const { return &__x; }

    const_pointer
      address(const_reference __x) const { return &__x; }

    // NB: __n is permitted to be 0.  The C++ standard says nothing
    // about what the return value is when __n == 0.
    pointer
      allocate(size_type __n, const void* = 0)
      {
        if (__builtin_expect(__n > this->max_size(), false))
          std::__throw_bad_alloc();


        //void *p = ::operator new(__n * sizeof(_Tp));
        void *p = lba->allocate(__n * sizeof(_Tp));

        //std::cout << "Allocating "<< name <<" " << __n * sizeof(_Tp) << " "  << ((unsigned long long)p) % 4 << " " << ((unsigned long long)p) % 8 << std::endl;

        return static_cast<_Tp*>(p);
      }

    // __p is not permitted to be a null pointer.
    void
      deallocate(pointer __p, size_type)
      {
        //::operator delete(__p);
      }

    size_type
      max_size() const throw()
      { return size_t(-1) / sizeof(_Tp); }

    // _GLIBCXX_RESOLVE_LIB_DEFECTS
    // 402. wrong new expression in [some_] allocator::construct
    void
      construct(pointer __p, const _Tp& __val)
      { ::new(__p) _Tp(__val); }

    void
      destroy(pointer __p) { __p->~_Tp(); }


};


template<typename _Tp>
  inline bool
operator==(const BlockAllocator<_Tp>& ba1, const BlockAllocator<_Tp>& ba2)
{ return true; }

template<typename _Tp>
  inline bool
operator!=(const BlockAllocator<_Tp>& ba1, const BlockAllocator<_Tp>& ba2)
{ return false; }

#endif
