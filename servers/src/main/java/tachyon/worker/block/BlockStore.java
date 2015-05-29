/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker.block;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.conf.TachyonConf;
import tachyon.worker.BlockLockManager;
import tachyon.worker.block.allocator.Allocator;
import tachyon.worker.block.allocator.NaiveAllocator;
import tachyon.worker.block.evictor.EvictionPlan;
import tachyon.worker.block.evictor.Evictor;
import tachyon.worker.block.evictor.RandomEvictor;
import tachyon.worker.block.meta.BlockMeta;

/**
 * This class represents an object store that manages all the blocks in the local tiered storage.
 * This store exposes simple public APIs to operate blocks. Inside this store, it creates an
 * Allocator to decide where to put a new block, an Evictor to decide where to evict a stale block,
 * a BlockMetadataManager to maintain the status of the tiered storage, and a LockManager to
 * coordinate read/write on the same block.
 * <p>
 * This class is thread-safe.
 */
public class BlockStore {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final TachyonConf mTachyonConf;
  private final BlockMetadataManager mMetaManager;
  private final BlockLockManager mLockManager;
  private final Allocator mAllocator;
  private final Evictor mEvictor;

  public BlockStore() {
    mTachyonConf = new TachyonConf();
    mMetaManager = new BlockMetadataManager(mTachyonConf);
    mLockManager = new BlockLockManager();

    // TODO: create Allocator according to tachyonConf.
    mAllocator = new NaiveAllocator(mMetaManager);
    // TODO: create Evictor according to tachyonConf
    mEvictor = new RandomEvictor(mMetaManager);
  }

  /**
   * Get the metadata of the specific block. This method assumes the corresponding {@link BlockLock}
   * has been acquired.
   * <p>
   * This method is a wrapper around MetaManager.getBlockMeta() to provide public access to it.
   *
   * @param userId the user ID
   * @param blockId the block ID
   * @return the block metadata of this block id or absent if not found.
   */
  public Optional<BlockMeta> getBlockMetaNoLock(long userId, long blockId) {
    return mMetaManager.getBlockMeta(blockId);
  }

  /**
   * Remove the metadata of the specific block. This method assumes the corresponding
   * {@link BlockLock} has been acquired.
   * <p>
   * This method is a wrapper around MetaManager.removeBlockMeta() to provide public access to it.
   *
   * @param userId the user ID
   * @param blockId the id of the block
   * @return the block metadata of this block id or absent if not found.
   */
  public boolean removeBlockMetaNoLock(long userId, long blockId) {
    return mMetaManager.removeBlockMeta(blockId);
  }

  /**
   * Create the metadata of a new block. This method assumes the corresponding {@link BlockLock} has
   * been acquired.
   * <p>
   * This method is a wrapper around MetaManager.createBlockMeta() to provide public access to it.
   *
   * @param userId the user ID
   * @param blockId the block ID
   * @param blockSize block size in bytes
   * @param tierHint which tier to create this block
   * @return the newly created block metadata or absent on creation failure.
   */
  public Optional<BlockMeta> createBlockMetaNoLock(long userId, long blockId, long blockSize,
      int tierHint) {
    return mMetaManager.createBlockMeta(userId, blockId, blockSize, tierHint);
  }

  /**
   * Create a new block with data from a ByteBuffer.
   *
   * @param userId the user ID
   * @param blockId the block ID
   * @param buf the input buffer
   * @param tierHint which tier to put this block
   * @return true if success, false otherwise
   * @throws IOException
   */
  public boolean createBlock(long userId, long blockId, ByteBuffer buf, int tierHint)
      throws IOException {
    Preconditions.checkNotNull(buf);
    mEvictor.preCreateBlock(userId, blockId, tierHint);
    boolean result = createBlockInternal(userId, blockId, buf, tierHint);
    mEvictor.postCreateBlock(userId, blockId, tierHint);
    return result;
  }

  private boolean createBlockInternal(long userId, long blockId, ByteBuffer buf, int tierHint)
      throws IOException {
    if (!mLockManager.addBlockLock(blockId)) {
      return false;
    }
    Lock blockWriteLock = mLockManager.getBlockReadLock(blockId);
    Lock metaWriteLock = mLockManager.getMetaReadLock();


    // long blockSize = buf.limit();
    // synchronized (mMetaManager) {
    // Optional<BlockMeta> optionalBlock =
    // mAllocator.allocateBlock(userId, blockId, blockSize, tierHint);
    // if (!optionalBlock.isPresent()) {
    // mEvictor.freeSpace(blockSize, tierHint);
    // optionalBlock = mAllocator.allocateBlock(userId, blockId, blockSize, tierHint);
    // if (!optionalBlock.isPresent()) {
    // LOG.error("Cannot create block {}:", blockId);
    // return false;
    // }
    // }
    //
    // if (!freeSpace(userId, blockSize, tierHint)) {
    // unlockBlock(userId, blockId);
    // return false;
    // }
    // }

    blockWriteLock.lock();
    metaWriteLock.lock();
    Optional<BlockMeta> optionalBlock =
        createBlockMetaNoLock(userId, blockId, buf.limit(), tierHint);
    metaWriteLock.unlock();

    if (!optionalBlock.isPresent()) {
      blockWriteLock.unlock();
      return false;
    }
    BlockMeta block = optionalBlock.get();
    BlockFileOperator operator = new BlockFileOperator(block);
    long bytes = operator.write(0, buf);
    blockWriteLock.unlock();
    return bytes == buf.limit();
  }

  /**
   * Read from an existing block at the specific offset and length.
   *
   * @param userId the user ID
   * @param blockId the block ID
   * @param offset offset of the data to read in bytes
   * @param length length of the data to read in bytes
   * @return a ByteBuffer containing data read or absent
   * @throws IOException
   */
  public Optional<ByteBuffer> readBlock(long userId, long blockId, long offset, long length)
      throws IOException {
    mEvictor.preReadBlock(userId, blockId, offset, length);
    Optional<ByteBuffer> result = readBlockInternal(userId, blockId, offset, length);
    mEvictor.postReadBlock(userId, blockId, offset, length);
    return result;
  }

  private Optional<ByteBuffer> readBlockInternal(long userId, long blockId, long offset, long length)
      throws IOException {
    Lock blockReadLock = mLockManager.getBlockReadLock(blockId);
    Lock metaReadLock = mLockManager.getMetaReadLock();

    blockReadLock.lock();
    metaReadLock.lock();
    Optional<BlockMeta> optionalBlock = getBlockMetaNoLock(userId, blockId);
    metaReadLock.unlock();
    if (!optionalBlock.isPresent()) {
      blockReadLock.unlock();
      return Optional.absent();
    }
    BlockMeta block = optionalBlock.get();
    BlockFileOperator operator = new BlockFileOperator(block);
    ByteBuffer buf = operator.read(offset, length);
    blockReadLock.unlock();
    return Optional.of(buf);
  }

  /**
   * move an existing block to a different tier.
   *
   * @param userId the user ID
   * @param blockId the block ID
   * @param newTierHint dest tier to move
   * @return true if success, false otherwise
   * @throws IOException
   */
  public boolean moveBlock(long userId, long blockId, int newTierHint) throws IOException {
    mEvictor.preMoveBlock(userId, blockId, newTierHint);
    boolean result = moveBlockInternal(userId, blockId, newTierHint);
    mEvictor.postMoveBlock(userId, blockId, newTierHint);
    return result;
  }


  private boolean moveBlockInternal(long userId, long blockId, int newTierHint) throws IOException {
    Lock blockWriteLock = mLockManager.getBlockWriteLock(blockId);
    Lock metaWriteLock = mLockManager.getMetaWriteLock();

    blockWriteLock.lock();
    metaWriteLock.lock();
    Optional<BlockMeta> optionalSrcBlock = getBlockMetaNoLock(userId, blockId);
    if (!optionalSrcBlock.isPresent()) {
      metaWriteLock.unlock();
      blockWriteLock.unlock();
      return false;
    }
    Optional<BlockMeta> optionalDstBlock = moveBlockMetaNoLock(blockId, newTierHint);
    metaWriteLock.unlock();
    if (!optionalDstBlock.isPresent()) {
      blockWriteLock.unlock();
      return false;
    }
    BlockMeta srcBlock = optionalSrcBlock.get();
    BlockMeta dstBlock = optionalDstBlock.get();
    BlockFileOperator operator = new BlockFileOperator(srcBlock);
    boolean done = operator.move(dstBlock.getPath());
    blockWriteLock.unlock();
    return done;
  }


  /**
   * Remove a block.
   *
   * @param userId the user ID
   * @param blockId the block ID
   * @return true if successful, false otherwise.
   * @throws FileNotFoundException
   */
  public boolean removeBlock(long userId, long blockId) throws FileNotFoundException {
    mEvictor.preRemoveBlock(userId, blockId);
    boolean result = removeBlockInternal(userId, blockId);
    mEvictor.postRemoveBlock(userId, blockId);
    return result;
  }

  private boolean removeBlockInternal(long userId, long blockId) throws FileNotFoundException {
    Lock blockWriteLock = mLockManager.getBlockWriteLock(blockId);
    Lock metaWriteLock = mLockManager.getMetaWriteLock();

    blockWriteLock.lock();
    metaWriteLock.lock();

    Optional<BlockMeta> optionalBlock = getBlockMetaNoLock(userId, blockId);
    if (!optionalBlock.isPresent()) {
      metaWriteLock.unlock();
      blockWriteLock.unlock();
      return false;
    }
    BlockMeta block = optionalBlock.get();
    if (!block.isCheckpointed()) {
      LOG.error("Cannot free block {}: not checkpointed", blockId);
      metaWriteLock.unlock();
      blockWriteLock.unlock();
      return false;
    }
    boolean metaRemoved = removeBlockMetaNoLock(userId, blockId);
    metaWriteLock.unlock();
    // Step1: delete metadata of the block
    if (!metaRemoved) {
      blockWriteLock.unlock();
      return false;
    }
    // Step2: delete the data file of the block
    BlockFileOperator operator = new BlockFileOperator(block);
    boolean done = operator.delete();
    Preconditions.checkState(mLockManager.removeBlockLock(blockId));

    blockWriteLock.unlock();
    return done;
  }

  public boolean freeSpace(long userId, long bytes, int tierHint) throws IOException {
    Lock metaReadLock = mLockManager.getMetaReadLock();
    metaReadLock.lock();
    EvictionPlan plan = mEvictor.freeSpace(bytes, tierHint);
    metaReadLock.unlock();

    if (!executeEvictionPlan(userId, plan)) {

    }
  }

  /**
   * Execute an eviction plan proposed by Evictor.
   *
   * @param userId the user ID
   * @param plan How to make space
   * @return true if success, false otherwise
   * @throws IOException
   */
  private boolean executeEvictionPlan(long userId, EvictionPlan plan) throws IOException {
    // Step1: remove blocks to make room.
    for (long blockId : plan.toEvict()) {
      if (!removeBlockInternal(userId, blockId)) {
        return false;
      }
    }
    // Step2: transfer blocks among tiers.
    for (Pair<Long, Integer> entry : plan.toTransfer()) {
      long blockId = entry.getFirst();
      int tierAlias = entry.getSecond();
      if (!moveBlockInternal(userId, blockId, tierAlias)) {
        return false;
      }
    }
    return true;
  }
}
