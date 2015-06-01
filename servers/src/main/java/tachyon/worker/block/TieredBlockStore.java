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
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
import tachyon.worker.block.evictor.NaiveEvictor;
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
public class TieredBlockStore implements BlockStore<int> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final TachyonConf mTachyonConf;
  private final BlockMetadataManager mMetaManager;
  private final BlockLockManager mLockManager;
  private final Allocator mAllocator;
  private final Evictor mEvictor;

  /** A readwrite lock for meta data **/
  private final ReentrantReadWriteLock mEvictionLock = new ReentrantReadWriteLock();

  public TieredBlockStore() {
    mTachyonConf = new TachyonConf();
    mMetaManager = new BlockMetadataManager(mTachyonConf);
    mLockManager = new BlockLockManager();

    // TODO: create Allocator according to tachyonConf.
    mAllocator = new NaiveAllocator(mMetaManager);
    // TODO: create Evictor according to tachyonConf
    mEvictor = new NaiveEvictor(mMetaManager);
  }

  @Override
  public Optional<Long> lockBlock(long userId, long blockId, BlockLockType blockLockType) {
    Lock blockLock;
    switch (blockLockType) {
      case READ:
        blockLock = mLockManager.getBlockReadLock(blockId);
        break;
      case WRITE:
        blockLock = mLockManager.getBlockWriteLock(blockId);
        break;
      default:
        LOG.error("Unsupported lock type %s", blockLockType);
        return Optional.absent();
    }
    mEvictionLock.readLock().lock();
    blockLock.lock();
    return Optional.of(blockLock.);
  }

  @Override
  public boolean unlockBlock(long blockId, BlockLockType blockLockType) {
    Lock blockLock;
    switch (blockLockType) {
      case READ:
        blockLock = mLockManager.getBlockReadLock(blockId);
        break;
      case WRITE:
        blockLock = mLockManager.getBlockWriteLock(blockId);
        break;
      default:
        LOG.error("Unsupported lock type %s", blockLockType);
        return false;
    }
    blockLock.unlock();
    mEvictionLock.readLock().unlock();
    return true;
  }

  @Override
  public Optional<String> getBlockFilePath(long userId, long blockId, long lockId) {
    Optional<BlockMeta> optionalBlock = mMetaManager.getBlockMeta(blockId);
    if (!optionalBlock.isPresent()) {
      return Optional.absent();
    }
    return Optional.of(optionalBlock.get().getPath());
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
  @Override
  public boolean createBlock(long userId, long blockId, ByteBuffer buf, int tierHint)
      throws IOException {
    Preconditions.checkNotNull(buf);
    mEvictor.preCreateBlock(userId, blockId, tierHint);

    mEvictionLock.writeLock().lock();
    boolean result = createBlockNoLock(userId, blockId, buf, tierHint);
    mEvictionLock.writeLock().unlock();

    mEvictor.postCreateBlock(userId, blockId, tierHint);
    return result;
  }

  private boolean createBlockNoLock(long userId, long blockId, ByteBuffer buf, int tierHint)
      throws IOException {
    if (!mLockManager.addBlockLock(blockId)) {
      LOG.error("Cannot add block lock of {}", blockId);
      return false;
    }

    long blockSize = buf.limit();
    Optional<BlockMeta> optionalBlock =
        mAllocator.allocateBlock(userId, blockId, blockSize, tierHint);
    if (!optionalBlock.isPresent()) {
      // Not enough space in this block store, let's try to free some space.
      if (freeSpaceNoLock(userId, blockSize, tierHint)) {
        LOG.error("Cannot free space of {} bytes", blockSize);
        return false;
      }
      // Try again
      optionalBlock = mAllocator.allocateBlock(userId, blockId, blockSize, tierHint);
      Preconditions.checkState(optionalBlock.isPresent(), "Cannot create block {}:", blockId);
    }
    BlockMeta block = optionalBlock.get();
    if (!addBlockMetaNoLock(userId, block, tierHint)) {
      return false;
    }
    BlockFileOperator operator = new BlockFileOperator(block);
    long bytes = operator.write(0, buf);
    return bytes == buf.limit();
  }

  /**
   * Read data from an existing block at a specific offset and length.
   *
   * @param userId the user ID
   * @param blockId the block ID
   * @param offset offset of the data to read in bytes
   * @param length length of the data to read in bytes
   * @return a ByteBuffer containing data read or absent
   * @throws IOException
   */
  @Override
  public Optional<ByteBuffer> readBlock(long userId, long blockId, long offset, long length,
      int tierHint)
      throws IOException {
    mEvictor.preReadBlock(userId, blockId, offset, length);
    Lock blockReadLock = mLockManager.getBlockReadLock(blockId);

    mEvictionLock.readLock().lock();
    blockReadLock.lock();
    Optional<ByteBuffer> result = readBlockNoLock(userId, blockId, offset, length);
    blockReadLock.unlock();
    mEvictionLock.readLock().unlock();

    mEvictor.postReadBlock(userId, blockId, offset, length);
    return result;
  }

  private Optional<ByteBuffer> readBlockNoLock(long userId, long blockId, long offset, long length)
      throws IOException {
    Optional<BlockMeta> optionalBlock = mMetaManager.getBlockMeta(blockId);
    if (!optionalBlock.isPresent()) {
      return Optional.absent();
    }
    BlockMeta block = optionalBlock.get();
    BlockFileOperator operator = new BlockFileOperator(block);
    return Optional.of(operator.read(offset, length));
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
  @Override
  public boolean relocateBlock(long userId, long blockId, int newTierHint) throws IOException {
    mEvictor.preMoveBlock(userId, blockId, newTierHint);
    Lock blockWriteLock = mLockManager.getBlockWriteLock(blockId);

    mEvictionLock.readLock().lock();
    blockWriteLock.lock();
    boolean result = relocateBlockNoLock(userId, blockId, newTierHint);
    blockWriteLock.unlock();
    mEvictionLock.readLock().unlock();

    mEvictor.postMoveBlock(userId, blockId, newTierHint);
    return result;
  }


  private boolean relocateBlockNoLock(long userId, long blockId, int newTierHint) throws IOException {
    Optional<BlockMeta> optionalSrcBlock = mMetaManager.getBlockMeta(userId);
    if (!optionalSrcBlock.isPresent()) {
      return false;
    }
    Optional<BlockMeta> optionalDstBlock = mMetaManager.moveBlockMeta(userId, blockId, newTierHint);
    if (!optionalDstBlock.isPresent()) {
      return false;
    }
    BlockMeta srcBlock = optionalSrcBlock.get();
    BlockMeta dstBlock = optionalDstBlock.get();
    BlockFileOperator operator = new BlockFileOperator(srcBlock);
    return operator.move(dstBlock.getPath());
  }


  /**
   * Remove a block.
   *
   * @param userId the user ID
   * @param blockId the block ID
   * @return true if successful, false otherwise.
   * @throws FileNotFoundException
   */
  @Override
  public boolean removeBlock(long userId, long blockId) throws FileNotFoundException {
    mEvictor.preRemoveBlock(userId, blockId);
    Lock blockWriteLock = mLockManager.getBlockWriteLock(blockId);

    mEvictionLock.readLock().lock();
    blockWriteLock.lock();
    boolean result = removeBlockNoLock(userId, blockId);
    blockWriteLock.unlock();
    mEvictionLock.readLock().unlock();

    Preconditions.checkState(mLockManager.removeBlockLock(blockId));
    mEvictor.postRemoveBlock(userId, blockId);
    return result;
  }

  private boolean removeBlockNoLock(long userId, long blockId) throws FileNotFoundException {
    Optional<BlockMeta> optionalBlock = mMetaManager.getBlockMeta(blockId);
    if (!optionalBlock.isPresent()) {
      return false;
    }
    BlockMeta block = optionalBlock.get();
    if (!block.isCheckpointed()) {
      LOG.error("Cannot free block {}: not checkpointed", blockId);
      return false;
    }

    // Step1: delete metadata of the block
    if (!mMetaManager.removeBlockMeta(blockId)) {
      return false;
    }
    // Step2: delete the data file of the block
    BlockFileOperator operator = new BlockFileOperator(block);
    return operator.delete();
  }

  /**
   * Free a certain amount of space
   *
   * @param userId the user ID
   * @param bytes the space to free in bytes
   * @param tierHint which tier to free
   * @return true if success, false otherwise
   * @throws IOException
   */
  @Override
  public boolean freeSpace(long userId, long bytes, int tierHint) throws IOException {
    mEvictionLock.writeLock().lock();
    boolean result = freeSpaceNoLock(userId, bytes, tierHint);
    mEvictionLock.writeLock().unlock();
    return result;
  }

  private boolean freeSpaceNoLock(long userId, long bytes, int tierHint) throws IOException {
    EvictionPlan plan = mEvictor.freeSpace(bytes, tierHint);
    // Step1: remove blocks to make room.
    for (long blockId : plan.toEvict()) {
      if (!removeBlockNoLock(userId, blockId)) {
        return false;
      }
    }
    // Step2: transfer blocks among tiers.
    for (Pair<Long, Integer> entry : plan.toTransfer()) {
      long blockId = entry.getFirst();
      int tierAlias = entry.getSecond();
      if (!relocateBlockNoLock(userId, blockId, tierAlias)) {
        return false;
      }
    }
    return true;
  }
}
