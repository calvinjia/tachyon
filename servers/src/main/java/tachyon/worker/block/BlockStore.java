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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.worker.BlockLockManager;
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

  public BlockStore() {
    mTachyonConf = new TachyonConf();
    mMetaManager = new BlockMetadataManager(mTachyonConf);
    mLockManager = new BlockLockManager();
  }

  /**
   * Lock a specific block. A {@link BlockLock} of the specific block must be acquired before
   * operating the metadata (by {@link BlockMetadataManager} or the data (by
   * {@link BlockFileOperator} of this block.
   * <p>
   * Short-circuit read or write by a local client should call this method before operating the
   * block to prevent race condition.
   *
   * @param blockId the block ID
   * @return true if success, false otherwise
   */
  public boolean lockBlock(long blockId) {
    Optional<BlockLock> optionalLock = mLockManager.getBlockLock(blockId);
    if (!optionalLock.isPresent()) {
      LOG.error("Cannot lock block {}: no lock found for this block", blockId);
      return false;
    }
    BlockLock lock = optionalLock.get();
    lock.lock();
    return true;
  }

  /**
   * Unlock a specific block. When block operation (on metadata or raw data or both) is done, the
   * lock must be released.
   *
   * @param blockId the block ID
   * @return true if success, false otherwise
   */
  public boolean unlockBlock(long blockId) {
    Optional<BlockLock> optionalLock = mLockManager.getBlockLock(blockId);
    if (!optionalLock.isPresent()) {
      LOG.error("Fail to lock block {}: no lock found for this block", blockId);
      return false;
    }
    BlockLock lock = optionalLock.get();
    if (!lock.isLocked()) {
      LOG.warn("no lock of Block {} is acquired", blockId);
      return true;
    }
    lock.unlock();
    return true;
  }

  /**
   * Get the metadata of the specific block. This method assumes the corresponding {@link BlockLock}
   * has been acquired.
   * <p>
   * This method is a wrapper around MetaManager.getBlockMeta() to provide public access to it.
   *
   * @param blockId the id of the block
   * @return the block metadata of this block id or absent if not found.
   */
  public Optional<BlockMeta> getBlockMetaNoLock(long blockId) {
    return mMetaManager.getBlockMeta(blockId);
  }

  /**
   * Remove the metadata of the specific block. This method assumes the corresponding
   * {@link BlockLock} has been acquired.
   * <p>
   * This method is a wrapper around MetaManager.removeBlockMeta() to provide public access to it.
   *
   * @param blockId the id of the block
   * @return the block metadata of this block id or absent if not found.
   */
  public boolean removeBlockMetaNoLock(long blockId) {
    return mMetaManager.removeBlockMeta(blockId);
  }

  /**
   * Create the metadata of a new block. This method assumes the corresponding {@link BlockLock} has
   * been acquired.
   * <p>
   * This method is a wrapper around MetaManager.createBlockMeta() to provide public access to it.
   *
   * @param userId the id of the user
   * @param blockId the id of the block
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
    if (!mLockManager.addBlockLock(blockId)) {
      return false;
    }
    lockBlock(blockId);
    Optional<BlockMeta> optionalBlock =
        createBlockMetaNoLock(userId, blockId, buf.limit(), tierHint);
    if (!optionalBlock.isPresent()) {
      unlockBlock(blockId);
      return false;
    }
    BlockMeta block = optionalBlock.get();
    BlockFileOperator operator = new BlockFileOperator(block);
    long bytes = operator.write(0, buf);
    if (bytes != buf.limit()) {
      unlockBlock(blockId);
      return false;
    }
    unlockBlock(blockId);
    return true;
  }

  /**
   * Read from an existing block at the specific offset and length.
   *
   * @param blockId the block ID
   * @param offset offset of the data to read in bytes
   * @param length length of the data to read in bytes
   * @return a ByteBuffer containing data read or absent
   * @throws IOException
   */
  public Optional<ByteBuffer> readBlock(long blockId, long offset, long length) throws IOException {
    lockBlock(blockId);
    Optional<BlockMeta> optionalBlock = getBlockMetaNoLock(blockId);
    if (!optionalBlock.isPresent()) {
      unlockBlock(blockId);
      return Optional.absent();
    }
    BlockMeta block = optionalBlock.get();
    BlockFileOperator operator = new BlockFileOperator(block);
    ByteBuffer buf = operator.read(offset, length);
    unlockBlock(blockId);
    return Optional.of(buf);
  }

  /**
   * Read all bytes from an existing block.
   *
   * @param blockId the block ID
   * @return a ByteBuffer containing data read or absent
   * @throws IOException
   */
  public Optional<ByteBuffer> readEntireBlock(long blockId) throws IOException {
    lockBlock(blockId);
    Optional<BlockMeta> optionalBlock = getBlockMetaNoLock(blockId);
    if (!optionalBlock.isPresent()) {
      unlockBlock(blockId);
      return Optional.absent();
    }
    BlockMeta block = optionalBlock.get();
    BlockFileOperator operator = new BlockFileOperator(block);
    ByteBuffer buf = operator.read(0, block.getBlockSize());
    unlockBlock(blockId);
    return Optional.of(buf);
  }

  /**
   * move an existing block to a different tier.
   *
   * @param blockId the block ID
   * @param newTierHint dest tier to move
   * @return true if success, false otherwise
   * @throws IOException
   */
  public boolean moveBlock(long userId, long blockId, int newTierHint) throws IOException {
    lockBlock(blockId);
    Optional<BlockMeta> optionalSrcBlock = getBlockMetaNoLock(blockId);
    if (!optionalSrcBlock.isPresent()) {
      unlockBlock(blockId);
      return false;
    }
    BlockMeta srcBlock = optionalSrcBlock.get();
    if (!removeBlockMetaNoLock(blockId)) {
      unlockBlock(blockId);
      return false;
    }
    Optional<BlockMeta> optionalDestBlock =
        createBlockMetaNoLock(userId, blockId, srcBlock.getBlockSize(), newTierHint);
    if (!optionalDestBlock.isPresent()) {
      unlockBlock(blockId);
      return false;
    }
    BlockMeta destBlock = optionalDestBlock.get();
    BlockFileOperator operator = new BlockFileOperator(srcBlock);
    boolean done = operator.move(destBlock.getPath());
    unlockBlock(blockId);
    return done;
  }


  /**
   * Free a block.
   *
   * @param blockId the id of the block
   * @return true if successful, false otherwise.
   */
  public boolean freeBlock(long blockId) throws FileNotFoundException {
    lockBlock(blockId);
    Optional<BlockMeta> optionalBlock = getBlockMetaNoLock(blockId);
    if (!optionalBlock.isPresent()) {
      unlockBlock(blockId);
      return false;
    }
    BlockMeta block = optionalBlock.get();
    if (!block.isCheckpointed()) {
      LOG.error("Cannot free block {}: not checkpointed", blockId);
      unlockBlock(blockId);
      return false;
    }

    // Step1: delete metadata of the block
    if (!removeBlockMetaNoLock(blockId)) {
      unlockBlock(blockId);
      return false;
    }
    // Step2: delete the data file of the block
    BlockFileOperator operator = new BlockFileOperator(block);
    boolean done = operator.delete();
    Preconditions.checkState(mLockManager.removeBlockLock(blockId));
    unlockBlock(blockId);
    return done;
  }
}
