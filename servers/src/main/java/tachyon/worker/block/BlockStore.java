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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.worker.BlockLockManager;
import tachyon.worker.block.allocator.Allocator;
import tachyon.worker.block.allocator.NaiveAllocator;
import tachyon.worker.block.evictor.Evictor;
import tachyon.worker.block.evictor.NaiveEvictor;
import tachyon.worker.block.meta.BlockMeta;


/**
 * Central management for block level operations.
 * <p>
 * This class is thread-safe.
 */
public class BlockStore {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final TachyonConf mTachyonConf;
  private final BlockMetadataManager mMetaManager;

  private final Allocator mAllocator;
  private final Evictor mEvictor;
  private final BlockLockManager mLockManager;

  public BlockStore() {
    mTachyonConf = new TachyonConf();
    mMetaManager = new BlockMetadataManager(mTachyonConf);
    mLockManager = new BlockLockManager();
    mAllocator = new NaiveAllocator(mMetaManager);
    mEvictor = new NaiveEvictor(mMetaManager);
  }

  /**
   * Get the block given its block id.
   *
   * @param blockId the id of the block
   * @return the block metadata of this block id or absent if not found.
   */
  public Optional<BlockMeta> getBlock(long blockId) {
    Optional<BlockMeta> optionalBlock = mMetaManager.getBlockMeta(blockId);
    if (!optionalBlock.isPresent()) {
      LOG.error("Fail to get block {}: not existing", blockId);
      return Optional.absent();
    }
    return optionalBlock;
  }

  /**
   * Create a new block.
   *
   * @param userId the id of the user
   * @param blockId the id of the block
   * @param blockSize block size in bytes
   * @param tierHint which tier to create this block
   * @return the newly created block metadata or absent on creation failure.
   */
  public Optional<BlockMeta> createBlock(long userId, long blockId, long blockSize, int tierHint) {
    if (!mLockManager.addBlockLock(blockId)) {
      return Optional.absent();
    }

    Optional<BlockMeta> optionalBlock =
        mAllocator.allocateBlock(userId, blockId, blockSize, tierHint);
    if (!optionalBlock.isPresent()) {
      mEvictor.freeSpace(blockSize, tierHint);
      optionalBlock = mAllocator.allocateBlock(userId, blockId, blockSize, tierHint);
      if (!optionalBlock.isPresent()) {
        LOG.error("Fail to create block {}:", blockId);
        return Optional.absent();
      }
    }
    return optionalBlock;
  }

  /**
   * Free a block.
   *
   * @param blockId the id of the block
   * @return true if successful, false otherwise.
   */
  public boolean freeBlock(long blockId) throws FileNotFoundException {
    Optional<BlockLock> optionalLock = mLockManager.getBlockLock(blockId);
    Preconditions.checkState(optionalLock.isPresent());
    BlockLock lock = optionalLock.get();
    lock.lock();

    Optional<BlockMeta> optionalBlock = mMetaManager.getBlockMeta(blockId);
    if (!optionalBlock.isPresent()) {
      LOG.error("Fail to free block {}: not existing", blockId);
      lock.unlock();
      return false;
    }
    BlockMeta block = optionalBlock.get();
    if (!block.isCheckpointed()) {
      LOG.error("Fail to free block {}: not checkpointed", blockId);
      lock.unlock();
      return false;
    }

    // Step1: delete metadata of the block
    if (!mMetaManager.removeBlockMeta(blockId)) {
      lock.unlock();
      return false;
    }
    // Step2: delete the data file of the block
    BlockFileOperator operator = new BlockFileOperator(block, lock);
    boolean deleted = operator.delete();

    lock.unlock();
    Preconditions.checkState(mLockManager.removeBlockLock(blockId));

    return deleted;
  }
}
