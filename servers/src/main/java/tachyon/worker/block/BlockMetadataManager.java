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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageTier;

/**
 * Manages the metadata of all blocks in managed space. This information is used by the BlockStore,
 * Allocator and Evictor.
 * <p>
 * This class is thread-safe and all operations on block metadata such as StorageTier, StorageDir
 * should go through this class.
 */
public class BlockMetadataManager {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private long mAvailableSpace;
  private Map<Integer, StorageTier> mTiers;

  public BlockMetadataManager(TachyonConf tachyonConf) {
    // Initialize storage tiers
    int totalTiers = tachyonConf.getInt(Constants.WORKER_MAX_TIERED_STORAGE_LEVEL, 1);
    mTiers = new HashMap<Integer, StorageTier>(totalTiers);
    for (int i = 0; i < totalTiers; i ++) {
      mTiers.put(i, new StorageTier(tachyonConf, i));
    }
  }

  public synchronized StorageTier getTier(int tierAlias) {
    return mTiers.get(tierAlias);
  }

  public synchronized Set<StorageTier> getTiers() {
    return new HashSet<StorageTier>(mTiers.values());
  }

  public synchronized long getAvailableSpace() {
    return mAvailableSpace;
  }

  /* Operations on metadata information */

  /**
   * Get the metadata of a specific block.
   *
   * @param blockId the block ID
   * @return metadata of the block or absent
   */
  public synchronized Optional<BlockMeta> getBlockMeta(long blockId) {
    for (StorageTier tier : mTiers.values()) {
      Optional<BlockMeta> optionalBlock = tier.getBlockMeta(blockId);
      if (optionalBlock.isPresent()) {
        return optionalBlock;
      }
    }
    return Optional.absent();
  }

  /**
   * Add the metadata of a specific block to a storage tier.
   *
   * @param userId the user ID
   * @param blockId the block ID
   * @param blockSize the block size in bytes
   * @param tierAlias alias of the tier
   * @return metadata of the block or absent
   */
  public synchronized Optional<BlockMeta> addBlockMetaInTier(long userId, long blockId,
      long blockSize, int tierAlias) {
    StorageTier tier = getTier(tierAlias);
    Preconditions.checkArgument(tier != null, "tierAlias must be valid: %s", tierAlias);
    return tier.addBlockMeta(userId, blockId, blockSize);
  }

  /**
   * Remove the metadata of a specific block.
   *
   * @param blockId the block ID
   * @return true if success, false otherwise
   */
  public synchronized boolean removeBlockMeta(long blockId) {
    for (StorageTier tier : mTiers.values()) {
      if (tier.removeBlockMeta(blockId)) {
        return true;
      }
    }
    return false;
  }
}
