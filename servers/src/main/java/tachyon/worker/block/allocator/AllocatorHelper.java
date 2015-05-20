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

package tachyon.worker.block.allocator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.BlockWorkerMetadata;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.StorageTier;

/**
 * Helper class to implement common operations by different Allocators.
 */
public class AllocatorHelper {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final BlockWorkerMetadata mMetadata;

  public AllocatorHelper(BlockWorkerMetadata metadata) {
    mMetadata = Preconditions.checkNotNull(metadata);
  }

  public Optional<BlockMeta> createBlockInTierWithAlias(long userId, long blockId, long blockSize,
                                                        int tierAlias) {
    StorageTier tier = mMetadata.getTier(tierAlias);
    Preconditions.checkArgument(tier != null, "tierAlias must be valid: %s", tierAlias);
    return createBlockInTier(userId, blockId, blockSize, tier);
  }

  public synchronized Optional<BlockMeta> createBlockInTier(long userId, long blockId, long
      blockSize, StorageTier tier) {
    Preconditions.checkArgument(tier != null, "tier must be valid");
    for (StorageDir dir : tier.getDirs()) {
      Optional<BlockMeta> optionalBlock = createBlockInDir(userId, blockId, blockSize, dir);
      if (optionalBlock.isPresent()) {
        return optionalBlock;
      }
    }
    return Optional.absent();
  }

  public synchronized Optional<BlockMeta> createBlockInDir(long userId, long blockId,
                                                           long blockSize, StorageDir dir) {
    if (dir.getAvailableBytes() < blockSize) {
      LOG.error("Fail to create blockId {} in dir {}: {} bytes required, but {} bytes available",
          blockId, dir.toString(), blockSize, dir.getAvailableBytes());
      return Optional.absent();
    }
    if (dir.hasBlock(blockId)) {
      LOG.error("Fail to create blockId {} in dir {}: blockId exists", blockId, dir.toString());
      return Optional.absent();
    }
    if (!dir.addBlock(userId, blockId, blockSize)) {
      LOG.error("Fail to create blockId {} in dir {}", blockId, dir.toString());
      return Optional.absent();
    }
    return Optional.of(new BlockMeta(blockId, blockSize, dir.getDirPath()));
  }
}
