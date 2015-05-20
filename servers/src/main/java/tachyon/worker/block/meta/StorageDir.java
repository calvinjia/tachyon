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

package tachyon.worker.block.meta;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

/**
 * Represents one isolated storage unit. The methods provided by this class are thread safe.
 */
public class StorageDir {
  private Map<Long, BlockMeta> mIdToBlocksMap;
  private Map<Long, Set<Long>> mUsersToBlocksMap;
  private long mCapacityBytes;
  private long mAvailableBytes;
  private String mDirPath;

  public StorageDir(long capacityBytes, String dirPath) {
    mCapacityBytes = capacityBytes;
    mAvailableBytes = capacityBytes;
    mDirPath = dirPath;
    mIdToBlocksMap = new HashMap<Long, BlockMeta>(200);
    mUsersToBlocksMap = new HashMap<Long, Set<Long>>(20);
  }

  public long getCapacityBytes() {
    return mCapacityBytes;
  }

  public long getAvailableBytes() {
    return mAvailableBytes;
  }

  public String getDirPath() {
    return mDirPath;
  }

  public boolean hasBlock(long blockId) {
    return mIdToBlocksMap.containsKey(blockId)
  }

  public boolean addBlock(long userId, long blockId, long blockSize) {
    if (hasBlock(blockId)) {
      return false;
    }
    Set<Long> userBlocks = mUsersToBlocksMap.get(userId);
    if (null == userBlocks) {
      mUsersToBlocksMap.put(userId, Sets.newHashSet(blockId));
    } else {
      userBlocks.add(blockId);
    }
    mCapacityBytes += blockSize;
    mAvailableBytes -= blockSize;
    return true;
  }
}
