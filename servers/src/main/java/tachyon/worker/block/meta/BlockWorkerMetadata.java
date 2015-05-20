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

import tachyon.Constants;
import tachyon.conf.TachyonConf;

/**
 * Class that provides data structure for the Worker, Allocator and Evictor. The methods provided
 * by this class is not thread safe.
 */
public class BlockWorkerMetadata {

  private long mAvailableSpace;
  private Map<Integer, StorageTier> mTiers;

  public BlockWorkerMetadata(TachyonConf tachyonConf) {
    // Initialize storage tiers
    int totalTiers = tachyonConf.getInt(Constants.WORKER_MAX_TIERED_STORAGE_LEVEL, 1);
    mTiers = new HashMap<Integer, StorageTier>(totalTiers);
    for (int i = 0; i < totalTiers; i ++) {
      mTiers.put(i, new StorageTier(tachyonConf, i));
    }
  }

  public StorageTier getTier(int tierAlias) {
    mTiers.get(tierAlias);
  }

  public long getAvailableSpace() {
    return mAvailableSpace;
  }
}
