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

package tachyon.worker.block.evictor;

import java.util.List;

import tachyon.Pair;

/**
 * This class provides information about the blocks that need to be moved when evicting.
 */
public class EvictionSummary {
  private final List<Pair<Long, Long>> mToTransfer;
  private final List<Long> mToEvict;

  public EvictionSummary(List<Pair<Long, Long>> toTransfer, List<Long> toEvict) {
    mToTransfer = toTransfer;
    mToEvict = toEvict;
  }

  // TODO: Implement toString()
}
