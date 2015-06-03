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

import com.google.common.base.Optional;

import tachyon.worker.block.meta.BlockMeta;

/**
 * This interface represents a blob store that manages and serves all the blobs (i.e., blocks) in
 * the local storage.
 */
public interface BlockStore {

  /**
   * Locks a block for a specific mode (READ or WRITE) and guards the subsequent operations on this
   * block.
   *
   * @param userId ID of the user to lock this block
   * @param blockId ID of the block to lock
   * @param blockLockType the lock type
   * @return the lock ID if the lock has been acquired, absent otherwise
   */
  Optional<Long> lockBlock(long userId, long blockId, BlockLock.BlockLockType blockLockType);

  /**
   * Unlocks a block which has been locked before by {@link #lockBlock}.
   *
   * @param userId ID of the user to unlock this block
   * @param blockId ID of the block to unlock
   * @param lockId ID of the lock returned by {@link #lockBlock}
   * @return true if the lock has been released, false otherwise
   */
  boolean unlockBlock(long userId, long blockId, long lockId);

  /**
   * Creates a new block in a temporary path in the specific location. This method will create the
   * meta data of this block and assign a temporary path to store its data. Before commit, all data
   * of this newly created block will be stored in the temp path and the block is only "visible" to
   * its writer client.
   *
   * @param userId the user ID
   * @param blockId the block ID
   * @param location location to create this block
   * @return block meta if success, absent otherwise
   */
  Optional<BlockMeta> createBlock(long userId, long blockId, BlockLocation location);

  /**
   * Commits a temporary block to the local store and returns the updated meta data. After commit,
   * the block will be available in this block store for all clients.
   *
   * @param userId the user ID
   * @param blockId the block ID
   * @return block meta if success, absent otherwise
   */
  Optional<BlockMeta> commitBlock(long userId, long blockId);

  /**
   * Aborts a temporary block. The meta data of this block will not be added, its data will be
   * deleted and the space will be reclaimed.
   *
   * @param userId the user ID
   * @param blockId the block ID
   * @return true if success, false otherwise
   */
  boolean abortBlock(long userId, long blockId);

  /**
   * Requests more space for a temporary block
   *
   * @param userId the user ID
   * @param size the amount of more space to request in bytes
   * @return the amount of space requested
   */
  long requestSpace(long userId, long size);

  /**
   * Creates a writer on the block content to write data to this block.
   *
   * @param userId the user ID
   * @param blockId the block ID
   * @return a BlockWriter instance on this block if success, absent otherwise
   */
  Optional<BlockWriter> getBlockWriter(long userId, long blockId);

  /**
   * Creates a reader on the block content to read data from this block.
   * <p>
   * This method requires the lock ID returned by a proceeding {@link #lockBlock}.
   *
   * @param userId the user ID
   * @param blockId the block ID
   * @param lockId the lock ID
   * @return a BlockReader instance on this block if success, absent otherwise
   */
  Optional<BlockReader> getBlockReader(long userId, long blockId, long lockId);

  /**
   * Copies an existing block to another location in the storage. If the block can not be found or
   * the new location doesn't have enough space, return false.
   *
   * @param userId the user ID
   * @param blockId the block ID
   * @param location the location of the destination
   * @return true if success, false otherwise
   */
  boolean copyBlock(long userId, long blockId, BlockLocation location);

  /**
   * Removes an existing block from a specific location. If the block can not be found, return
   * false.
   *
   * @param userId the user ID
   * @param blockId the block ID
   * @param location the location to remove this block
   * @return true if successful, false otherwise.
   */
  boolean removeBlock(long userId, long blockId, BlockLocation location);

  /**
   * Gets the file meta of the specific block in local storage. If the block can not be found,
   * return absent.
   * <p>
   * This method requires the lock ID returned by a proceeding {@link #lockBlock}.
   *
   * @param userId ID of the user to get this file
   * @param blockId ID of the block
   * @param lockId ID of the lock
   * @return the block meta, or absent if not found.
   */
  Optional<BlockMeta> getBlockMeata(long userId, long blockId, long lockId);

  /**
   * Gets the meta data of the entire store.
   *
   * @return store meta data
   */
  StoreMeta getStoreMeta();

  /**
   * Cleans up the data associated with a specific user (typically a dead user), e.g., the un
   * released locks by this user.
   *
   * @param userId user ID
   * @return true if success, false otherwise
   */
  boolean cleanupUser(long userId);
}
