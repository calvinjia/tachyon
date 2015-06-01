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

import java.nio.ByteBuffer;

import com.google.common.base.Optional;

/**
 * This interface represents an object store that manages and serves all the object (i.e., blocks)
 * in the local storage.
 */
public interface BlockStore<LocationHint> {
  enum BlockLockType {
    READ,  // A read lock
    WRITE,  // A write lock
  }

  //
  // A generic object store API. Its implementation is supposed to be thread-safe.
  //
  /**
   * Creates a new block with data from a ByteBuffer.
   * <p>
   * This method is thread-safe and blocking.
   *
   * @param userId the user ID
   * @param blockId the block ID
   * @param buf the input buffer
   * @param hint the hint on where to create this block, e.g. tier for {@link TieredBlockStore}
   * @return true if success, false otherwise
   */
  boolean createBlock(long userId, long blockId, ByteBuffer buf, LocationHint hint);

  /**
   * Reads data from an existing block at a specific offset and length.
   * <p>
   * This method is thread-safe and blocking.
   *
   * @param userId the user ID
   * @param blockId the block ID
   * @param offset offset of the data to read in bytes
   * @param length length of the data to read in bytes
   * @param hint the hint of the location to read this block
   * @return a ByteBuffer containing data read or absent
   */
  Optional<ByteBuffer> readBlock(long userId, long blockId, long offset, long length,
      LocationHint hint);

  /**
   * Relocates an existing block to another location in the storage.
   * <p>
   * This method is thread-safe and blocking.
   *
   * @param userId the user ID
   * @param blockId the block ID
   * @param hint the hint of the destination, e.g., destination tier for {@link TieredBlockStore}
   * @return true if success, false otherwise
   */
  boolean relocateBlock(long userId, long blockId, LocationHint hint);

  /**
   * Removes an existing block from a specific location.
   * <p>
   * This method is thread-safe and blocking.
   *
   * @param userId the user ID
   * @param blockId the block ID
   * @param hint the hint of the location to remove this block
   * @return true if successful, false otherwise.
   */
  boolean removeBlock(long userId, long blockId, LocationHint hint);

  /**
   * Frees a certain amount of space.
   * <p>
   * This method is thread-safe and blocking.
   *
   * @param userId the user ID
   * @param bytes the space to free in bytes
   * @param hint the hint to free space. e.g., tier for {@link TieredBlockStore}
   * @return true if success, false otherwise
   */
  boolean freeSpace(long userId, long bytes, LocationHint hint);

  //
  // Only for local client on short-circuit operations. In other cases, please use the generic
  // object store API above.
  //
  /**
   * Locks a block for a specific type.
   *
   * @param userId ID of the user to lock this block
   * @param blockId ID of the block to lock
   * @param blockLockType  the lock type (READ or WRITE)
   * @return the lock ID if the lock has been acquired, absent otherwise
   */
  Optional<Long> lockBlock(long userId, long blockId, BlockLockType blockLockType);

  /**
   * Unlocks a block for the given lock type.
   *
   * @param userId ID of the user to unlock this block
   * @param blockId ID of the block to unlock
   * @param lockId ID of the lock
   * @param blockLockType  the lock type (READ or WRITE)
   * @return true if the lock has been released, false otherwise
   */
  boolean unlockBlock(long userId, long blockId, long lockId, BlockLockType blockLockType);

  /**
   * Gets the file path of the specific block in local storage. This method assumes the
   * corresponding lock has been acquired by {@link #lockBlock} and the returned lock ID is thus
   * required for this method.
   *
   * @param userId ID of the user to get this file
   * @param blockId ID of the block
   * @param lockId ID of the lock
   * @return the block file path of this block, or absent if not found.
   */
  Optional<String> getBlockFilePath(long userId, long blockId, long lockId);
}
