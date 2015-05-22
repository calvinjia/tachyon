package tachyon.worker.block;

import java.util.concurrent.locks.ReentrantLock;

/**
 * A Lock that guards one block.
 */
public class BlockLock {
  private final ReentrantLock mLock;
  /** The block Id this lock guards **/
  private final long mBlockId;
  /** The unique id of this lock **/
  private final Integer mLockId;

  public BlockLock(long blockId, Integer lockId) {
    mBlockId = blockId;
    mLockId = lockId;
    mLock = new ReentrantLock();
  }

  public long getBlockId() {
    return mBlockId;
  }

  public Integer getLockId() {
    return mLockId;
  }

  public void lock() {
    mLock.lock();
  }

  public void unlock() {
    mLock.unlock();
  }

  public boolean isLocked() {
    mLock.isLocked();
  }
}
