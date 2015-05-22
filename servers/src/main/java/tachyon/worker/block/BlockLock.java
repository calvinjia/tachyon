package tachyon.worker.block;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A Lock to guard one block. There should be only one lock per block.
 */
public class BlockLock {
  static final AtomicInteger mBlockLockId = new AtomicInteger(0);

  private final ReentrantLock mLock;
  /** The block Id this lock guards **/
  private final long mBlockId;
  /** The unique id of each lock **/
  private final int mLockId;

  public BlockLock(long blockId) {
    mBlockId = blockId;
    mLockId = mBlockLockId.incrementAndGet();
    mLock = new ReentrantLock();
  }

  public long getBlockId() {
    return mBlockId;
  }

  public int getLockId() {
    return mLockId;
  }

  public void lock() {
    mLock.lock();
  }

  public void tryLock() {
    mLock.tryLock();
  }

  public void unlock() {
    mLock.unlock();
  }

  public boolean isLocked() {
    mLock.isLocked();
  }
}
