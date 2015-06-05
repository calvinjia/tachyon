package tachyon.worker;

import tachyon.worker.block.BlockStore;
import tachyon.worker.block.TieredBlockStore;

public class CoreWorker {
  private final BlockStore mBlockStore;

  public CoreWorker() {
    mBlockStore = new TieredBlockStore();
  }

}
