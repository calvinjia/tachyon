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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import tachyon.Constants;
import tachyon.util.CommonUtils;
import tachyon.worker.block.meta.BlockMeta;

/**
 * This class contains a collection of static methods to operat block data files in managed storage.
 * <p>
 * This class does not provide thread-safety. Corresponding {@link BlockLock} should be acquired
 * before creating an instance of this class.
 */
public class BlockFileOperator {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final BlockMeta mBlockMeta;
  private final String mFilePath;
  private final RandomAccessFile mLocalFile;
  private final FileChannel mLocalFileChannel;
  private final Closer mCloser = Closer.create();

  public BlockFileOperator(BlockMeta blockMeta) throws FileNotFoundException {
    mBlockMeta = Preconditions.checkNotNull(blockMeta);
    mFilePath = mBlockMeta.getPath();
    mLocalFile = mCloser.register(new RandomAccessFile(mFilePath, "rw"));
    mLocalFileChannel = mCloser.register(mLocalFile.getChannel());
  }

  /**
   * Read data from block file.
   *
   * @param offset the offset from starting of the block file
   * @param length the length of data to read
   * @return ByteBuffer the data that was read
   * @throws IOException
   */
  public ByteBuffer read(long offset, long length) throws IOException {
    final long fileSize = mLocalFile.length();
    Preconditions.checkArgument(offset + length <= fileSize,
        "offset=%s, length=%s, exceeds file size(%s)", offset, length, fileSize);
    ByteBuffer buf = mLocalFileChannel.map(FileChannel.MapMode.READ_ONLY, offset, length);
    return buf;
  }

  /**
   * Write data to the block from an input ByteBuffer.
   *
   * @param offset starting offset of the block file to write
   * @param inputBuf ByteBuffer that input data is stored in
   * @return the size of data that was written
   * @throws IOException
   */
  public long write(long offset, ByteBuffer inputBuf) throws IOException {
    int inputBufLength = inputBuf.limit();
    ByteBuffer outputBuf = mLocalFileChannel.map(MapMode.READ_WRITE, offset, inputBufLength);
    outputBuf.put(inputBuf);
    CommonUtils.cleanDirectBuffer(outputBuf);
    return outputBuf.limit();
  }

  /**
   * Delete the block file.
   *
   * @return true on success, false otherwise.
   */
  public boolean delete() {
    return new File(mFilePath).delete();
  }

  /**
   * Move the block file
   *
   * @param destPath
   * @return
   */
  public boolean move(String destPath) {
    // TODO: implement this
    return true;
  }
}
