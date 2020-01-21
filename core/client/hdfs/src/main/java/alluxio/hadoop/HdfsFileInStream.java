package alluxio.hadoop;

import alluxio.client.file.FileInStream;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

public class HdfsFileInStream extends FileInStream {
  private final FSDataInputStream mIn;

  public HdfsFileInStream(FSDataInputStream in) {
    mIn = in;
  }

  @Override public int read(byte[] b) throws IOException {
    return mIn.read(b);
  }

  @Override public int read(byte[] b, int off, int len) throws IOException {
    return mIn.read(b, off, len);
  }

  @Override public long skip(long n) throws IOException {
    return mIn.skip(n);
  }

  @Override public int available() throws IOException {
    return mIn.available();
  }

  @Override public void close() throws IOException {
    mIn.close();
  }

  @Override public synchronized void mark(int readlimit) {
    mIn.mark(readlimit);
  }

  @Override public synchronized void reset() throws IOException {
    mIn.reset();
  }

  @Override public boolean markSupported() {
    return mIn.markSupported();
  }

  @Override public void seek(long pos) throws IOException {
    mIn.seek(pos);
  }

  @Override public long getPos() throws IOException {
    return mIn.getPos();
  }

  @Override public long remaining() {
    throw new UnsupportedOperationException("Remaining is not supported");
  }

  @Override public int positionedRead(long position, byte[] buffer, int offset, int length)
      throws IOException {
    return mIn.read(position, buffer, offset, length);
  }

  @Override public int read() throws IOException {
    return mIn.read();
  }
}
