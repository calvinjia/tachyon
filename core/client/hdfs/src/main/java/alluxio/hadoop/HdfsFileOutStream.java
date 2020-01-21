package alluxio.hadoop;

import alluxio.client.file.FileOutStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;

public class HdfsFileOutStream extends FileOutStream {
  private final FSDataOutputStream mOut;

  public HdfsFileOutStream(FSDataOutputStream out) {
    mOut = out;
  }

  @Override public void write(byte[] b) throws IOException {
    mOut.write(b);
  }

  @Override public void write(byte[] b, int off, int len) throws IOException {
    mOut.write(b, off, len);
  }

  @Override public void flush() throws IOException {
    mOut.flush();
  }

  @Override public void close() throws IOException {
    mOut.close();
  }

  @Override public void write(int b) throws IOException {
    mOut.write(b);
  }
}
