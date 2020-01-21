package alluxio.hadoop;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.FileIncompleteException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.OpenDirectoryException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ExistsPOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.ScheduleAsyncPersistencePOptions;
import alluxio.grpc.SetAclAction;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.UnmountPOptions;
import alluxio.security.authorization.AclEntry;
import alluxio.wire.BlockLocationInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.MountPointInfo;
import alluxio.wire.SyncPointInfo;
import com.google.common.base.Suppliers;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HadoopFileSystem implements FileSystem {
  private final org.apache.hadoop.fs.FileSystem mClient;
  private final FileSystemContext mContext;

  public HadoopFileSystem(FileSystemContext context) {
    mClient = Suppliers.memoize(() -> {
      try {
        return org.apache.hadoop.fs.FileSystem.get(new Configuration());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }).get();

    mContext = context;
  }

  @Override public boolean isClosed() {
    return false;
  }

  @Override public void createDirectory(AlluxioURI path, CreateDirectoryPOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    mClient.mkdirs(toPath(path));
  }

  @Override public FileOutStream createFile(AlluxioURI path, CreateFilePOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    return new HdfsFileOutStream(mClient.create(toPath(path)));
  }

  @Override public void delete(AlluxioURI path, DeletePOptions options)
      throws DirectoryNotEmptyException, FileDoesNotExistException, IOException, AlluxioException {
    mClient.delete(toPath(path), options.getRecursive());
  }

  @Override public boolean exists(AlluxioURI path, ExistsPOptions options)
      throws InvalidPathException, IOException, AlluxioException {
    return mClient.exists(toPath(path));
  }

  @Override public void free(AlluxioURI path, FreePOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    throw new UnsupportedOperationException("Free is not supported.");
  }

  @Override public List<BlockLocationInfo> getBlockLocations(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    // TODO(calvin): This can be supported.
    throw new UnsupportedOperationException("GetBlockLocations is not supported.");
  }

  @Override public AlluxioConfiguration getConf() {
    return mContext.getClusterConf();
  }

  @Override public URIStatus getStatus(AlluxioURI path, GetStatusPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    return toUriStatus(mClient.getFileStatus(toPath(path)));
  }

  @Override public List<URIStatus> listStatus(AlluxioURI path, ListStatusPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    return Arrays.stream(
        mClient.listStatus(toPath(path))).map(this::toUriStatus).collect(Collectors.toList());
  }

  @Override public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountPOptions options)
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException("Mount is not supported.");
  }

  @Override public void updateMount(AlluxioURI alluxioPath, MountPOptions options)
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException("UpdateMount is not supported.");
  }

  @Override public Map<String, MountPointInfo> getMountTable()
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException("GetMountTable is not supported.");
  }

  @Override public List<SyncPointInfo> getSyncPathList() throws IOException, AlluxioException {
    throw new UnsupportedOperationException("GetSyncPathList is not supported.");
  }

  @Override public FileInStream openFile(AlluxioURI path, OpenFilePOptions options)
      throws FileDoesNotExistException, OpenDirectoryException, FileIncompleteException,
      IOException, AlluxioException {
    return new HdfsFileInStream(mClient.open(toPath(path)));
  }

  @Override public void persist(AlluxioURI path, ScheduleAsyncPersistencePOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    throw new UnsupportedOperationException("Persist is not supported.");
  }

  @Override public void rename(AlluxioURI src, AlluxioURI dst, RenamePOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    mClient.rename(toPath(src), toPath(dst));
  }

  @Override public AlluxioURI reverseResolve(AlluxioURI ufsUri)
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException("ReverseResolve is not supported.");
  }

  @Override public void setAcl(AlluxioURI path, SetAclAction action, List<AclEntry> entries,
      SetAclPOptions options) throws FileDoesNotExistException, IOException, AlluxioException {
    // TODO(calvin): This could be supported
    throw new UnsupportedOperationException("SetAcl is not supported.");
  }

  @Override public void startSync(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    throw new UnsupportedOperationException("StartSync is not supported.");

  }

  @Override public void stopSync(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    throw new UnsupportedOperationException("StopSync is not supported.");
  }

  @Override public void setAttribute(AlluxioURI path, SetAttributePOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    // TODO(calvin): This could be supported
    throw new UnsupportedOperationException("SetAttribute is not supported.");
  }

  @Override public void unmount(AlluxioURI path, UnmountPOptions options)
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException("Unmount is not supported.");
  }

  @Override public void close() throws IOException {
    mClient.close();
  }

  private Path toPath(AlluxioURI uri) {
    return new Path(uri.toString());
  }

  private URIStatus toUriStatus(FileStatus status) {
    FileInfo info = new FileInfo();
    // TODO(calvin): Fill in more fields
    info
        .setLength(status.getLen())
        .setPath(status.getPath().toString());
    return new URIStatus(info);
  }
}
