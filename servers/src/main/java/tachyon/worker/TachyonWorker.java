package tachyon.worker;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.util.ThreadFactoryUtils;
import tachyon.worker.block.BlockWorkerServiceHandler;
import tachyon.worker.block.TieredBlockStore;

/**
 * The main program that runs the Tachyon Worker. The Tachyon Worker is responsible for managing
 * its own local Tachyon space as well as its under storage system space.
 */
public class TachyonWorker {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private BlockWorkerServiceHandler mServiceHandler;
  private CoreWorker mCoreWorker;
  private DataServer mDataServer;
  private ExecutorService mHeartbeatExecutorService;
  private boolean shouldRun;

  public TachyonWorker(TachyonConf tachyonConf) {
    mCoreWorker = new CoreWorker();
    mDataServer = DataServer.Factory.createDataServer(tachyonConf, mCoreWorker);
    mServiceHandler = new BlockWorkerServiceHandler(mCoreWorker);

    try {
      LOG.info("Tachyon Worker version " + Version.VERSION + " tries to start @ " + workerAddress);
      WorkerService.Processor<WorkerServiceHandler> processor =
          new WorkerService.Processor<WorkerServiceHandler>(mWorkerServiceHandler);

      mServerTServerSocket = new TServerSocket(workerAddress);
      mPort = NetworkUtils.getPort(mServerTServerSocket);

      mServer =
          new TThreadPoolServer(new TThreadPoolServer.Args(mServerTServerSocket)
              .minWorkerThreads(minWorkerThreads).maxWorkerThreads(maxWorkerThreads)
              .processor(processor).transportFactory(new TFramedTransport.Factory())
              .protocolFactory(new TBinaryProtocol.Factory(true, true)));
    } catch (TTransportException e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }

    mHeartbeatExecutorService =
        Executors.newFixedThreadPool(1, ThreadFactoryUtils.daemon("worker-heartbeat-%d"));
  }

  public static void main(String[] args) {
    checkArgs(args);
    TachyonConf tachyonConf = new TachyonConf();
    TachyonWorker worker = new TachyonWorker(tachyonConf);
    try {
      worker.join();
    } catch (Exception e) {
      LOG.error("Uncaught exception, shutting down Tachyon Worker", e);
      System.exit(-1);
    }
  }

  private static void checkArgs(String[] args) {
    if (args.length != 0) {
      LOG.info("Usage: java TachyonWorker");
      System.exit(-1);
    }
  }

  public void join() {
  }
}
