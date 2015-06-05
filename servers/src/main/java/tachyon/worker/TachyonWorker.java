package tachyon.worker;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.util.NetworkUtils;
import tachyon.util.ThreadFactoryUtils;

/**
 * The main program that runs the Tachyon Worker. The Tachyon Worker is responsible for managing
 * its own local Tachyon space as well as its under storage system space.
 */
public class TachyonWorker {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private ExecutorService mHeartbeatExecutorService;
  private boolean shouldRun;

  public TachyonWorker() {
    mHeartbeatExecutorService =
        Executors.newFixedThreadPool(1, ThreadFactoryUtils.daemon("worker-heartbeat-%d"));
  }

  public static void main(String[] args) {
    checkArgs(args);
    TachyonConf tachyonConf = new TachyonConf();
    TachyonWorker worker = TachyonWorker.createWorker(tachyonConf);
    try {
      worker.join();
    } catch (Exception e) {
      LOG.error("Uncaught exception, shutting down Tachyon Worker", e);
      System.exit(-1);
    }
  }

  private static synchronized TachyonWorker createWorker(TachyonConf tachyonConf) {
    String masterHostname =
        tachyonConf.get(Constants.MASTER_HOSTNAME, NetworkUtils.getLocalHostName(tachyonConf));
    int masterPort = tachyonConf.getInt(Constants.MASTER_PORT, Constants.DEFAULT_MASTER_PORT);
    return new TachyonWorker();
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
