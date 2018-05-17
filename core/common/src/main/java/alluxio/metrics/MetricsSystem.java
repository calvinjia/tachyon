/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.metrics;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.metrics.sink.Sink;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * MetricsSystem is a singleton that provides counters, gauges, and timers to the process. These
 * metrics are pushed to sinks at regular intervals. The MetricsSystem also includes built in
 * metrics sets (GC and memory usage).
 */
@ThreadSafe
public final class MetricsSystem {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsSystem.class);

  private static final String CLIENT_INSTANCE = "client";
  private static final String PROXY_INSTANCE = "proxy";
  private static final String MASTER_INSTANCE = "master";
  private static final String WORKER_INSTANCE = "worker";
  private static final TimeUnit MINIMAL_POLL_UNIT = TimeUnit.SECONDS;
  private static final int MINIMAL_POLL_PERIOD = 1;

  @VisibleForTesting
  public static final String SINK_REGEX = "^sink\\.(.+)\\.(.+)";

  public static final MetricRegistry METRIC_REGISTRY;

  static {
    METRIC_REGISTRY = new MetricRegistry();
    METRIC_REGISTRY.registerAll(new GarbageCollectorMetricSet());
    METRIC_REGISTRY.registerAll(new MemoryUsageGaugeSet());
  }

  @GuardedBy("MetricsSystem")
  private static List<Sink> sSinks;

  /**
   * Starts sinks specified in the configuration. This is an no-op if the sinks have already been
   * started.
   * Note: This has to be called after Alluxio configuration is initialized.
   */
  public static void startSinks() {
    synchronized (MetricsSystem.class) {
      if (sSinks != null) {
        LOG.info("Sinks have already been started.");
        return;
      }
    }
    String metricsConfFile = Configuration.get(PropertyKey.METRICS_CONF_FILE);
    if (metricsConfFile.isEmpty()) {
      LOG.info("Metrics is not enabled.");
      return;
    }
    MetricsConfig config = new MetricsConfig(metricsConfFile);
    startSinksFromConfig(config);
  }

  /**
   * Starts sinks from a given metrics configuration.
   *
   * @param config the metrics config
   */
  @VisibleForTesting
  public static synchronized void startSinksFromConfig(MetricsConfig config) {
    if (sSinks != null) {
      LOG.info("Sinks have already been started.");
      return;
    }
    LOG.info("Starting sinks with config: {}.", config);
    sSinks = new ArrayList<>();
    Map<String, Properties> sinkConfigs =
        MetricsConfig.subProperties(config.getProperties(), SINK_REGEX);
    for (Map.Entry<String, Properties> entry : sinkConfigs.entrySet()) {
      String classPath = entry.getValue().getProperty("class");
      if (classPath != null) {
        LOG.info("Starting sink {}.", classPath);
        try {
          Sink sink =
              (Sink) Class.forName(classPath).getConstructor(Properties.class, MetricRegistry.class)
                  .newInstance(entry.getValue(), METRIC_REGISTRY);
          sink.start();
          sSinks.add(sink);
        } catch (Exception e) {
          LOG.error("Sink class {} cannot be instantiated", classPath, e);
        }
      }
    }
  }

  /**
   * @return the number of sinks started
   */
  @VisibleForTesting
  public static synchronized int getNumSinks() {
    int sz = 0;
    if (sSinks != null) {
      sz = sSinks.size();
    }
    return sz;
  }

  /**
   * Stops all the sinks.
   */
  public static synchronized void stopSinks() {
    if (sSinks != null) {
      for (Sink sink : sSinks) {
        sink.stop();
      }
    }
    sSinks = null;
  }

  /**
   * @param name the basic metric name to create a timer for
   * @return a timer object associated with the fully qualified name of the metric
   */
  public static Timer timer(String name) {
    return METRIC_REGISTRY.timer(getMetricName(name));
  }

  /**
   * @param name the basic metric name to create a counter for
   * @return a counter object associated with the fully qualified name of the metric
   */
  public static Counter counter(String name) {
    return METRIC_REGISTRY.counter(getMetricName(name));
  }

  /**
   * Registers a gauge if it has not been registered.
   *
   * @param name the basic metric name
   * @param metric the gauge
   * @param <T> the type
   */
  public static synchronized <T> void registerGaugeIfAbsent(String name, Gauge<T> metric) {
    if (!METRIC_REGISTRY.getGauges().containsKey(name)) {
      METRIC_REGISTRY.register(name, metric);
    }
  }

  /**
   * @param name the basic metric name
   * @return the qualified metric name based on the process type
   */
  public static String getMetricName(String name) {
    switch (CommonUtils.PROCESS_TYPE.get()) {
      case CLIENT:
        return getClientMetricName(name);
      case PROXY:
        return getProxyMetricName(name);
      case MASTER:
        return getMasterMetricName(name);
      case WORKER:
        return getWorkerMetricName(name);
      default:
        throw new RuntimeException("Invalid Process Type " + CommonUtils.PROCESS_TYPE.get());
    }
  }

  /**
   * Builds metric registry names for master instance. The pattern is instance.metricName.
   *
   * @param name the metric name
   * @return the metric registry name
   */
  public static String getMasterMetricName(String name) {
    return Joiner.on(".").join(MASTER_INSTANCE, name);
  }

  /**
   * Builds metric registry name for worker instance. The pattern is instance.uniqueId.metricName.
   *
   * @param name the metric name
   * @return the metric registry name
   */
  public static String getWorkerMetricName(String name) {
    return getMetricNameWithUniqueId(WORKER_INSTANCE, name);
  }

  /**
   * Builds metric registry name for client instance. The pattern is instance.uniqueId.metricName.
   *
   * @param name the metric name
   * @return the metric registry name
   */
  public static String getClientMetricName(String name) {
    return getMetricNameWithUniqueId(CLIENT_INSTANCE, name);
  }

  /**
   * Builds metric registry name for proxy instance. The pattern is instance.uniqueId.metricName.
   *
   * @param name the metric name
   * @return the metric registry name
   */
  public static String getProxyMetricName(String name) {
    return getMetricNameWithUniqueId(PROXY_INSTANCE, name);
  }

  /**
   * Builds unique metric registry names with unique ID (set to host name). The pattern is
   * instance.hostname.metricName.
   *
   * @param instance the instance name
   * @param name the metric name
   * @return the metric registry name
   */
  private static String getMetricNameWithUniqueId(String instance, String name) {
    return Joiner.on(".")
        .join(instance, NetworkAddressUtils.getLocalHostName().replace('.', '_'), name);
  }

  /**
   * Checks if the poll period is smaller that the minimal poll period which is 1 second.
   *
   * @param pollUnit the polling unit
   * @param pollPeriod the polling period
   * @throws IllegalArgumentException if the polling period is invalid
   */
  public static void checkMinimalPollingPeriod(TimeUnit pollUnit, int pollPeriod)
      throws IllegalArgumentException {
    int period = (int) MINIMAL_POLL_UNIT.convert(pollPeriod, pollUnit);
    Preconditions.checkArgument(period >= MINIMAL_POLL_PERIOD,
        "Polling period %d %d is below the minimal polling period", pollPeriod, pollUnit);
  }

  /**
   * Util function to remove get the metrics name without instance and host.
   * @param metricsName the long metrics name with instance and host name
   * @return the metrics name without instance and host name
   */
  public static String stripInstanceAndHost(String metricsName) {
    String[] pieces = metricsName.split("\\.");
    Preconditions.checkArgument(pieces.length > 1, "Incorrect metrics name: %s.", metricsName);

    // Master metrics doesn't have hostname included.
    if (!pieces[0].equals(MASTER_INSTANCE)) {
      pieces[1] = null;
    }
    pieces[0] = null;
    return Joiner.on(".").skipNulls().join(pieces);
  }

  /**
   * Escapes a URI, replacing "/" and "." with "_" so that when the URI is used in a metric name,
   * the "/" and "." won't be interpreted as path separators.
   *
   * @param uri the URI to escape
   * @return the string representing the escaped URI
   */
  public static String escape(AlluxioURI uri) {
    return uri.toString().replace("/", "_").replace(".", "_");
  }

  /**
   * Resets all the counters to 0 for testing.
   */
  public static void resetAllCounters() {
    for (Map.Entry<String, Counter> entry : METRIC_REGISTRY.getCounters().entrySet()) {
      entry.getValue().dec(entry.getValue().getCount());
    }
  }

  /**
   * Resets the metric registry and removes all the metrics.
   */
  public static void clearAllMetrics() {
    for (String name : METRIC_REGISTRY.getNames()) {
      METRIC_REGISTRY.remove(name);
    }
  }

  /**
   * Disallows any explicit initialization.
   */
  private MetricsSystem() {
  }
}
