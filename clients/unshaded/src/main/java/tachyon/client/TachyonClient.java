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

package tachyon.client;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;

import java.net.InetSocketAddress;

/**
 * Entry point for interacting with the Tachyon system. The TachyonClient provides the means to
 * specify custom configuration options on initialization. Initialization only occurs once, and
 * future attempts to get a client with a different master address will fail. The configuration
 * options will only be respected on the first call to get. Tachyon operations are done through
 * secondary clients such as {@link tachyon.client.file.TachyonFileSystem} which are obtained
 * through the getter methods in TachyonClient.
 */
public final class TachyonClient {
  private static final Object INSTANCE_LOCK = new Object();
  private static TachyonClient sClient = null;

  private final InetSocketAddress mMasterAddress;
  private final TachyonConf mTachyonConf;
  private final TachyonURI mMaster;

  private TachyonClient(TachyonURI master, TachyonConf conf) {
    mMasterAddress = new InetSocketAddress(master.getHost(), master.getPort());
    mTachyonConf = conf;
    mMaster = master;
  }

  public static TachyonClient get() {
    TachyonConf conf = new TachyonConf();
    TachyonURI master = new TachyonURI(conf.get(Constants.MASTER_ADDRESS));
    return get(master, conf);
  }

  public static TachyonClient get(TachyonURI master) {
    TachyonConf conf = new TachyonConf();
    return get(master, conf);
  }

  public static TachyonClient get(TachyonURI master, TachyonConf conf) {
    if (sClient == null) {
      synchronized (INSTANCE_LOCK) {
        if (sClient == null) {
          sClient = new TachyonClient(master, conf);
        }
      }
    }
    return sClient;
  }
}
