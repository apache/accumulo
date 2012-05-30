/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.client.impl;

import java.net.UnknownHostException;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.log4j.Logger;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransportException;

public class MasterClient {
  private static final Logger log = Logger.getLogger(MasterClient.class);
  
  public static MasterClientService.Iface getConnectionWithRetry(Instance instance) {
    ArgumentChecker.notNull(instance);
    
    while (true) {
      
      MasterClientService.Iface result = getConnection(instance);
      if (result != null)
        return result;
      UtilWaitThread.sleep(250);
    }
    
  }
  
  public static MasterClientService.Iface getConnection(Instance instance) {
    List<String> locations = instance.getMasterLocations();
    
    if (locations.size() == 0) {
      log.debug("No masters...");
      return null;
    }
    
    String master = locations.get(0);
    int portHint = instance.getConfiguration().getPort(Property.MASTER_CLIENTPORT);
    
    try {
      // Master requests can take a long time: don't ever time out
      MasterClientService.Iface client = ThriftUtil.getClient(new MasterClientService.Client.Factory(), master, Property.MASTER_CLIENTPORT,
          instance.getConfiguration());
      return client;
    } catch (TTransportException tte) {
      if (tte.getCause().getClass().equals(UnknownHostException.class)) {
        // do not expect to recover from this
        throw new RuntimeException(tte);
      }
      log.debug("Failed to connect to master=" + master + " portHint=" + portHint + ", will retry... ", tte);
      return null;
    }
  }
  
  public static void close(MasterClientService.Iface iface) {
    TServiceClient client = (TServiceClient) iface;
    if (client != null && client.getInputProtocol() != null && client.getInputProtocol().getTransport() != null) {
      ThriftTransportPool.getInstance().returnTransport(client.getInputProtocol().getTransport());
    } else {
      log.debug("Attempt to close null connection to the master", new Exception());
    }
  }
  
  public static <T> T execute(Instance instance, ClientExecReturn<T,MasterClientService.Iface> exec) throws AccumuloException, AccumuloSecurityException {
    MasterClientService.Iface client = null;
    while (true) {
      try {
        client = getConnectionWithRetry(instance);
        return exec.execute(client);
      } catch (TTransportException tte) {
        log.debug("MasterClient request failed, retrying ... ", tte);
        UtilWaitThread.sleep(100);
      } catch (ThriftSecurityException e) {
        throw new AccumuloSecurityException(e.user, e.code, e);
      } catch (AccumuloException e) {
        throw e;
      } catch (Exception e) {
        throw new AccumuloException(e);
      } finally {
        if (client != null)
          close(client);
      }
    }
  }
  
  public static void execute(Instance instance, ClientExec<MasterClientService.Iface> exec) throws AccumuloException, AccumuloSecurityException {
    MasterClientService.Iface client = null;
    while (true) {
      try {
        client = getConnectionWithRetry(instance);
        exec.execute(client);
        break;
      } catch (TTransportException tte) {
        log.debug("MasterClient request failed, retrying ... ", tte);
        UtilWaitThread.sleep(100);
      } catch (ThriftSecurityException e) {
        throw new AccumuloSecurityException(e.user, e.code, e);
      } catch (AccumuloException e) {
        throw e;
      } catch (Exception e) {
        throw new AccumuloException(e);
      } finally {
        if (client != null)
          close(client);
      }
    }
  }
  
}
