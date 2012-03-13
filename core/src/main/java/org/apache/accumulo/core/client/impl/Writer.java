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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.TabletLocator.TabletLocation;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.core.tabletserver.thrift.ConstraintViolationException;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransportException;

public class Writer {
  
  private static final Logger log = Logger.getLogger(Writer.class);
  
  private Instance instance;
  private AuthInfo credentials;
  private Text table;
  
  public Writer(Instance instance, AuthInfo credentials, Text table) {
    ArgumentChecker.notNull(instance, credentials, table);
    this.instance = instance;
    this.credentials = credentials;
    this.table = table;
  }
  
  public Writer(Instance instance, AuthInfo credentials, String table) {
    this(instance, credentials, new Text(table));
  }
  
  private static void updateServer(Mutation m, KeyExtent extent, String server, AuthInfo ai, AccumuloConfiguration configuration) throws TException,
      NotServingTabletException, ConstraintViolationException, AccumuloSecurityException {
    ArgumentChecker.notNull(m, extent, server, ai);
    
    TabletClientService.Iface client = null;
    try {
      client = ThriftUtil.getTServerClient(server, configuration);
      client.update(null, ai, extent.toThrift(), m.toThrift());
      return;
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code);
    } catch (TTransportException e) {
      log.warn("Error connecting to " + server + ": " + e);
      throw e;
    } finally {
      ThriftUtil.returnClient((TServiceClient) client);
    }
  }
  
  public void update(Mutation m) throws AccumuloException, AccumuloSecurityException, ConstraintViolationException, TableNotFoundException {
    ArgumentChecker.notNull(m);
    
    if (m.size() == 0)
      throw new IllegalArgumentException("Can not add empty mutations");
    
    while (true) {
      TabletLocation tabLoc = TabletLocator.getInstance(instance, credentials, table).locateTablet(new Text(m.getRow()), false, true);
      
      if (tabLoc == null) {
        log.trace("No tablet location found for row " + new String(m.getRow()));
        UtilWaitThread.sleep(500);
        continue;
      }
      
      try {
        updateServer(m, tabLoc.tablet_extent, tabLoc.tablet_location, credentials, instance.getConfiguration());
        return;
      } catch (TException e) {
        log.trace("server = " + tabLoc.tablet_location, e);
        TabletLocator.getInstance(instance, credentials, table).invalidateCache(tabLoc.tablet_extent);
      } catch (NotServingTabletException e) {
        log.trace("Not serving tablet, server = " + tabLoc.tablet_location);
        TabletLocator.getInstance(instance, credentials, table).invalidateCache(tabLoc.tablet_extent);
      }
      
      UtilWaitThread.sleep(500);
    }
    
  }
}
