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

import static com.google.common.base.Charsets.UTF_8;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.TabletLocator.TabletLocation;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.tabletserver.thrift.ConstraintViolationException;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;

public class Writer {

  private static final Logger log = Logger.getLogger(Writer.class);

  private Instance instance;
  private Credentials credentials;
  private Text table;

  public Writer(Instance instance, Credentials credentials, Text table) {
    ArgumentChecker.notNull(instance, credentials, table);
    this.instance = instance;
    this.credentials = credentials;
    this.table = table;
  }

  public Writer(Instance instance, Credentials credentials, String table) {
    this(instance, credentials, new Text(table));
  }

  private static void updateServer(Instance instance, Mutation m, KeyExtent extent, String server, Credentials ai, AccumuloConfiguration configuration)
      throws TException, NotServingTabletException, ConstraintViolationException, AccumuloSecurityException {
    ArgumentChecker.notNull(m, extent, server, ai);

    TabletClientService.Iface client = null;
    try {
      client = ThriftUtil.getTServerClient(server, configuration);
      client.update(Tracer.traceInfo(), ai.toThrift(instance), extent.toThrift(), m.toThrift());
      return;
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code);
    } finally {
      ThriftUtil.returnClient((TServiceClient) client);
    }
  }

  public void update(Mutation m) throws AccumuloException, AccumuloSecurityException, ConstraintViolationException, TableNotFoundException {
    ArgumentChecker.notNull(m);

    if (m.size() == 0)
      throw new IllegalArgumentException("Can not add empty mutations");

    while (true) {
      TabletLocation tabLoc = TabletLocator.getLocator(instance, table).locateTablet(credentials, new Text(m.getRow()), false, true);

      if (tabLoc == null) {
        log.trace("No tablet location found for row " + new String(m.getRow(), UTF_8));
        UtilWaitThread.sleep(500);
        continue;
      }

      try {
        updateServer(instance, m, tabLoc.tablet_extent, tabLoc.tablet_location, credentials, ServerConfigurationUtil.getConfiguration(instance));
        return;
      } catch (NotServingTabletException e) {
        log.trace("Not serving tablet, server = " + tabLoc.tablet_location);
        TabletLocator.getLocator(instance, table).invalidateCache(tabLoc.tablet_extent);
      } catch (ConstraintViolationException cve) {
        log.error("error sending update to " + tabLoc.tablet_location + ": " + cve);
        // probably do not need to invalidate cache, but it does not hurt
        TabletLocator.getLocator(instance, table).invalidateCache(tabLoc.tablet_extent);
        throw cve;
      } catch (TException e) {
        log.error("error sending update to " + tabLoc.tablet_location + ": " + e);
        TabletLocator.getLocator(instance, table).invalidateCache(tabLoc.tablet_extent);
      }

      UtilWaitThread.sleep(500);
    }

  }
}
