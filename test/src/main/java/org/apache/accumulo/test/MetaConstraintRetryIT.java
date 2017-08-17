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

package org.apache.accumulo.test;

import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.Writer;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.tabletserver.thrift.ConstraintViolationException;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.junit.Test;

public class MetaConstraintRetryIT extends AccumuloClusterHarness {

  @Override
  public int defaultTimeoutSeconds() {
    return 30;
  }

  // a test for ACCUMULO-3096
  @Test(expected = ConstraintViolationException.class)
  public void test() throws Exception {

    getConnector().securityOperations().grantTablePermission(getAdminPrincipal(), MetadataTable.NAME, TablePermission.WRITE);

    Credentials credentials = new Credentials(getAdminPrincipal(), getAdminToken());
    ClientContext context = new ClientContext(getConnector().getInstance(), credentials, cluster.getClientConfig());
    Writer w = new Writer(context, MetadataTable.ID);
    KeyExtent extent = new KeyExtent(Table.ID.of("5"), null, null);

    Mutation m = new Mutation(extent.getMetadataEntry());
    // unknown columns should cause contraint violation
    m.put("badcolfam", "badcolqual", "3");

    try {
      MetadataTableUtil.update(w, null, m);
    } catch (RuntimeException e) {
      if (e.getCause().getClass().equals(ConstraintViolationException.class)) {
        throw (ConstraintViolationException) e.getCause();
      }
    }
  }
}
