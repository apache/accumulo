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

import org.apache.accumulo.core.client.impl.Writer;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.tabletserver.thrift.ConstraintViolationException;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class MetaConstraintRetryIT extends AccumuloClusterIT {

  @Override
  public int defaultTimeoutSeconds() {
    return 30;
  }

  // a test for ACCUMULO-3096
  @Test(expected = ConstraintViolationException.class)
  public void test() throws Exception {

    getConnector().securityOperations().grantTablePermission("root", MetadataTable.NAME, TablePermission.WRITE);

    Credentials credentials = new Credentials(getPrincipal(), getToken());
    Writer w = new Writer(super.getConnector().getInstance(), credentials, MetadataTable.ID);
    KeyExtent extent = new KeyExtent(new Text("5"), null, null);

    Mutation m = new Mutation(extent.getMetadataEntry());
    // unknown columns should cause contraint violation
    m.put("badcolfam", "badcolqual", "3");

    try {
      MetadataTableUtil.update(w, credentials, null, m);
    } catch (RuntimeException e) {
      if (e.getCause().getClass().equals(ConstraintViolationException.class)) {
        throw (ConstraintViolationException) e.getCause();
      }
    }
  }
}
