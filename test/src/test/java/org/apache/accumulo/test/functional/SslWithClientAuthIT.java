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
package org.apache.accumulo.test.functional;

import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Run all the same tests as SslIT, but with client auth turned on.
 *
 * All the methods are overridden just to make it easier to run individual tests from an IDE.
 *
 */
public class SslWithClientAuthIT extends SslIT {
  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    super.configure(cfg, hadoopCoreSite);
    Map<String,String> site = cfg.getSiteConfig();
    site.put(Property.INSTANCE_RPC_SSL_CLIENT_AUTH.getKey(), "true");
    cfg.setSiteConfig(site);
  }

  @Override
  public int defaultTimeoutSeconds() {
    return 8 * 60;
  }

  @Override
  @Test
  public void binary() throws AccumuloException, AccumuloSecurityException, Exception {
    super.binary();
  }

  @Override
  @Test
  public void concurrency() throws Exception {
    super.concurrency();
  }

  @Override
  @Test
  public void adminStop() throws Exception {
    super.adminStop();
  }

  @Override
  @Test
  public void bulk() throws Exception {
    super.bulk();
  }

  @Override
  @Test
  public void mapReduce() throws Exception {
    super.mapReduce();
  }
}
