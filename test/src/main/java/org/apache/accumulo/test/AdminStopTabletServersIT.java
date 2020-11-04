/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloITBase;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.MiniClusterHarness;
import org.apache.accumulo.harness.TestingKdc;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.test.categories.MiniClusterOnlyTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

@Category(MiniClusterOnlyTests.class)
public class AdminStopTabletServersIT extends AccumuloITBase {
    private static final Logger log = LoggerFactory.getLogger(AdminStopTabletServersIT.class);
    public static final String TRUE = Boolean.toString(true);

    private static TestingKdc kdc;
    private static String krbEnabledForITs = null;
    private static ClusterUser rootUser;
    private static AuthenticationToken token;

    @BeforeClass
    public static void startKdc() throws Exception {
        kdc = new TestingKdc();
        kdc.start();
        krbEnabledForITs = System.getProperty(MiniClusterHarness.USE_KERBEROS_FOR_IT_OPTION);
        if (krbEnabledForITs == null || !Boolean.parseBoolean(krbEnabledForITs)) {
            System.setProperty(MiniClusterHarness.USE_KERBEROS_FOR_IT_OPTION, "true");
        }
        rootUser = kdc.getRootUser();
        String rootPassword = "rootPasswordShared1";
        token = new PasswordToken(rootPassword);
    }

    @AfterClass
    public static void stopKdc() {
        if (kdc != null) {
            kdc.stop();
        }
        if (krbEnabledForITs != null) {
            System.setProperty(MiniClusterHarness.USE_KERBEROS_FOR_IT_OPTION, krbEnabledForITs);
        }
        UserGroupInformation.setConfiguration(new Configuration(false));
    }

    private MiniAccumuloClusterImpl mac;

    @Before
    public void startMac() throws Exception {
        MiniClusterHarness harness = new MiniClusterHarness();
        mac = harness.create(this, token, kdc, new MiniClusterConfigurationCallback() {

            @Override
            public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
                Map<String,String> site = cfg.getSiteConfig();
                site.put(Property.INSTANCE_ZK_TIMEOUT.getKey(), "15s");
                cfg.setSiteConfig(site);
            }

        });

        mac.getConfig().setNumTservers(3);
        mac.start();
        // Enabled kerberos auth
        Configuration conf = new Configuration(false);
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
        UserGroupInformation.setConfiguration(conf);
    }

    @After
    public void stopMac() throws Exception {
        if (mac != null) {
            mac.stop();
        }
    }

    @Test(timeout = 10000000)
    public void test() throws Exception {
        final String table = "table";
        try (
                AccumuloClient c = mac.createAccumuloClient(rootUser.getPrincipal(), rootUser.getToken())) {
            c.tableOperations().create(table);
            BatchWriterConfig config = new BatchWriterConfig();
            try (BatchWriter bw = c.createBatchWriter(table, config)) {
                for (int rows = 0; rows < 500; rows++) {
                    Mutation m = new Mutation(Integer.toString(rows));
                    for (int col = 0; col < 5; col++) {
                        String colStr = Integer.toString(col);
                        m.put(colStr, colStr, colStr);
                    }
                    bw.addMutation(m);
                }
            }

            List<String> tservers = c.instanceOperations().getTabletServers();

            for (String tserver : tservers) {
                log.info("Stopping {}", tserver);
                assertEquals(0, mac.exec(Admin.class, "stop", tserver).getProcess().waitFor());
                Thread.sleep(1000);
            }

            log.info("Starting all tabletservers");
            mac.getClusterControl().start(ServerType.TABLET_SERVER);

            log.info("Verifying data written");

            try (Scanner s = c.createScanner(table, Authorizations.EMPTY)) {
                int count = Iterables.size(s);
                assertEquals(500 * 5, count);
            }
        }
    }
}