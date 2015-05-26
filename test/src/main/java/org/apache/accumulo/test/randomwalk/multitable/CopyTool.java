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
package org.apache.accumulo.test.randomwalk.multitable;

import java.io.IOException;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.DelegationTokenConfig;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

public class CopyTool extends Configured implements Tool {
  protected final Logger log = Logger.getLogger(this.getClass());

  public static class SeqMapClass extends Mapper<Key,Value,Text,Mutation> {
    @Override
    public void map(Key key, Value val, Context output) throws IOException, InterruptedException {
      Mutation m = new Mutation(key.getRow());
      m.put(key.getColumnFamily(), key.getColumnQualifier(), val);
      output.write(null, m);
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), this.getClass().getSimpleName());
    job.setJarByClass(this.getClass());

    if (job.getJar() == null) {
      log.error("M/R requires a jar file!  Run mvn package.");
      return 1;
    }

    ClientConfiguration clientConf = new ClientConfiguration().withInstance(args[3]).withZkHosts(args[4]);

    job.setInputFormatClass(AccumuloInputFormat.class);
    AccumuloInputFormat.setInputTableName(job, args[2]);
    AccumuloInputFormat.setScanAuthorizations(job, Authorizations.EMPTY);
    AccumuloInputFormat.setZooKeeperInstance(job, clientConf);

    final String principal;
    final AuthenticationToken token;
    if (clientConf.getBoolean(ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey(), false)) {
      // Use the Kerberos creds to request a DelegationToken for MapReduce to use
      // We could use the specified keytab (args[1]), but we're already logged in and don't need to, so we can just use the current user
      KerberosToken kt = new KerberosToken();
      try {
        UserGroupInformation user = UserGroupInformation.getCurrentUser();
        if (!user.hasKerberosCredentials()) {
          throw new IllegalStateException("Expected current user to have Kerberos credentials");
        }

        // Get the principal via UGI
        principal = user.getUserName();

        // Connector w/ the Kerberos creds
        ZooKeeperInstance inst = new ZooKeeperInstance(clientConf);
        Connector conn = inst.getConnector(principal, kt);

        // Do the explicit check to see if the user has the permission to get a delegation token
        if (!conn.securityOperations().hasSystemPermission(conn.whoami(), SystemPermission.OBTAIN_DELEGATION_TOKEN)) {
          log.error(principal + " doesn't have the " + SystemPermission.OBTAIN_DELEGATION_TOKEN.name()
              + " SystemPermission neccesary to obtain a delegation token. MapReduce tasks cannot automatically use the client's"
              + " credentials on remote servers. Delegation tokens provide a means to run MapReduce without distributing the user's credentials.");
          throw new IllegalStateException(conn.whoami() + " does not have permission to obtain a delegation token");
        }

        // Fetch a delegation token from Accumulo
        token = conn.securityOperations().getDelegationToken(new DelegationTokenConfig());

      } catch (Exception e) {
        final String msg = "Failed to acquire DelegationToken for use with MapReduce";
        log.error(msg, e);
        throw new RuntimeException(msg, e);
      }
    } else {
      // Simple principal + password
      principal = args[0];
      token = new PasswordToken(args[1]);
    }

    AccumuloInputFormat.setConnectorInfo(job, principal, token);
    AccumuloOutputFormat.setConnectorInfo(job, principal, token);

    job.setMapperClass(SeqMapClass.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Mutation.class);

    job.setNumReduceTasks(0);

    job.setOutputFormatClass(AccumuloOutputFormat.class);
    AccumuloOutputFormat.setCreateTables(job, true);
    AccumuloOutputFormat.setDefaultTableName(job, args[5]);
    AccumuloOutputFormat.setZooKeeperInstance(job, clientConf);

    job.waitForCompletion(true);
    return job.isSuccessful() ? 0 : 1;
  }
}
