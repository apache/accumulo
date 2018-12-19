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
package org.apache.accumulo.test.mapreduce;

import java.io.IOException;
import java.util.Base64;
import java.util.Collections;

import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.DelegationTokenConfig;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

/**
 * This class supports deprecated mapreduce code in core jar
 */
@Deprecated
public class RowHash extends Configured implements Tool {
  /**
   * The Mapper class that given a row number, will generate the appropriate output line.
   */
  public static class HashDataMapper extends Mapper<Key,Value,Text,Mutation> {
    @Override
    public void map(Key row, Value data, Context context) throws IOException, InterruptedException {
      Mutation m = new Mutation(row.getRow());
      m.put(new Text("cf-HASHTYPE"), new Text("cq-MD5BASE64"),
          new Value(Base64.getEncoder().encode(MD5Hash.digest(data.toString()).getDigest())));
      context.write(null, m);
      context.progress();
    }

    @Override
    public void setup(Context job) {}
  }

  private static class Opts extends ClientOpts {
    private static final Logger log = LoggerFactory.getLogger(Opts.class);

    @Parameter(names = "--column", required = true)
    String column;

    @Parameter(names = {"-t", "--table"}, required = true, description = "table to use")
    private String tableName;

    public String getTableName() {
      return tableName;
    }

    public void setAccumuloConfigs(Job job) {
      org.apache.accumulo.core.clientImpl.mapreduce.lib.InputConfigurator.setClientProperties(
          org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.class,
          job.getConfiguration(), this.getClientProperties());
      org.apache.accumulo.core.clientImpl.mapreduce.lib.OutputConfigurator.setClientProperties(
          org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat.class,
          job.getConfiguration(), this.getClientProperties());
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setInputTableName(job,
          getTableName());
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setScanAuthorizations(job,
          auths);
      org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat.setCreateTables(job, true);
      org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat.setDefaultTableName(job,
          getTableName());
    }

    @Override
    public AuthenticationToken getToken() {
      AuthenticationToken authToken = super.getToken();
      // For MapReduce, Kerberos credentials don't make it to the Mappers and Reducers,
      // so we need to request a delegation token and use that instead.
      if (authToken instanceof KerberosToken) {
        log.info("Received KerberosToken, fetching DelegationToken for MapReduce");
        final KerberosToken krbToken = (KerberosToken) authToken;

        try {
          UserGroupInformation user = UserGroupInformation.getCurrentUser();
          if (!user.hasKerberosCredentials()) {
            throw new IllegalStateException("Expected current user to have Kerberos credentials");
          }

          String newPrincipal = user.getUserName();
          log.info("Obtaining delegation token for {}", newPrincipal);

          setPrincipal(newPrincipal);
          AccumuloClient client = Accumulo.newClient().from(getClientProperties())
              .as(newPrincipal, krbToken).build();

          // Do the explicit check to see if the user has the permission to get a delegation token
          if (!client.securityOperations().hasSystemPermission(client.whoami(),
              SystemPermission.OBTAIN_DELEGATION_TOKEN)) {
            log.error(
                "{} doesn't have the {} SystemPermission neccesary to obtain a delegation"
                    + " token. MapReduce tasks cannot automatically use the client's"
                    + " credentials on remote servers. Delegation tokens provide a means to run"
                    + " MapReduce without distributing the user's credentials.",
                user.getUserName(), SystemPermission.OBTAIN_DELEGATION_TOKEN.name());
            throw new IllegalStateException(
                client.whoami() + " does not have permission to obtain a delegation token");
          }

          // Get the delegation token from Accumulo
          return client.securityOperations().getDelegationToken(new DelegationTokenConfig());
        } catch (Exception e) {
          final String msg = "Failed to acquire DelegationToken for use with MapReduce";
          log.error(msg, e);
          throw new RuntimeException(msg, e);
        }
      }
      return authToken;
    }

  }

  @Override
  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName(this.getClass().getName());
    job.setJarByClass(this.getClass());
    Opts opts = new Opts();
    opts.parseArgs(RowHash.class.getName(), args);
    job.setInputFormatClass(org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.class);
    opts.setAccumuloConfigs(job);

    String col = opts.column;
    int idx = col.indexOf(":");
    Text cf = new Text(idx < 0 ? col : col.substring(0, idx));
    Text cq = idx < 0 ? null : new Text(col.substring(idx + 1));
    if (cf.getLength() > 0)
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.fetchColumns(job,
          Collections.singleton(new Pair<>(cf, cq)));

    job.setMapperClass(HashDataMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Mutation.class);

    job.setNumReduceTasks(0);

    job.setOutputFormatClass(org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat.class);

    job.waitForCompletion(true);
    return job.isSuccessful() ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new RowHash(), args);
  }
}
