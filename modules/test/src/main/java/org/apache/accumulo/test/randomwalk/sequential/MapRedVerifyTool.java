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
package org.apache.accumulo.test.randomwalk.sequential;

import java.io.IOException;
import java.util.Iterator;

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
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

public class MapRedVerifyTool extends Configured implements Tool {
  protected final Logger log = Logger.getLogger(this.getClass());

  public static class SeqMapClass extends Mapper<Key,Value,NullWritable,IntWritable> {
    @Override
    public void map(Key row, Value data, Context output) throws IOException, InterruptedException {
      Integer num = Integer.valueOf(row.getRow().toString());
      output.write(NullWritable.get(), new IntWritable(num.intValue()));
    }
  }

  public static class SeqReduceClass extends Reducer<NullWritable,IntWritable,Text,Mutation> {
    @Override
    public void reduce(NullWritable ignore, Iterable<IntWritable> values, Context output) throws IOException, InterruptedException {
      Iterator<IntWritable> iterator = values.iterator();

      if (iterator.hasNext() == false) {
        return;
      }

      int start = iterator.next().get();
      int index = start;
      while (iterator.hasNext()) {
        int next = iterator.next().get();
        if (next != index + 1) {
          writeMutation(output, start, index);
          start = next;
        }
        index = next;
      }
      writeMutation(output, start, index);
    }

    public void writeMutation(Context output, int start, int end) throws IOException, InterruptedException {
      Mutation m = new Mutation(new Text(String.format("%010d", start)));
      m.put(new Text(String.format("%010d", end)), new Text(""), new Value(new byte[0]));
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

    ClientConfiguration clientConf = ClientConfiguration.loadDefault().withInstance(args[3]).withZkHosts(args[4]);

    AccumuloInputFormat.setInputTableName(job, args[2]);
    AccumuloInputFormat.setZooKeeperInstance(job, clientConf);
    AccumuloOutputFormat.setDefaultTableName(job, args[5]);
    AccumuloOutputFormat.setZooKeeperInstance(job, clientConf);

    job.setInputFormatClass(AccumuloInputFormat.class);
    if (clientConf.getBoolean(ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey(), false)) {
      // Better be logged in
      KerberosToken token = new KerberosToken();
      try {
        UserGroupInformation user = UserGroupInformation.getCurrentUser();
        if (!user.hasKerberosCredentials()) {
          throw new IllegalStateException("Expected current user to have Kerberos credentials");
        }

        String newPrincipal = user.getUserName();

        ZooKeeperInstance inst = new ZooKeeperInstance(clientConf);
        Connector conn = inst.getConnector(newPrincipal, token);

        // Do the explicit check to see if the user has the permission to get a delegation token
        if (!conn.securityOperations().hasSystemPermission(conn.whoami(), SystemPermission.OBTAIN_DELEGATION_TOKEN)) {
          log.error(newPrincipal + " doesn't have the " + SystemPermission.OBTAIN_DELEGATION_TOKEN.name()
              + " SystemPermission neccesary to obtain a delegation token. MapReduce tasks cannot automatically use the client's"
              + " credentials on remote servers. Delegation tokens provide a means to run MapReduce without distributing the user's credentials.");
          throw new IllegalStateException(conn.whoami() + " does not have permission to obtain a delegation token");
        }

        // Fetch a delegation token from Accumulo
        AuthenticationToken dt = conn.securityOperations().getDelegationToken(new DelegationTokenConfig());

        // Set the delegation token instead of the kerberos token
        AccumuloInputFormat.setConnectorInfo(job, newPrincipal, dt);
        AccumuloOutputFormat.setConnectorInfo(job, newPrincipal, dt);
      } catch (Exception e) {
        final String msg = "Failed to acquire DelegationToken for use with MapReduce";
        log.error(msg, e);
        throw new RuntimeException(msg, e);
      }
    } else {
      AccumuloInputFormat.setConnectorInfo(job, args[0], new PasswordToken(args[1]));
      AccumuloOutputFormat.setConnectorInfo(job, args[0], new PasswordToken(args[1]));
    }

    job.setMapperClass(SeqMapClass.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setReducerClass(SeqReduceClass.class);
    job.setNumReduceTasks(1);

    job.setOutputFormatClass(AccumuloOutputFormat.class);
    AccumuloOutputFormat.setCreateTables(job, true);

    job.waitForCompletion(true);
    return job.isSuccessful() ? 0 : 1;
  }
}
