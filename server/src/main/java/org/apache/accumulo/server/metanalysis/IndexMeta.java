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
package org.apache.accumulo.server.metanalysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.cli.ClientOpts;
import org.apache.accumulo.server.logger.LogEvents;
import org.apache.accumulo.server.logger.LogFileKey;
import org.apache.accumulo.server.logger.LogFileValue;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.beust.jcommander.Parameter;

/**
 * A map reduce job that takes write ahead logs containing mutations for the metadata table and indexes them into Accumulo tables for analysis.
 * 
 */

public class IndexMeta extends Configured implements Tool {
  
  public static class IndexMapper extends Mapper<LogFileKey,LogFileValue,Text,Mutation> {
    private static final Text CREATE_EVENTS_TABLE = new Text("createEvents");
    private static final Text TABLET_EVENTS_TABLE = new Text("tabletEvents");
    private Map<Integer,KeyExtent> tabletIds = new HashMap<Integer,KeyExtent>();
    private String uuid = null;
    
    @Override
    protected void setup(Context context) throws java.io.IOException, java.lang.InterruptedException {
      tabletIds = new HashMap<Integer,KeyExtent>();
      uuid = null;
    }
    
    @Override
    public void map(LogFileKey key, LogFileValue value, Context context) throws IOException, InterruptedException {
      if (key.event == LogEvents.OPEN) {
        uuid = key.tserverSession;
      } else if (key.event == LogEvents.DEFINE_TABLET) {
        if (key.tablet.getTableId().toString().equals(MetadataTable.ID)) {
          tabletIds.put(key.tid, new KeyExtent(key.tablet));
        }
      } else if ((key.event == LogEvents.MUTATION || key.event == LogEvents.MANY_MUTATIONS) && tabletIds.containsKey(key.tid)) {
        for (Mutation m : value.mutations) {
          index(context, m, uuid, tabletIds.get(key.tid));
        }
      }
    }
    
    void index(Context context, Mutation m, String logFile, KeyExtent metaTablet) throws IOException, InterruptedException {
      List<ColumnUpdate> columnsUpdates = m.getUpdates();
      
      Text prevRow = null;
      long timestamp = 0;
      
      if (m.getRow().length > 0 && m.getRow()[0] == '~') {
        return;
      }
      
      for (ColumnUpdate cu : columnsUpdates) {
        if (TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.equals(new Text(cu.getColumnFamily()), new Text(cu.getColumnQualifier())) && !cu.isDeleted()) {
          prevRow = new Text(cu.getValue());
        }
        
        timestamp = cu.getTimestamp();
      }
      
      byte[] serMut = WritableUtils.toByteArray(m);
      
      if (prevRow != null) {
        Mutation createEvent = new Mutation(new Text(m.getRow()));
        createEvent.put(prevRow, new Text(String.format("%020d", timestamp)), new Value(metaTablet.toString().getBytes()));
        context.write(CREATE_EVENTS_TABLE, createEvent);
      }
      
      Mutation tabletEvent = new Mutation(new Text(m.getRow()));
      tabletEvent.put(new Text(String.format("%020d", timestamp)), new Text("mut"), new Value(serMut));
      tabletEvent.put(new Text(String.format("%020d", timestamp)), new Text("mtab"), new Value(metaTablet.toString().getBytes()));
      tabletEvent.put(new Text(String.format("%020d", timestamp)), new Text("log"), new Value(logFile.getBytes()));
      context.write(TABLET_EVENTS_TABLE, tabletEvent);
    }
  }
  
  static class Opts extends ClientOpts {
    @Parameter(description = "<logfile> { <logfile> ...}")
    List<String> logFiles = new ArrayList<String>();
  }
  
  @Override
  public int run(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(IndexMeta.class.getName(), args);
    
    String jobName = this.getClass().getSimpleName() + "_" + System.currentTimeMillis();
    
    Job job = new Job(getConf(), jobName);
    job.setJarByClass(this.getClass());
    
    List<String> logFiles = Arrays.asList(args).subList(4, args.length);
    Path paths[] = new Path[logFiles.size()];
    int count = 0;
    for (String logFile : logFiles) {
      paths[count++] = new Path(logFile);
    }
    
    job.setInputFormatClass(LogFileInputFormat.class);
    LogFileInputFormat.setInputPaths(job, paths);
    
    job.setNumReduceTasks(0);
    
    job.setOutputFormatClass(AccumuloOutputFormat.class);
    AccumuloOutputFormat.setZooKeeperInstance(job, opts.instance, opts.zookeepers);
    AccumuloOutputFormat.setConnectorInfo(job, opts.principal, opts.getToken());
    AccumuloOutputFormat.setCreateTables(job, false);
    
    job.setMapperClass(IndexMapper.class);
    
    Connector conn = opts.getConnector();
    
    try {
      conn.tableOperations().create("createEvents");
    } catch (TableExistsException tee) {
      Logger.getLogger(IndexMeta.class).warn("Table createEvents exists");
    }
    
    try {
      conn.tableOperations().create("tabletEvents");
    } catch (TableExistsException tee) {
      Logger.getLogger(IndexMeta.class).warn("Table tabletEvents exists");
    }
    
    job.waitForCompletion(true);
    return job.isSuccessful() ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(CachedConfiguration.getInstance(), new IndexMeta(), args);
    System.exit(res);
  }
}
