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
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.logger.LogEvents;
import org.apache.accumulo.server.logger.LogFileKey;
import org.apache.accumulo.server.logger.LogFileValue;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A map reduce job that takes a set of walogs and filters out all non metadata table events.
 */
public class FilterMeta extends Configured implements Tool {
  
  public static class FilterMapper extends Mapper<LogFileKey,LogFileValue,LogFileKey,LogFileValue> {
    private Set<Integer> tabletIds;
    
    @Override
    protected void setup(Context context) throws java.io.IOException, java.lang.InterruptedException {
      tabletIds = new HashSet<Integer>();
    }
    
    @Override
    public void map(LogFileKey key, LogFileValue value, Context context) throws IOException, InterruptedException {
      if (key.event == LogEvents.OPEN) {
        context.write(key, value);
      } else if (key.event == LogEvents.DEFINE_TABLET && key.tablet.getTableId().toString().equals(MetadataTable.ID)) {
        tabletIds.add(key.tid);
        context.write(key, value);
      } else if ((key.event == LogEvents.MUTATION || key.event == LogEvents.MANY_MUTATIONS) && tabletIds.contains(key.tid)) {
        context.write(key, value);
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    
    String jobName = this.getClass().getSimpleName() + "_" + System.currentTimeMillis();
    
    Job job = new Job(getConf(), jobName);
    job.setJarByClass(this.getClass());
    
    Path paths[] = new Path[args.length - 1];
    for (int i = 0; i < paths.length; i++) {
      paths[i] = new Path(args[i]);
    }

    job.setInputFormatClass(LogFileInputFormat.class);
    LogFileInputFormat.setInputPaths(job, paths);
    
    job.setOutputFormatClass(LogFileOutputFormat.class);
    LogFileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));

    job.setMapperClass(FilterMapper.class);
    
    job.setNumReduceTasks(0);

    job.waitForCompletion(true);
    return job.isSuccessful() ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(CachedConfiguration.getInstance(), new FilterMeta(), args);
    System.exit(res);
  }
}
