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
package org.apache.accumulo.examples.dirlist;

import java.io.IOException;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FileCountMR extends Configured implements Tool {
    private static final String OUTPUT_VIS = FileCountMR.class.getSimpleName() + ".output.vis";
    
    public static class FileCountMapper extends Mapper<Key,Value,Key,Value> {
        long dirCount = 0;
        long fileCount = 0;
        Text lastRowFound = null;
        String prefix;
        
        private void incrementCounts(Key k) {
            if (k.getColumnFamily().equals(QueryUtil.DIR_COLF)) dirCount++;
            else fileCount++;
        }
        
        // store last row found and its prefix, reset counts, and increment
        private void initVars(Key k) {
            lastRowFound = k.getRow();
            prefix = lastRowFound.toString();
            int slashIndex = prefix.lastIndexOf("/");
            if (slashIndex >= 0) prefix = prefix.substring(0, slashIndex + 1);
            dirCount = 0;
            fileCount = 0;
            incrementCounts(k);
        }
        
        @Override
        protected void map(Key key, Value value, Context context) throws IOException, InterruptedException {
            if (lastRowFound == null) {
                // handle first key/value
                initVars(key);
                return;
            }
            if (lastRowFound.equals(key.getRow())) {
                // skip additional info for the same row
                return;
            }
            if (key.getRow().getLength() > prefix.length() && prefix.equals(key.getRow().toString().substring(0, prefix.length()))) {
                // if this is a new row in the same dir, update last row found and increment counts
                lastRowFound = key.getRow();
                incrementCounts(key);
                return;
            }
            // got the first row of the next directory, output accumulated counts and reset
            cleanup(context);
            initVars(key);
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (lastRowFound == null) return;
            String lrf = lastRowFound.toString().substring(3);
            int slashIndex;
            int parentCount = 0;
            while ((slashIndex = lrf.lastIndexOf("/")) >= 0) {
                lrf = lrf.substring(0, slashIndex);
                context.write(
                        new Key(lrf),
                        new Value(StringArraySummation.longArrayToStringBytes(parentCount == 0 ? new Long[] {dirCount, fileCount, dirCount, fileCount}
                                : new Long[] {0l, 0l, dirCount, fileCount})));
                parentCount++;
            }
        }
    }
    
    public static class FileCountReducer extends Reducer<Key,Value,Text,Mutation> {
        ColumnVisibility colvis;
        StringArraySummation sas = new StringArraySummation();
        
        @Override
        protected void reduce(Key key, Iterable<Value> values, Context context) throws IOException, InterruptedException {
            sas.reset();
            for (Value v : values) {
                sas.collect(v);
            }
            Mutation m = new Mutation(QueryUtil.getRow(key.getRow().toString()));
            m.put(QueryUtil.DIR_COLF, QueryUtil.COUNTS_COLQ, colvis, sas.aggregate());
            context.write(null, m);
        }
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            colvis = new ColumnVisibility(context.getConfiguration().get(OUTPUT_VIS, ""));
        }
    }
    
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(CachedConfiguration.getInstance(), new FileCountMR(), args));
    }
    
    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 8) {
            System.out.println("usage: " + FileCountMR.class.getSimpleName()
                    + " <instance> <zoo> <user> <pass> <input table> <output table> <auths> <output visibility>");
            System.exit(1);
        }
        
        String instance = args[0];
        String zooKeepers = args[1];
        String user = args[2];
        String pass = args[3];
        String inputTable = args[4];
        String outputTable = args[5];
        Authorizations auths = new Authorizations(args[6].split(","));
        String colvis = args[7];
        
        Job job = new Job(getConf(), this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());
        
        job.setInputFormatClass(AccumuloInputFormat.class);
        AccumuloInputFormat.setInputInfo(job, user, pass.getBytes(), inputTable, auths);
        AccumuloInputFormat.setZooKeeperInstance(job, instance, zooKeepers);
        
        job.setMapperClass(FileCountMapper.class);
        job.setMapOutputKeyClass(Key.class);
        job.setMapOutputValueClass(Value.class);
        
        job.setCombinerClass(FileCountReducer.class);
        
        job.setReducerClass(FileCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Mutation.class);
        
        job.setOutputFormatClass(AccumuloOutputFormat.class);
        AccumuloOutputFormat.setOutputInfo(job, user, pass.getBytes(), true, outputTable);
        AccumuloOutputFormat.setZooKeeperInstance(job, instance, zooKeepers);
        
        job.setNumReduceTasks(5);
        
        job.getConfiguration().set(OUTPUT_VIS, colvis);
        
        job.waitForCompletion(true);
        return job.isSuccessful() ? 0 : 1;
    }
}
