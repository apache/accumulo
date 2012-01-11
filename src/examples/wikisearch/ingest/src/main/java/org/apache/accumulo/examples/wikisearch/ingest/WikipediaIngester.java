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
package org.apache.accumulo.examples.wikisearch.ingest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.aggregation.NumSummation;
import org.apache.accumulo.core.iterators.aggregation.conf.AggregatorConfiguration;
import org.apache.accumulo.examples.wikisearch.reader.AggregatingRecordReader;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


@SuppressWarnings("deprecation")
public class WikipediaIngester extends Configured implements Tool {
  
  public final static String INGEST_LANGUAGE = "wikipedia.ingest_language";
  public final static String SPLIT_FILE = "wikipedia.split_file";
  public final static String TABLE_NAME = "wikipedia.table";
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new WikipediaIngester(), args);
    System.exit(res);
  }
  
  private void createTables(TableOperations tops, String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      TableExistsException {
    // Create the shard table
    String indexTableName = tableName + "Index";
    String reverseIndexTableName = tableName + "ReverseIndex";
    String metadataTableName = tableName + "Metadata";
    
    // create the shard table
    if (!tops.exists(tableName)) {
      // Set a text index aggregator on the given field names. No aggregator is set if the option is not supplied
      String textIndexFamilies = WikipediaMapper.TOKENS_FIELD_NAME;
      
      if (textIndexFamilies.length() > 0) {
        System.out.println("Adding content aggregator on the fields: " + textIndexFamilies);
        
        // Create and set the aggregators in one shot
        List<AggregatorConfiguration> aggregators = new ArrayList<AggregatorConfiguration>();
        
        for (String family : StringUtils.split(textIndexFamilies, ',')) {
          aggregators.add(new AggregatorConfiguration(new Text("fi\0" + family), org.apache.accumulo.examples.wikisearch.aggregator.TextIndexAggregator.class.getName()));
        }
        
        tops.create(tableName);
        tops.addAggregators(tableName, aggregators);
      } else {
        tops.create(tableName);
      }
      
      // Set the locality group for the full content column family
      tops.setLocalityGroups(tableName, Collections.singletonMap("WikipediaDocuments", Collections.singleton(new Text(WikipediaMapper.DOCUMENT_COLUMN_FAMILY))));
      
    }
    
    if (!tops.exists(indexTableName)) {
      tops.create(indexTableName);
      // Add the UID aggregator
      for (IteratorScope scope : IteratorScope.values()) {
        String stem = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scope.name(), "UIDAggregator");
        tops.setProperty(indexTableName, stem, "19,org.apache.accumulo.examples.wikisearch.iterator.TotalAggregatingIterator");
        stem += ".opt.";
        tops.setProperty(indexTableName, stem + "*", "org.apache.accumulo.examples.wikisearch.aggregator.GlobalIndexUidAggregator");
        
      }
    }
    
    if (!tops.exists(reverseIndexTableName)) {
      tops.create(reverseIndexTableName);
      // Add the UID aggregator
      for (IteratorScope scope : IteratorScope.values()) {
        String stem = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scope.name(), "UIDAggregator");
        tops.setProperty(reverseIndexTableName, stem, "19,org.apache.accumulo.examples.wikisearch.iterator.TotalAggregatingIterator");
        stem += ".opt.";
        tops.setProperty(reverseIndexTableName, stem + "*", "org.apache.accumulo.examples.wikisearch.aggregator.GlobalIndexUidAggregator");
        
      }
    }
    
    if (!tops.exists(metadataTableName)) {
      // Add the NumSummation aggregator for the frequency column
      List<AggregatorConfiguration> aggregators = new ArrayList<AggregatorConfiguration>();
      aggregators.add(new AggregatorConfiguration(new Text("f"), NumSummation.class.getName()));
      tops.create(metadataTableName);
      tops.addAggregators(metadataTableName, aggregators);
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    Job job = new Job(getConf(), "Ingest Wikipedia");
    Configuration conf = job.getConfiguration();
    conf.set("mapred.map.tasks.speculative.execution", "false");

    String tablename = WikipediaConfiguration.getTableName(conf);
    
    String zookeepers = WikipediaConfiguration.getZookeepers(conf);
    String instanceName = WikipediaConfiguration.getInstanceName(conf);
    
    String user = WikipediaConfiguration.getUser(conf);
    byte[] password = WikipediaConfiguration.getPassword(conf);
    Connector connector = WikipediaConfiguration.getConnector(conf);
    
    TableOperations tops = connector.tableOperations();
    
    createTables(tops, tablename);
    
    configureJob(job);
    
    List<Path> inputPaths = new ArrayList<Path>();
    SortedSet<String> languages = new TreeSet<String>();
    FileSystem fs = FileSystem.get(conf);
    Path parent = new Path(conf.get("wikipedia.input"));
    listFiles(parent, fs, inputPaths, languages);
    
    System.out.println("Input files in " + parent + ":" + inputPaths.size());
    Path[] inputPathsArray = new Path[inputPaths.size()];
    inputPaths.toArray(inputPathsArray);
    
    System.out.println("Languages:" + languages.size());
    
    FileInputFormat.setInputPaths(job, inputPathsArray);
    
    job.setMapperClass(WikipediaMapper.class);
    job.setNumReduceTasks(0);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Mutation.class);
    job.setOutputFormatClass(AccumuloOutputFormat.class);
    AccumuloOutputFormat.setOutputInfo(job, user, password, true, tablename);
    AccumuloOutputFormat.setZooKeeperInstance(job, instanceName, zookeepers);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public final static PathFilter partFilter = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return path.getName().startsWith("part");
    };
  };
  
  protected void configureJob(Job job) {
    Configuration conf = job.getConfiguration();
    job.setJarByClass(WikipediaIngester.class);
    job.setInputFormatClass(WikipediaInputFormat.class);
    conf.set(AggregatingRecordReader.START_TOKEN, "<page>");
    conf.set(AggregatingRecordReader.END_TOKEN, "</page>");
  }
  
  protected static final Pattern filePattern = Pattern.compile("([a-z_]+).*.xml(.bz2)?");
  
  protected void listFiles(Path path, FileSystem fs, List<Path> files, Set<String> languages) throws IOException {
    for (FileStatus status : fs.listStatus(path)) {
      if (status.isDir()) {
        listFiles(status.getPath(), fs, files, languages);
      } else {
        Path p = status.getPath();
        Matcher matcher = filePattern.matcher(p.getName());
        if (matcher.matches()) {
          languages.add(matcher.group(1));
          files.add(p);
        }
      }
    }
  }
}
