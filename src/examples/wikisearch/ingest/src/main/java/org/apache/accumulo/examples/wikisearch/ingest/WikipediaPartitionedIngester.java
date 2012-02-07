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
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.examples.wikisearch.ingest.ArticleExtractor.Article;
import org.apache.accumulo.examples.wikisearch.iterator.GlobalIndexUidCombiner;
import org.apache.accumulo.examples.wikisearch.iterator.TextIndexCombiner;
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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WikipediaPartitionedIngester extends Configured implements Tool {
  
  public final static String INGEST_LANGUAGE = "wikipedia.ingest_language";
  public final static String SPLIT_FILE = "wikipedia.split_file";
  public final static String TABLE_NAME = "wikipedia.table";
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new WikipediaPartitionedIngester(), args);
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
      // Set a text index combiner on the given field names. No combiner is set if the option is not supplied
      String textIndexFamilies = WikipediaMapper.TOKENS_FIELD_NAME;
      
      tops.create(tableName);
      if (textIndexFamilies.length() > 0) {
        System.out.println("Adding content combiner on the fields: " + textIndexFamilies);
        
        IteratorSetting setting = new IteratorSetting(10, TextIndexCombiner.class);
        List<Column> columns = new ArrayList<Column>();
        for (String family : StringUtils.split(textIndexFamilies, ',')) {
          columns.add(new Column("fi\0" + family));
        }
        TextIndexCombiner.setColumns(setting, columns);
        TextIndexCombiner.setLossyness(setting, true);
        
        tops.attachIterator(tableName, setting, EnumSet.allOf(IteratorScope.class));
      }
      
      // Set the locality group for the full content column family
      tops.setLocalityGroups(tableName, Collections.singletonMap("WikipediaDocuments", Collections.singleton(new Text(WikipediaMapper.DOCUMENT_COLUMN_FAMILY))));
      
    }
    
    if (!tops.exists(indexTableName)) {
      tops.create(indexTableName);
      // Add the UID combiner
      IteratorSetting setting = new IteratorSetting(19, "UIDAggregator", GlobalIndexUidCombiner.class);
      GlobalIndexUidCombiner.setCombineAllColumns(setting, true);
      GlobalIndexUidCombiner.setLossyness(setting, true);
      tops.attachIterator(indexTableName, setting, EnumSet.allOf(IteratorScope.class));
    }
    
    if (!tops.exists(reverseIndexTableName)) {
      tops.create(reverseIndexTableName);
      // Add the UID combiner
      IteratorSetting setting = new IteratorSetting(19, "UIDAggregator", GlobalIndexUidCombiner.class);
      GlobalIndexUidCombiner.setCombineAllColumns(setting, true);
      GlobalIndexUidCombiner.setLossyness(setting, true);
      tops.attachIterator(reverseIndexTableName, setting, EnumSet.allOf(IteratorScope.class));
    }
    
    if (!tops.exists(metadataTableName)) {
      // Add the SummingCombiner with VARLEN encoding for the frequency column
      tops.create(metadataTableName);
      IteratorSetting setting = new IteratorSetting(10, SummingCombiner.class);
      SummingCombiner.setColumns(setting, Collections.singletonList(new Column("f")));
      SummingCombiner.setEncodingType(setting, SummingCombiner.Type.VARLEN);
      tops.attachIterator(metadataTableName, setting, EnumSet.allOf(IteratorScope.class));
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    if(WikipediaConfiguration.runPartitioner(conf))
    {
      int result = runPartitionerJob();
      if(result != 0)
        return result;
    }
    if(WikipediaConfiguration.runIngest(conf))
      return runIngestJob();
    return 0;
  }
  
  public int runPartitionerJob() throws Exception
  {
    Job partitionerJob = new Job(getConf(), "Partition Wikipedia");
    Configuration partitionerConf = partitionerJob.getConfiguration();
    partitionerConf.set("mapred.map.tasks.speculative.execution", "false");

    configurePartitionerJob(partitionerJob);
    
    List<Path> inputPaths = new ArrayList<Path>();
    SortedSet<String> languages = new TreeSet<String>();
    FileSystem fs = FileSystem.get(partitionerConf);
    Path parent = new Path(partitionerConf.get("wikipedia.input"));
    listFiles(parent, fs, inputPaths, languages);
    
    System.out.println("Input files in " + parent + ":" + inputPaths.size());
    Path[] inputPathsArray = new Path[inputPaths.size()];
    inputPaths.toArray(inputPathsArray);
    
    System.out.println("Languages:" + languages.size());

    // setup input format
    
    WikipediaInputFormat.setInputPaths(partitionerJob, inputPathsArray);
    
    partitionerJob.setMapperClass(WikipediaPartitioner.class);
    partitionerJob.setNumReduceTasks(0);

    // setup output format
    partitionerJob.setMapOutputKeyClass(Text.class);
    partitionerJob.setMapOutputValueClass(Article.class);
    partitionerJob.setOutputFormatClass(SequenceFileOutputFormat.class);
    Path outputDir = WikipediaConfiguration.getPartitionedArticlesPath(partitionerConf);
    SequenceFileOutputFormat.setOutputPath(partitionerJob, outputDir);
    
    return partitionerJob.waitForCompletion(true) ? 0 : 1;
  }
  
  public int runIngestJob() throws Exception
  {
    Job ingestJob = new Job(getConf(), "Ingest Partitioned Wikipedia");
    Configuration ingestConf = ingestJob.getConfiguration();
    ingestConf.set("mapred.map.tasks.speculative.execution", "false");

    String tablename = WikipediaConfiguration.getTableName(ingestConf);
    
    String zookeepers = WikipediaConfiguration.getZookeepers(ingestConf);
    String instanceName = WikipediaConfiguration.getInstanceName(ingestConf);
    
    String user = WikipediaConfiguration.getUser(ingestConf);
    byte[] password = WikipediaConfiguration.getPassword(ingestConf);
    Connector connector = WikipediaConfiguration.getConnector(ingestConf);
    
    TableOperations tops = connector.tableOperations();
    
    createTables(tops, tablename);
    
    // setup input format
    ingestJob.setInputFormatClass(SequenceFileInputFormat.class);
    SequenceFileInputFormat.setInputPaths(ingestJob, WikipediaConfiguration.getPartitionedArticlesPath(ingestConf));

    // setup output format
    ingestJob.setMapOutputKeyClass(Text.class);
    ingestJob.setMapOutputValueClass(Mutation.class);
    ingestJob.setOutputFormatClass(AccumuloOutputFormat.class);
    AccumuloOutputFormat.setOutputInfo(ingestJob.getConfiguration(), user, password, true, tablename);
    AccumuloOutputFormat.setZooKeeperInstance(ingestJob.getConfiguration(), instanceName, zookeepers);
    
    return ingestJob.waitForCompletion(true) ? 0 : 1;
  }
  
  public final static PathFilter partFilter = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return path.getName().startsWith("part");
    };
  };
  
  protected void configurePartitionerJob(Job job) {
    Configuration conf = job.getConfiguration();
    job.setJarByClass(WikipediaPartitionedIngester.class);
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
