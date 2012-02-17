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
/**
 * 
 */
package org.apache.accumulo.examples.wikisearch.ingest;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.examples.wikisearch.ingest.ArticleExtractor.Article;
import org.apache.accumulo.examples.wikisearch.ingest.WikipediaInputFormat.WikipediaInputSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WikipediaPartitioner extends Mapper<LongWritable,Text,Text,Article> {
  
  // private static final Logger log = Logger.getLogger(WikipediaPartitioner.class);
  
  public final static Charset UTF8 = Charset.forName("UTF-8");
  public static final String DOCUMENT_COLUMN_FAMILY = "d";
  public static final String METADATA_EVENT_COLUMN_FAMILY = "e";
  public static final String METADATA_INDEX_COLUMN_FAMILY = "i";
  public static final String TOKENS_FIELD_NAME = "TEXT";
  
  private final static Pattern languagePattern = Pattern.compile("([a-z_]+).*.xml(.bz2)?");
  
  private ArticleExtractor extractor;
  private String language;

  private int myGroup = -1;
  private int numGroups = -1;
  
  @Override
  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    
    WikipediaInputSplit wiSplit = (WikipediaInputSplit)context.getInputSplit();
    myGroup = wiSplit.getPartition();
    numGroups = WikipediaConfiguration.getNumGroups(conf);
    
    FileSplit split = wiSplit.getFileSplit();
    String fileName = split.getPath().getName();
    Matcher matcher = languagePattern.matcher(fileName);
    if (matcher.matches()) {
      language = matcher.group(1).replace('_', '-').toLowerCase();
    } else {
      throw new RuntimeException("Unknown ingest language! " + fileName);
    }
    extractor = new ArticleExtractor();
  }
  
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    Article article = extractor.extract(new InputStreamReader(new ByteArrayInputStream(value.getBytes()), UTF8));
    if (article != null) {
      int groupId = WikipediaMapper.getPartitionId(article, numGroups);
      if(groupId != myGroup)
        return;
      context.write(new Text(language), article);
    } else {
      context.getCounter("wikipedia", "invalid articles").increment(1);
      context.progress();
    }
  }
  
}
