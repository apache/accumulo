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


import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.examples.wikisearch.ingest.ArticleExtractor.Article;
import org.apache.accumulo.examples.wikisearch.iterator.GlobalIndexUidCombiner;
import org.apache.accumulo.examples.wikisearch.normalizer.LcNoDiacriticsNormalizer;
import org.apache.accumulo.examples.wikisearch.protobuf.Uid;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class WikipediaPartitionedMapper extends Mapper<Text,Article,Text,Mutation> {
  
  private static final Logger log = Logger.getLogger(WikipediaPartitionedMapper.class);
  
  public final static Charset UTF8 = Charset.forName("UTF-8");
  public static final String DOCUMENT_COLUMN_FAMILY = "d";
  public static final String METADATA_EVENT_COLUMN_FAMILY = "e";
  public static final String METADATA_INDEX_COLUMN_FAMILY = "i";
  public static final String TOKENS_FIELD_NAME = "TEXT";
  
  private static final Value NULL_VALUE = new Value(new byte[0]);
  private static final String cvPrefix = "all|";
  
  private int numPartitions = 0;

  private Text tablename = null;
  private Text indexTableName = null;
  private Text reverseIndexTableName = null;
  private Text metadataTableName = null;
  
  private static class MutationInfo {
    final String row;
    final String colfam;
    final String colqual;
    final ColumnVisibility cv;
    final long timestamp;
    
    public MutationInfo(String row, String colfam, String colqual, ColumnVisibility cv, long timestamp) {
      super();
      this.row = row;
      this.colfam = colfam;
      this.colqual = colqual;
      this.cv = cv;
      this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object obj) {
      MutationInfo other = (MutationInfo)obj;
      return (row == other.row || row.equals(other.row)) &&
          (colfam == other.colfam || colfam.equals(other.colfam)) &&
          colqual.equals(other.colqual) &&
          (cv == other.cv || cv.equals(other.cv)) &&
          timestamp == other.timestamp;
    }

    @Override
    public int hashCode() {
      return row.hashCode() ^ colfam.hashCode() ^ colqual.hashCode() ^ cv.hashCode() ^ (int)timestamp;
    }
  }
  
  private LRUOutputCombiner<MutationInfo,CountAndSet> wikiIndexOutput;
  private LRUOutputCombiner<MutationInfo,CountAndSet> wikiReverseIndexOutput;
  private LRUOutputCombiner<MutationInfo,Value> wikiMetadataOutput;
  
  private static class CountAndSet
  {
    public int count;
    public HashSet<String> set;
    
    public CountAndSet(String entry)
    {
      set = new HashSet<String>();
      set.add(entry);
      count = 1;
    }
  }
  
  MultiTableBatchWriter mtbw;

  @Override
  public void setup(final Context context) {
    Configuration conf = context.getConfiguration();
    tablename = new Text(WikipediaConfiguration.getTableName(conf));
    indexTableName = new Text(tablename + "Index");
    reverseIndexTableName = new Text(tablename + "ReverseIndex");
    metadataTableName = new Text(tablename + "Metadata");
    
    try {
      mtbw = WikipediaConfiguration.getConnector(conf).createMultiTableBatchWriter(10000000, 1000, 10);
    } catch (AccumuloException e) {
      throw new RuntimeException(e);
    } catch (AccumuloSecurityException e) {
      throw new RuntimeException(e);
    }
    
    final Text metadataTableNameFinal = metadataTableName;
    final Text indexTableNameFinal = indexTableName;
    final Text reverseIndexTableNameFinal = reverseIndexTableName;
    
    numPartitions = WikipediaConfiguration.getNumPartitions(conf);

    LRUOutputCombiner.Fold<CountAndSet> indexFold = 
        new LRUOutputCombiner.Fold<CountAndSet>() {
      @Override
      public CountAndSet fold(CountAndSet oldValue, CountAndSet newValue) {
        oldValue.count += newValue.count;
        if(oldValue.set == null || newValue.set == null)
        {
          oldValue.set = null;
          return oldValue;
        }
        oldValue.set.addAll(newValue.set);
        if(oldValue.set.size() > GlobalIndexUidCombiner.MAX)
          oldValue.set = null;
        return oldValue;
      }
    };
    LRUOutputCombiner.Output<MutationInfo,CountAndSet> indexOutput =
        new LRUOutputCombiner.Output<WikipediaPartitionedMapper.MutationInfo,CountAndSet>() {
      
      @Override
      public void output(MutationInfo key, CountAndSet value)
      {
          Uid.List.Builder builder = Uid.List.newBuilder();
          builder.setCOUNT(value.count);
          if (value.set == null) {
            builder.setIGNORE(true);
            builder.clearUID();
          } else {
            builder.setIGNORE(false);
            builder.addAllUID(value.set);
          }
          Uid.List list = builder.build();
          Value val = new Value(list.toByteArray());
          Mutation m = new Mutation(key.row);
          m.put(key.colfam, key.colqual, key.cv, key.timestamp, val);
          try {
            mtbw.getBatchWriter(indexTableNameFinal.toString()).addMutation(m);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
      }
    };
    LRUOutputCombiner.Output<MutationInfo,CountAndSet> reverseIndexOutput =
        new LRUOutputCombiner.Output<WikipediaPartitionedMapper.MutationInfo,CountAndSet>() {
      
      @Override
      public void output(MutationInfo key, CountAndSet value)
      {
          Uid.List.Builder builder = Uid.List.newBuilder();
          builder.setCOUNT(value.count);
          if (value.set == null) {
            builder.setIGNORE(true);
            builder.clearUID();
          } else {
            builder.setIGNORE(false);
            builder.addAllUID(value.set);
          }
          Uid.List list = builder.build();
          Value val = new Value(list.toByteArray());
          Mutation m = new Mutation(key.row);
          m.put(key.colfam, key.colqual, key.cv, key.timestamp, val);
          try {
            mtbw.getBatchWriter(reverseIndexTableNameFinal.toString()).addMutation(m);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
      }
    };
      
    wikiIndexOutput = new LRUOutputCombiner<WikipediaPartitionedMapper.MutationInfo,CountAndSet>(10000,indexFold,indexOutput);
    wikiReverseIndexOutput = new LRUOutputCombiner<WikipediaPartitionedMapper.MutationInfo,CountAndSet>(10000, indexFold,reverseIndexOutput);
    wikiMetadataOutput = new LRUOutputCombiner<WikipediaPartitionedMapper.MutationInfo,Value>(10000,
        new LRUOutputCombiner.Fold<Value>() {
          @Override
          public Value fold(Value oldValue, Value newValue) {
            return oldValue;
          }},
        new LRUOutputCombiner.Output<MutationInfo,Value>() {
          @Override
          public void output(MutationInfo key, Value value) {
            Mutation m = new Mutation(key.row);
            m.put(key.colfam, key.colqual, key.cv, key.timestamp, value);
            try {
              mtbw.getBatchWriter(metadataTableNameFinal.toString()).addMutation(m);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }});
  }
  
  
  
  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    wikiIndexOutput.flush();
    wikiMetadataOutput.flush();
    wikiReverseIndexOutput.flush();
    try {
      mtbw.close();
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
  }



  @Override
  protected void map(Text language, Article article, Context context) throws IOException, InterruptedException {
    String NULL_BYTE = "\u0000";
    String colfPrefix = language.toString() + NULL_BYTE;
    String indexPrefix = "fi" + NULL_BYTE;
    ColumnVisibility cv = new ColumnVisibility(cvPrefix + language);
    
    if (article != null) {
      Text partitionId = new Text(Integer.toString(WikipediaMapper.getPartitionId(article, numPartitions)));
      
      // Create the mutations for the document.
      // Row is partition id, colf is language0articleid, colq is fieldName\0fieldValue
      Mutation m = new Mutation(partitionId);
      for (Entry<String,Object> entry : article.getFieldValues().entrySet()) {
        m.put(colfPrefix + article.getId(), entry.getKey() + NULL_BYTE + entry.getValue().toString(), cv, article.getTimestamp(), NULL_VALUE);
        // Create mutations for the metadata table.
        MutationInfo mm = new MutationInfo(entry.getKey(), METADATA_EVENT_COLUMN_FAMILY, language.toString(), cv, article.getTimestamp());
        wikiMetadataOutput.put(mm, NULL_VALUE);
      }
      
      // Tokenize the content
      Set<String> tokens = WikipediaMapper.getTokens(article);
      
      // We are going to put the fields to be indexed into a multimap. This allows us to iterate
      // over the entire set once.
      Multimap<String,String> indexFields = HashMultimap.create();
      // Add the normalized field values
      LcNoDiacriticsNormalizer normalizer = new LcNoDiacriticsNormalizer();
      for (Entry<String,String> index : article.getNormalizedFieldValues().entrySet())
        indexFields.put(index.getKey(), index.getValue());
      // Add the tokens
      for (String token : tokens)
        indexFields.put(TOKENS_FIELD_NAME, normalizer.normalizeFieldValue("", token));
      
      for (Entry<String,String> index : indexFields.entries()) {
        // Create mutations for the in partition index
        // Row is partition id, colf is 'fi'\0fieldName, colq is fieldValue\0language\0article id
        m.put(indexPrefix + index.getKey(), index.getValue() + NULL_BYTE + colfPrefix + article.getId(), cv, article.getTimestamp(), NULL_VALUE);
        
        // Create mutations for the global index
        // Row is field value, colf is field name, colq is partitionid\0language, value is Uid.List object
        MutationInfo gm = new MutationInfo(index.getValue(),index.getKey(),partitionId + NULL_BYTE + language, cv, article.getTimestamp());
        wikiIndexOutput.put(gm, new CountAndSet(Integer.toString(article.getId())));
        
        // Create mutations for the global reverse index
        MutationInfo grm = new MutationInfo(StringUtils.reverse(index.getValue()),index.getKey(),partitionId + NULL_BYTE + language, cv, article.getTimestamp());
        wikiReverseIndexOutput.put(grm, new CountAndSet(Integer.toString(article.getId())));
        
        // Create mutations for the metadata table.
        MutationInfo mm = new MutationInfo(index.getKey(),METADATA_INDEX_COLUMN_FAMILY, language + NULL_BYTE + LcNoDiacriticsNormalizer.class.getName(), cv, article.getTimestamp());
        wikiMetadataOutput.put(mm, NULL_VALUE);
      }
      // Add the entire text to the document section of the table.
      // row is the partition, colf is 'd', colq is language\0articleid, value is Base64 encoded GZIP'd document
      m.put(DOCUMENT_COLUMN_FAMILY, colfPrefix + article.getId(), cv, article.getTimestamp(), new Value(Base64.encodeBase64(article.getText().getBytes())));
      context.write(tablename, m);
      
    } else {
      context.getCounter("wikipedia", "invalid articles").increment(1);
    }
    context.progress();
  }
}
