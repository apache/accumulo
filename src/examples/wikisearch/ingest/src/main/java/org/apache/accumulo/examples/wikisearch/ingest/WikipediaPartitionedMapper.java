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
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.examples.wikisearch.ingest.ArticleExtractor.Article;
import org.apache.accumulo.examples.wikisearch.normalizer.LcNoDiacriticsNormalizer;
import org.apache.accumulo.examples.wikisearch.protobuf.Uid;
import org.apache.accumulo.examples.wikisearch.protobuf.Uid.List.Builder;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.wikipedia.analysis.WikipediaTokenizer;

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
  
  @Override
  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    tablename = new Text(WikipediaConfiguration.getTableName(conf));
    indexTableName = new Text(tablename + "Index");
    reverseIndexTableName = new Text(tablename + "ReverseIndex");
    metadataTableName = new Text(tablename + "Metadata");
    
    numPartitions = WikipediaConfiguration.getNumPartitions(conf);
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
        Mutation mm = new Mutation(entry.getKey());
        mm.put(METADATA_EVENT_COLUMN_FAMILY, language.toString(), cv, article.getTimestamp(), NULL_VALUE);
        context.write(metadataTableName, mm);
      }
      
      // Tokenize the content
      Set<String> tokens = getTokens(article);
      
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
        // Create a UID object for the Value
        Builder uidBuilder = Uid.List.newBuilder();
        uidBuilder.setIGNORE(false);
        uidBuilder.setCOUNT(1);
        uidBuilder.addUID(Integer.toString(article.getId()));
        Uid.List uidList = uidBuilder.build();
        Value val = new Value(uidList.toByteArray());
        
        // Create mutations for the global index
        // Row is field value, colf is field name, colq is partitionid\0language, value is Uid.List object
        Mutation gm = new Mutation(index.getValue());
        gm.put(index.getKey(), partitionId + NULL_BYTE + language, cv, article.getTimestamp(), val);
        context.write(indexTableName, gm);
        
        // Create mutations for the global reverse index
        Mutation grm = new Mutation(StringUtils.reverse(index.getValue()));
        grm.put(index.getKey(), partitionId + NULL_BYTE + language, cv, article.getTimestamp(), val);
        context.write(reverseIndexTableName, grm);
        
        // Create mutations for the metadata table.
        Mutation mm = new Mutation(index.getKey());
        mm.put(METADATA_INDEX_COLUMN_FAMILY, language + NULL_BYTE + LcNoDiacriticsNormalizer.class.getName(), cv, article.getTimestamp(), NULL_VALUE);
        context.write(metadataTableName, mm);
        
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
  
  /**
   * Tokenize the wikipedia content
   * 
   * @param article
   * @return
   * @throws IOException
   */
  private Set<String> getTokens(Article article) throws IOException {
    Set<String> tokenList = new HashSet<String>();
    WikipediaTokenizer tok = new WikipediaTokenizer(new StringReader(article.getText()));
    TermAttribute term = tok.addAttribute(TermAttribute.class);
    try {
      while (tok.incrementToken()) {
        String token = term.term();
        if (!StringUtils.isEmpty(token))
          tokenList.add(token);
      }
    } catch (IOException e) {
      log.error("Error tokenizing text", e);
    } finally {
      try {
        tok.end();
      } catch (IOException e) {
        log.error("Error calling end()", e);
      } finally {
        try {
          tok.close();
        } catch (IOException e) {
          log.error("Error closing tokenizer", e);
        }
      }
    }
    return tokenList;
  }
  
}
