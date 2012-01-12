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
package org.apache.accumulo.examples.wikisearch.logic;

import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.examples.wikisearch.ingest.WikipediaMapper;
import org.apache.accumulo.examples.wikisearch.sample.Document;
import org.apache.accumulo.examples.wikisearch.sample.Field;
import org.apache.accumulo.examples.wikisearch.sample.Results;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;


/**
 * This query table implementation returns a Results object that contains documents from the wiki table. The query will contain the partition id, wikitype, and
 * UID so that we can seek directly to the document. The document is stored as base64 compressed binary in the Accumulo table. We will decompress the data so
 * that it is base64 encoded binary data in the Results object.
 * 
 * The query that needs to be passed to the web service is: DOCUMENT:partitionId/wikitype/uid.
 * 
 */
public class ContentLogic {
  
  private static final Logger log = Logger.getLogger(ContentLogic.class);
  
  private static final String NULL_BYTE = "\u0000";
  
  private String tableName = null;
  
  private Pattern queryPattern = Pattern.compile("^DOCUMENT:(.*)/(.*)/(.*)$");
  
  public String getTableName() {
    return tableName;
  }
  
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }
  
  public Results runQuery(Connector connector, String query, List<String> authorizations) {
    
    Results results = new Results();
    Authorizations auths = new Authorizations(StringUtils.join(authorizations, "|"));
    
    Matcher match = queryPattern.matcher(query);
    if (!match.matches()) {
      throw new IllegalArgumentException("Query does not match the pattern: DOCUMENT:partitionId/wikitype/uid, your query: " + query.toString());
    } else {
      String partitionId = match.group(1);
      String wikitype = match.group(2);
      String id = match.group(3);
      
      log.debug("Received pieces: " + partitionId + ", " + wikitype + ", " + id);
      
      // Create the Range
      Key startKey = new Key(partitionId, WikipediaMapper.DOCUMENT_COLUMN_FAMILY, wikitype + NULL_BYTE + id);
      Key endKey = new Key(partitionId, WikipediaMapper.DOCUMENT_COLUMN_FAMILY, wikitype + NULL_BYTE + id + NULL_BYTE);
      Range r = new Range(startKey, true, endKey, false);
      
      log.debug("Setting range: " + r);
      
      try {
        Scanner scanner = connector.createScanner(this.getTableName(), auths);
        scanner.setRange(r);
        // This should in theory only match one thing.
        for (Entry<Key,Value> entry : scanner) {
          Document doc = new Document();
          doc.setId(id);
          Field val = new Field();
          val.setFieldName("DOCUMENT");
          val.setFieldValue(new String(Base64.decodeBase64(entry.getValue().toString())));
          doc.getFields().add(val);
          results.getResults().add(doc);
        }
      } catch (TableNotFoundException e) {
        throw new RuntimeException("Table not found: " + this.getTableName(), e);
      }
      
    }
    return results;
  }
  
}
