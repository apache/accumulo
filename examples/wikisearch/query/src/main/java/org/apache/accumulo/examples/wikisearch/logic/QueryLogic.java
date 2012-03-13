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


import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.examples.wikisearch.iterator.EvaluatingIterator;
import org.apache.accumulo.examples.wikisearch.normalizer.LcNoDiacriticsNormalizer;
import org.apache.accumulo.examples.wikisearch.normalizer.Normalizer;
import org.apache.accumulo.examples.wikisearch.parser.QueryParser.QueryTerm;
import org.apache.accumulo.examples.wikisearch.parser.RangeCalculator;
import org.apache.accumulo.examples.wikisearch.protobuf.Uid;
import org.apache.accumulo.examples.wikisearch.util.TextUtil;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.Multimap;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * <pre>
 * <h2>Overview</h2>
 * QueryTable implementation that works with the JEXL grammar. This QueryTable
 * uses the metadata, global index, and partitioned table to return
 * results based on the query. Example queries:
 * 
 *  <b>Single Term Query</b>
 *  'foo' - looks in global index for foo, and if any entries are found, then the query
 *          is rewritten to be field1 == 'foo' or field2 == 'foo', etc. This is then passed
 *          down the optimized query path which uses the intersecting iterators on the shard
 *          table.
 * 
 *  <b>Boolean expression</b>        
 *  field == 'foo' - For fielded queries, those that contain a field, an operator, and a literal (string or number),
 *                   the query is parsed and the set of eventFields in the query that are indexed is determined by
 *                   querying the metadata table. Depending on the conjunctions in the query (or, and, not) and the
 *                   eventFields that are indexed, the query may be sent down the optimized path or the full scan path.
 * 
 *  We are not supporting all of the operators that JEXL supports at this time. We are supporting the following operators:
 * 
 *  ==, !=, &gt;, &ge;, &lt;, &le;, =~, and !~
 * 
 *  Custom functions can be created and registered with the Jexl engine. The functions can be used in the queries in conjunction
 *  with other supported operators. A sample function has been created, called between, and is bound to the 'f' namespace. An
 *  example using this function is : "f:between(LATITUDE,60.0, 70.0)"
 * 
 *  <h2>Constraints on Query Structure</h2>
 *  Queries that are sent to this class need to be formatted such that there is a space on either side of the operator. We are
 *  rewriting the query in some cases and the current implementation is expecting a space on either side of the operator. Users
 *  should also be aware that the literals used in the query need to match the data in the table. If an error occurs in the evaluation 
 *  we are skipping the event.
 * 
 *  <h2>Notes on Optimization</h2>
 *  Queries that meet any of the following criteria will perform a full scan of the events in the partitioned table:
 * 
 *  1. An 'or' conjunction exists in the query but not all of the terms are indexed.
 *  2. No indexed terms exist in the query
 *  3. An unsupported operator exists in the query
 * 
 * </pre>
 * 
 */
public class QueryLogic extends AbstractQueryLogic {
  
  protected static Logger log = Logger.getLogger(QueryLogic.class);
  
  public QueryLogic() {
    super();
  }
  
  @Override
  protected RangeCalculator getTermIndexInformation(Connector c, Authorizations auths, Multimap<String,Normalizer> indexedTerms,
      Multimap<String,QueryTerm> terms, String indexTableName, String reverseIndexTableName, String queryString, int queryThreads, Set<String> typeFilter)
      throws TableNotFoundException, org.apache.commons.jexl2.parser.ParseException {
    RangeCalculator calc = new RangeCalculator();
    calc.execute(c, auths, indexedTerms, terms, queryString, this, typeFilter);
    return calc;
  }
  
  protected Collection<Range> getFullScanRange(Date begin, Date end, Multimap<String,QueryTerm> terms) {
    return Collections.singletonList(new Range());
  }
  
  @Override
  protected IndexRanges getTermIndexInformation(Connector c, Authorizations auths, String value, Set<String> typeFilter) throws TableNotFoundException {
    final String dummyTermName = "DUMMY";
    UnionIndexRanges indexRanges = new UnionIndexRanges();
    
    // The entries in the index are normalized, since we don't have a field, just try using the LcNoDiacriticsNormalizer.
    String normalizedFieldValue = new LcNoDiacriticsNormalizer().normalizeFieldValue("", value);
    // Remove the begin and end ' marks
    if (normalizedFieldValue.startsWith("'") && normalizedFieldValue.endsWith("'")) {
      normalizedFieldValue = normalizedFieldValue.substring(1, normalizedFieldValue.length() - 1);
    }
    Text fieldValue = new Text(normalizedFieldValue);
    if (log.isDebugEnabled()) {
      log.debug("Querying index table : " + this.getIndexTableName() + " for normalized indexed term: " + fieldValue);
    }
    Scanner scanner = c.createScanner(this.getIndexTableName(), auths);
    Range r = new Range(fieldValue);
    scanner.setRange(r);
    if (log.isDebugEnabled()) {
      log.debug("Range for index query: " + r.toString());
    }
    for (Entry<Key,Value> entry : scanner) {
      if (log.isDebugEnabled()) {
        log.debug("Index entry: " + entry.getKey().toString());
      }
      // Get the shard id and datatype from the colq
      String fieldName = entry.getKey().getColumnFamily().toString();
      String colq = entry.getKey().getColumnQualifier().toString();
      int separator = colq.indexOf(EvaluatingIterator.NULL_BYTE_STRING);
      String shardId = null;
      String datatype = null;
      if (separator != -1) {
        shardId = colq.substring(0, separator);
        datatype = colq.substring(separator + 1);
      } else {
        shardId = colq;
      }
      // Skip this entry if the type is not correct
      if (null != datatype && null != typeFilter && !typeFilter.contains(datatype))
        continue;
      // Parse the UID.List object from the value
      Uid.List uidList = null;
      try {
        uidList = Uid.List.parseFrom(entry.getValue().get());
      } catch (InvalidProtocolBufferException e) {
        // Don't add UID information, at least we know what shards
        // it is located in.
      }
      
      // Add the count for this shard to the total count for the term.
      long count = 0;
      Long storedCount = indexRanges.getTermCardinality().get(dummyTermName);
      if (null == storedCount) {
        count = uidList.getCOUNT();
      } else {
        count = uidList.getCOUNT() + storedCount;
      }
      indexRanges.getTermCardinality().put(dummyTermName, count);
      // Add the field name
      indexRanges.getFieldNamesAndValues().put(fieldName, normalizedFieldValue);
      
      // Create the keys
      Text shard = new Text(shardId);
      if (uidList.getIGNORE()) {
        // Then we create a scan range that is the entire shard
        indexRanges.add(dummyTermName, new Range(shard));
      } else {
        // We should have UUIDs, create event ranges
        for (String uuid : uidList.getUIDList()) {
          Text cf = new Text(datatype);
          TextUtil.textAppend(cf, uuid);
          Key startKey = new Key(shard, cf);
          Key endKey = new Key(shard, new Text(cf.toString() + EvaluatingIterator.NULL_BYTE_STRING));
          Range eventRange = new Range(startKey, true, endKey, false);
          indexRanges.add(dummyTermName, eventRange);
        }
      }
    }
    if (log.isDebugEnabled()) {
      log.debug("Found " + indexRanges.getRanges().size() + " entries in the index for field value: " + normalizedFieldValue);
    }
    return indexRanges;
    
  }
  
}
