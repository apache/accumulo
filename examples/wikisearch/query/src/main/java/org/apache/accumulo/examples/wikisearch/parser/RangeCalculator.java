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
package org.apache.accumulo.examples.wikisearch.parser;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.examples.wikisearch.iterator.EvaluatingIterator;
import org.apache.accumulo.examples.wikisearch.logic.AbstractQueryLogic;
import org.apache.accumulo.examples.wikisearch.normalizer.Normalizer;
import org.apache.accumulo.examples.wikisearch.protobuf.Uid;
import org.apache.accumulo.examples.wikisearch.util.TextUtil;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * This class is used to query the global indices to determine that set of ranges to use when querying the shard table. The RangeCalculator looks at each term
 * in the query to determine if it is a equivalence, range, or wildcard comparison, and queries the appropriate index to find the ranges for the terms which are
 * then cached. The final set of ranges is computed as the AST is traversed.
 */
public class RangeCalculator extends QueryParser {
  
  /**
   * Container used as map keys in this class
   * 
   */
  public static class MapKey implements Comparable<MapKey> {
    private String fieldName = null;
    private String fieldValue = null;
    private String originalQueryValue = null;
    
    public MapKey(String fieldName, String fieldValue) {
      super();
      this.fieldName = fieldName;
      this.fieldValue = fieldValue;
    }
    
    public String getFieldName() {
      return fieldName;
    }
    
    public String getFieldValue() {
      return fieldValue;
    }
    
    public void setFieldName(String fieldName) {
      this.fieldName = fieldName;
    }
    
    public void setFieldValue(String fieldValue) {
      this.fieldValue = fieldValue;
    }
    
    public String getOriginalQueryValue() {
      return originalQueryValue;
    }
    
    public void setOriginalQueryValue(String originalQueryValue) {
      this.originalQueryValue = originalQueryValue;
    }
    
    @Override
    public int hashCode() {
      return new HashCodeBuilder(17, 37).append(fieldName).append(fieldValue).toHashCode();
    }
    
    @Override
    public String toString() {
      return this.fieldName + " " + this.fieldValue;
    }
    
    @Override
    public boolean equals(Object other) {
      if (other == null)
        return false;
      if (other instanceof MapKey) {
        MapKey o = (MapKey) other;
        return (this.fieldName.equals(o.fieldName) && this.fieldValue.equals(o.fieldValue));
      } else
        return false;
    }
    
    public int compareTo(MapKey o) {
      int result = this.fieldName.compareTo(o.fieldName);
      if (result != 0) {
        return this.fieldValue.compareTo(o.fieldValue);
      } else {
        return result;
      }
    }
    
  }
  
  /**
   * Container used to hold the lower and upper bound of a range
   * 
   */
  public static class RangeBounds {
    private String originalLower = null;
    private Text lower = null;
    private String originalUpper = null;
    private Text upper = null;
    
    public Text getLower() {
      return lower;
    }
    
    public Text getUpper() {
      return upper;
    }
    
    public void setLower(Text lower) {
      this.lower = lower;
    }
    
    public void setUpper(Text upper) {
      this.upper = upper;
    }
    
    public String getOriginalLower() {
      return originalLower;
    }
    
    public String getOriginalUpper() {
      return originalUpper;
    }
    
    public void setOriginalLower(String originalLower) {
      this.originalLower = originalLower;
    }
    
    public void setOriginalUpper(String originalUpper) {
      this.originalUpper = originalUpper;
    }
  }
  
  /**
   * 
   * Object that is used to hold ranges found in the index. Subclasses may compute the final range set in various ways.
   */
  protected static class TermRange implements Comparable<TermRange> {
    
    private String fieldName = null;
    private Object fieldValue = null;
    private Set<Range> ranges = new TreeSet<Range>();
    
    public TermRange(String name, Object fieldValue) {
      this.fieldName = name;
      this.fieldValue = fieldValue;
    }
    
    public String getFieldName() {
      return this.fieldName;
    }
    
    public Object getFieldValue() {
      return this.fieldValue;
    }
    
    public void addAll(Set<Range> r) {
      ranges.addAll(r);
    }
    
    public void add(Range r) {
      ranges.add(r);
    }
    
    public Set<Range> getRanges() {
      return ranges;
    }
    
    @Override
    public String toString() {
      ToStringBuilder tsb = new ToStringBuilder(this);
      tsb.append("fieldName", fieldName);
      tsb.append("fieldValue", fieldValue);
      tsb.append("ranges", ranges);
      return tsb.toString();
    }
    
    public int compareTo(TermRange o) {
      int result = this.fieldName.compareTo(o.fieldName);
      if (result == 0) {
        return ((Integer) ranges.size()).compareTo(o.ranges.size());
      } else {
        return result;
      }
    }
  }
  
  /**
   * Object used to store context information as the AST is being traversed.
   */
  static class EvaluationContext {
    boolean inOrContext = false;
    boolean inNotContext = false;
    boolean inAndContext = false;
    TermRange lastRange = null;
    String lastProcessedTerm = null;
  }
  
  protected static Logger log = Logger.getLogger(RangeCalculator.class);
  private static String WILDCARD = ".*";
  private static String SINGLE_WILDCARD = "\\.";
  
  protected Connector c;
  protected Authorizations auths;
  protected Multimap<String,Normalizer> indexedTerms;
  protected Multimap<String,QueryTerm> termsCopy = HashMultimap.create();
  protected String indexTableName;
  protected String reverseIndexTableName;
  protected int queryThreads = 8;
  
  /* final results of index lookups, ranges for the shard table */
  protected Set<Range> result = null;
  /* map of field names to values found in the index */
  protected Multimap<String,String> indexEntries = HashMultimap.create();
  /* map of value in the index to the original query values */
  protected Map<String,String> indexValues = new HashMap<String,String>();
  /* map of values in the query to map keys used */
  protected Multimap<String,MapKey> originalQueryValues = HashMultimap.create();
  /* map of field name to cardinality */
  protected Map<String,Long> termCardinalities = new HashMap<String,Long>();
  /* cached results of all ranges found global index lookups */
  protected Map<MapKey,TermRange> globalIndexResults = new HashMap<MapKey,TermRange>();
  
  /**
   * 
   * @param c
   * @param auths
   * @param indexedTerms
   * @param terms
   * @param query
   * @param logic
   * @param typeFilter
   * @throws ParseException
   */
  public void execute(Connector c, Authorizations auths, Multimap<String,Normalizer> indexedTerms, Multimap<String,QueryTerm> terms, String query,
      AbstractQueryLogic logic, Set<String> typeFilter) throws ParseException {
    super.execute(query);
    this.c = c;
    this.auths = auths;
    this.indexedTerms = indexedTerms;
    this.termsCopy.putAll(terms);
    this.indexTableName = logic.getIndexTableName();
    this.reverseIndexTableName = logic.getReverseIndexTableName();
    this.queryThreads = logic.getQueryThreads();
    
    Map<MapKey,Set<Range>> indexRanges = new HashMap<MapKey,Set<Range>>();
    Map<MapKey,Set<Range>> trailingWildcardRanges = new HashMap<MapKey,Set<Range>>();
    Map<MapKey,Set<Range>> leadingWildcardRanges = new HashMap<MapKey,Set<Range>>();
    Map<Text,RangeBounds> rangeMap = new HashMap<Text,RangeBounds>();
    
    // Here we iterate over all of the terms in the query to determine if they are an equivalence,
    // wildcard, or range type operator
    for (Entry<String,QueryTerm> entry : terms.entries()) {
      if (entry.getValue().getOperator().equals(JexlOperatorConstants.getOperator(ASTEQNode.class))
          || entry.getValue().getOperator().equals(JexlOperatorConstants.getOperator(ASTERNode.class))
          || entry.getValue().getOperator().equals(JexlOperatorConstants.getOperator(ASTLTNode.class))
          || entry.getValue().getOperator().equals(JexlOperatorConstants.getOperator(ASTLENode.class))
          || entry.getValue().getOperator().equals(JexlOperatorConstants.getOperator(ASTGTNode.class))
          || entry.getValue().getOperator().equals(JexlOperatorConstants.getOperator(ASTGENode.class))) {
        // If this term is not in the set of indexed terms, then bail
        if (!indexedTerms.containsKey(entry.getKey())) {
          termCardinalities.put(entry.getKey().toUpperCase(), 0L);
          continue;
        }
        // In the case of function calls, the query term could be null. Dont query the index for it.
        if (null == entry.getValue()) {
          termCardinalities.put(entry.getKey().toUpperCase(), 0L);
          continue;
        }
        // In the case where we are looking for 'null', then skip.
        if (null == entry.getValue().getValue() || ((String) entry.getValue().getValue()).equals("null")) {
          termCardinalities.put(entry.getKey().toUpperCase(), 0L);
          continue;
        }
        
        // Remove the begin and end ' marks
        String value = null;
        if (((String) entry.getValue().getValue()).startsWith("'") && ((String) entry.getValue().getValue()).endsWith("'"))
          value = ((String) entry.getValue().getValue()).substring(1, ((String) entry.getValue().getValue()).length() - 1);
        else
          value = (String) entry.getValue().getValue();
        // The entries in the index are normalized
        for (Normalizer normalizer : indexedTerms.get(entry.getKey())) {
          String normalizedFieldValue = normalizer.normalizeFieldValue(null, value);
          Text fieldValue = new Text(normalizedFieldValue);
          Text fieldName = new Text(entry.getKey().toUpperCase());
          
          // EQUALS
          if (entry.getValue().getOperator().equals(JexlOperatorConstants.getOperator(ASTEQNode.class))) {
            Key startRange = new Key(fieldValue, fieldName);
            Range r = new Range(startRange, true, startRange.followingKey(PartialKey.ROW), true);
            
            MapKey key = new MapKey(fieldName.toString(), fieldValue.toString());
            key.setOriginalQueryValue(value);
            this.originalQueryValues.put(value, key);
            if (!indexRanges.containsKey(key))
              indexRanges.put(key, new HashSet<Range>());
            indexRanges.get(key).add(r);
            // WILDCARD
          } else if (entry.getValue().getOperator().equals(JexlOperatorConstants.getOperator(ASTERNode.class))) {
            // This is a wildcard query using regex. We can only support leading and trailing wildcards at this time. Leading
            // wildcards will need be reversed and sent to the global reverse index. Trailing wildcard queries will be sent to the
            // global index. In all cases, the range for the wilcard will be the range of possible UNICODE codepoints, hex 0 to 10FFFF.
            int loc = normalizedFieldValue.indexOf(WILDCARD);
            if (-1 == loc)
              loc = normalizedFieldValue.indexOf(SINGLE_WILDCARD);
            if (-1 == loc) {
              // Then no wildcard in the query? Treat like the equals case above.
              Key startRange = new Key(fieldValue, fieldName);
              Range r = new Range(startRange, true, startRange.followingKey(PartialKey.ROW), true);
              
              MapKey key = new MapKey(fieldName.toString(), fieldValue.toString());
              key.setOriginalQueryValue(value);
              this.originalQueryValues.put(value, key);
              if (!indexRanges.containsKey(key))
                indexRanges.put(key, new HashSet<Range>());
              indexRanges.get(key).add(r);
            } else {
              if (loc == 0) {
                // Then we have a leading wildcard, reverse the term and use the global reverse index.
                StringBuilder buf = new StringBuilder(normalizedFieldValue.substring(2));
                normalizedFieldValue = buf.reverse().toString();
                Key startRange = new Key(new Text(normalizedFieldValue + "\u0000"), fieldName);
                Key endRange = new Key(new Text(normalizedFieldValue + "\u10FFFF"), fieldName);
                Range r = new Range(startRange, true, endRange, true);
                
                MapKey key = new MapKey(fieldName.toString(), normalizedFieldValue);
                key.setOriginalQueryValue(value);
                this.originalQueryValues.put(value, key);
                if (!leadingWildcardRanges.containsKey(key))
                  leadingWildcardRanges.put(key, new HashSet<Range>());
                leadingWildcardRanges.get(key).add(r);
              } else if (loc == (normalizedFieldValue.length() - 2)) {
                normalizedFieldValue = normalizedFieldValue.substring(0, loc);
                // Then we have a trailing wildcard character.
                Key startRange = new Key(new Text(normalizedFieldValue + "\u0000"), fieldName);
                Key endRange = new Key(new Text(normalizedFieldValue + "\u10FFFF"), fieldName);
                Range r = new Range(startRange, true, endRange, true);
                
                MapKey key = new MapKey(fieldName.toString(), normalizedFieldValue);
                key.setOriginalQueryValue(value);
                this.originalQueryValues.put(value, key);
                if (!trailingWildcardRanges.containsKey(key))
                  trailingWildcardRanges.put(key, new HashSet<Range>());
                trailingWildcardRanges.get(key).add(r);
              } else {
                // throw new RuntimeException("Unsupported wildcard location. Only trailing or leading wildcards are supported: " + normalizedFieldValue);
                // Don't throw an exception, there must be a wildcard in the query, we'll treat it as a filter on the results since it is not
                // leading or trailing.
              }
            }
            // RANGES
          } else if (entry.getValue().getOperator().equals(JexlOperatorConstants.getOperator(ASTGTNode.class))
              || entry.getValue().getOperator().equals(JexlOperatorConstants.getOperator(ASTGENode.class))) {
            // Then we have a lower bound to a range query
            if (!rangeMap.containsKey(fieldName))
              rangeMap.put(fieldName, new RangeBounds());
            rangeMap.get(fieldName).setLower(fieldValue);
            rangeMap.get(fieldName).setOriginalLower(value);
          } else if (entry.getValue().getOperator().equals(JexlOperatorConstants.getOperator(ASTLTNode.class))
              || entry.getValue().getOperator().equals(JexlOperatorConstants.getOperator(ASTLENode.class))) {
            // Then we have an upper bound to a range query
            if (!rangeMap.containsKey(fieldName))
              rangeMap.put(fieldName, new RangeBounds());
            rangeMap.get(fieldName).setUpper(fieldValue);
            rangeMap.get(fieldName).setOriginalUpper(value);
          }
        }
      }
    }
    
    // INDEX RANGE QUERY
    // Now that we have figured out the range bounds, create the index ranges.
    for (Entry<Text,RangeBounds> entry : rangeMap.entrySet()) {
      if (entry.getValue().getLower() != null && entry.getValue().getUpper() != null) {
        // Figure out the key order
        Key lk = new Key(entry.getValue().getLower());
        Key up = new Key(entry.getValue().getUpper());
        Text lower = lk.getRow();
        Text upper = up.getRow();
        // Swith the order if needed.
        if (lk.compareTo(up) > 0) {
          lower = up.getRow();
          upper = lk.getRow();
        }
        Key startRange = new Key(lower, entry.getKey());
        Key endRange = new Key(upper, entry.getKey());
        Range r = new Range(startRange, true, endRange, true);
        // For the range queries we need to query the global index and then handle the results a little differently.
        Map<MapKey,Set<Range>> ranges = new HashMap<MapKey,Set<Range>>();
        MapKey key = new MapKey(entry.getKey().toString(), entry.getValue().getLower().toString());
        key.setOriginalQueryValue(entry.getValue().getOriginalLower().toString());
        this.originalQueryValues.put(entry.getValue().getOriginalLower().toString(), key);
        ranges.put(key, new HashSet<Range>());
        ranges.get(key).add(r);
        
        // Now query the global index and override the field value used in the results map
        try {
          Map<MapKey,TermRange> lowerResults = queryGlobalIndex(ranges, entry.getKey().toString(), this.indexTableName, false, key, typeFilter);
          // Add the results to the global index results for both the upper and lower field values.
          Map<MapKey,TermRange> upperResults = new HashMap<MapKey,TermRange>();
          for (Entry<MapKey,TermRange> e : lowerResults.entrySet()) {
            MapKey key2 = new MapKey(e.getKey().getFieldName(), entry.getValue().getUpper().toString());
            key2.setOriginalQueryValue(entry.getValue().getOriginalUpper().toString());
            upperResults.put(key2, e.getValue());
            this.originalQueryValues.put(entry.getValue().getOriginalUpper(), key2);
            
          }
          
          this.globalIndexResults.putAll(lowerResults);
          this.globalIndexResults.putAll(upperResults);
          
        } catch (TableNotFoundException e) {
          log.error("index table not found", e);
          throw new RuntimeException(" index table not found", e);
        }
      } else {
        log.warn("Unbounded range detected, not querying index for it. Field  " + entry.getKey().toString() + " in query: " + query);
      }
    }
    // Now that we have calculated all of the ranges, query the global index.
    try {
      
      // Query for the trailing wildcards if we have any
      for (Entry<MapKey,Set<Range>> trailing : trailingWildcardRanges.entrySet()) {
        Map<MapKey,Set<Range>> m = new HashMap<MapKey,Set<Range>>();
        m.put(trailing.getKey(), trailing.getValue());
        if (log.isDebugEnabled())
          log.debug("Ranges for Wildcard Global Index query: " + m.toString());
        this.globalIndexResults.putAll(queryGlobalIndex(m, trailing.getKey().getFieldName(), this.indexTableName, false, trailing.getKey(), typeFilter));
      }
      
      // Query for the leading wildcards if we have any
      for (Entry<MapKey,Set<Range>> leading : leadingWildcardRanges.entrySet()) {
        Map<MapKey,Set<Range>> m = new HashMap<MapKey,Set<Range>>();
        m.put(leading.getKey(), leading.getValue());
        if (log.isDebugEnabled())
          log.debug("Ranges for Wildcard Global Reverse Index query: " + m.toString());
        this.globalIndexResults.putAll(queryGlobalIndex(m, leading.getKey().getFieldName(), this.reverseIndexTableName, true, leading.getKey(), typeFilter));
      }
      
      // Query for the equals case
      for (Entry<MapKey,Set<Range>> equals : indexRanges.entrySet()) {
        Map<MapKey,Set<Range>> m = new HashMap<MapKey,Set<Range>>();
        m.put(equals.getKey(), equals.getValue());
        if (log.isDebugEnabled())
          log.debug("Ranges for Global Index query: " + m.toString());
        this.globalIndexResults.putAll(queryGlobalIndex(m, equals.getKey().getFieldName(), this.indexTableName, false, equals.getKey(), typeFilter));
      }
    } catch (TableNotFoundException e) {
      log.error("index table not found", e);
      throw new RuntimeException(" index table not found", e);
    }
    
    if (log.isDebugEnabled())
      log.debug("Ranges from Global Index query: " + globalIndexResults.toString());
    
    // Now traverse the AST
    EvaluationContext ctx = new EvaluationContext();
    this.getAST().childrenAccept(this, ctx);
    
    if (ctx.lastRange.getRanges().size() == 0) {
      log.debug("No resulting range set");
    } else {
      if (log.isDebugEnabled())
        log.debug("Setting range results to: " + ctx.lastRange.getRanges().toString());
      this.result = ctx.lastRange.getRanges();
    }
  }
  
  /**
   * 
   * @return set of ranges to use for the shard table
   */
  public Set<Range> getResult() {
    return result;
  }
  
  /**
   * 
   * @return map of field names to index field values
   */
  public Multimap<String,String> getIndexEntries() {
    return indexEntries;
  }
  
  public Map<String,String> getIndexValues() {
    return indexValues;
  }
  
  /**
   * 
   * @return Cardinality for each field name.
   */
  public Map<String,Long> getTermCardinalities() {
    return termCardinalities;
  }
  
  /**
   * 
   * @param indexRanges
   * @param tableName
   * @param isReverse
   *          switch that determines whether or not to reverse the results
   * @param override
   *          mapKey for wildcard and range queries that specify which mapkey to use in the results
   * @param typeFilter
   *          - optional list of datatypes
   * @throws TableNotFoundException
   */
  protected Map<MapKey,TermRange> queryGlobalIndex(Map<MapKey,Set<Range>> indexRanges, String specificFieldName, String tableName, boolean isReverse,
      MapKey override, Set<String> typeFilter) throws TableNotFoundException {
    
    // The results map where the key is the field name and field value and the
    // value is a set of ranges. The mapkey will always be the field name
    // and field value that was passed in the original query. The TermRange
    // will contain the field name and field value found in the index.
    Map<MapKey,TermRange> results = new HashMap<MapKey,TermRange>();
    
    // Seed the results map and create the range set for the batch scanner
    Set<Range> rangeSuperSet = new HashSet<Range>();
    for (Entry<MapKey,Set<Range>> entry : indexRanges.entrySet()) {
      rangeSuperSet.addAll(entry.getValue());
      TermRange tr = new TermRange(entry.getKey().getFieldName(), entry.getKey().getFieldValue());
      if (null == override)
        results.put(entry.getKey(), tr);
      else
        results.put(override, tr);
    }
    
    if (log.isDebugEnabled())
      log.debug("Querying global index table: " + tableName + ", range: " + rangeSuperSet.toString() + " colf: " + specificFieldName);
    BatchScanner bs = this.c.createBatchScanner(tableName, this.auths, this.queryThreads);
    bs.setRanges(rangeSuperSet);
    if (null != specificFieldName) {
      bs.fetchColumnFamily(new Text(specificFieldName));
    }
    
    for (Entry<Key,Value> entry : bs) {
      if (log.isDebugEnabled()) {
        log.debug("Index entry: " + entry.getKey().toString());
      }
      String fieldValue = null;
      if (!isReverse) {
        fieldValue = entry.getKey().getRow().toString();
      } else {
        StringBuilder buf = new StringBuilder(entry.getKey().getRow().toString());
        fieldValue = buf.reverse().toString();
      }
      
      String fieldName = entry.getKey().getColumnFamily().toString();
      // Get the shard id and datatype from the colq
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
      Long storedCount = termCardinalities.get(fieldName);
      if (null == storedCount || 0 == storedCount) {
        count = uidList.getCOUNT();
      } else {
        count = uidList.getCOUNT() + storedCount;
      }
      termCardinalities.put(fieldName, count);
      this.indexEntries.put(fieldName, fieldValue);
      
      if (null == override)
        this.indexValues.put(fieldValue, fieldValue);
      else
        this.indexValues.put(fieldValue, override.getOriginalQueryValue());
      
      // Create the keys
      Text shard = new Text(shardId);
      if (uidList.getIGNORE()) {
        // Then we create a scan range that is the entire shard
        if (null == override)
          results.get(new MapKey(fieldName, fieldValue)).add(new Range(shard));
        else
          results.get(override).add(new Range(shard));
      } else {
        // We should have UUIDs, create event ranges
        for (String uuid : uidList.getUIDList()) {
          Text cf = new Text(datatype);
          TextUtil.textAppend(cf, uuid);
          Key startKey = new Key(shard, cf);
          Key endKey = new Key(shard, new Text(cf.toString() + EvaluatingIterator.NULL_BYTE_STRING));
          Range eventRange = new Range(startKey, true, endKey, false);
          if (null == override)
            results.get(new MapKey(fieldName, fieldValue)).add(eventRange);
          else
            results.get(override).add(eventRange);
        }
      }
    }
    bs.close();
    return results;
  }
  
  @Override
  public Object visit(ASTOrNode node, Object data) {
    boolean previouslyInOrContext = false;
    EvaluationContext ctx = null;
    if (null != data && data instanceof EvaluationContext) {
      ctx = (EvaluationContext) data;
      previouslyInOrContext = ctx.inOrContext;
    } else {
      ctx = new EvaluationContext();
    }
    ctx.inOrContext = true;
    // Process both sides of this node. Left branch first
    node.jjtGetChild(0).jjtAccept(this, ctx);
    Long leftCardinality = this.termCardinalities.get(ctx.lastProcessedTerm);
    if (null == leftCardinality)
      leftCardinality = 0L;
    TermRange leftRange = ctx.lastRange;
    if (log.isDebugEnabled())
      log.debug("[OR-left] term: " + ctx.lastProcessedTerm + ", cardinality: " + leftCardinality + ", ranges: " + leftRange.getRanges().size());
    
    // Process the right branch
    node.jjtGetChild(1).jjtAccept(this, ctx);
    Long rightCardinality = this.termCardinalities.get(ctx.lastProcessedTerm);
    if (null == rightCardinality)
      rightCardinality = 0L;
    TermRange rightRange = ctx.lastRange;
    if (log.isDebugEnabled())
      log.debug("[OR-right] term: " + ctx.lastProcessedTerm + ", cardinality: " + rightCardinality + ", ranges: " + rightRange.getRanges().size());
    
    // reset the state
    if (null != data && !previouslyInOrContext)
      ctx.inOrContext = false;
    // Add the ranges for the left and right branches to a TreeSet to sort them
    Set<Range> ranges = new TreeSet<Range>();
    ranges.addAll(leftRange.getRanges());
    ranges.addAll(rightRange.getRanges());
    // Now create the union set
    Set<Text> shardsAdded = new HashSet<Text>();
    Set<Range> returnSet = new HashSet<Range>();
    for (Range r : ranges) {
      if (!shardsAdded.contains(r.getStartKey().getRow())) {
        // Only add ranges with a start key for the entire shard.
        if (r.getStartKey().getColumnFamily() == null) {
          shardsAdded.add(r.getStartKey().getRow());
        }
        returnSet.add(r);
      } else {
        // if (log.isTraceEnabled())
        log.info("Skipping event specific range: " + r.toString() + " because shard range has already been added: "
            + shardsAdded.contains(r.getStartKey().getRow()));
      }
    }
    // Clear the ranges from the context and add the result in its place
    TermRange orRange = new TermRange("OR_RESULT", "foo");
    orRange.addAll(returnSet);
    if (log.isDebugEnabled())
      log.debug("[OR] results: " + orRange.getRanges().toString());
    ctx.lastRange = orRange;
    ctx.lastProcessedTerm = "OR_RESULT";
    this.termCardinalities.put("OR_RESULT", (leftCardinality + rightCardinality));
    return null;
  }
  
  @Override
  public Object visit(ASTAndNode node, Object data) {
    boolean previouslyInAndContext = false;
    EvaluationContext ctx = null;
    if (null != data && data instanceof EvaluationContext) {
      ctx = (EvaluationContext) data;
      previouslyInAndContext = ctx.inAndContext;
    } else {
      ctx = new EvaluationContext();
    }
    ctx.inAndContext = true;
    // Process both sides of this node.
    node.jjtGetChild(0).jjtAccept(this, ctx);
    String leftTerm = ctx.lastProcessedTerm;
    Long leftCardinality = this.termCardinalities.get(leftTerm);
    if (null == leftCardinality)
      leftCardinality = 0L;
    TermRange leftRange = ctx.lastRange;
    if (log.isDebugEnabled())
      log.debug("[AND-left] term: " + ctx.lastProcessedTerm + ", cardinality: " + leftCardinality + ", ranges: " + leftRange.getRanges().size());
    
    // Process the right branch
    node.jjtGetChild(1).jjtAccept(this, ctx);
    String rightTerm = ctx.lastProcessedTerm;
    Long rightCardinality = this.termCardinalities.get(rightTerm);
    if (null == rightCardinality)
      rightCardinality = 0L;
    TermRange rightRange = ctx.lastRange;
    if (log.isDebugEnabled())
      log.debug("[AND-right] term: " + ctx.lastProcessedTerm + ", cardinality: " + rightCardinality + ", ranges: " + rightRange.getRanges().size());
    
    // reset the state
    if (null != data && !previouslyInAndContext)
      ctx.inAndContext = false;
    
    long card = 0L;
    TermRange andRange = new TermRange("AND_RESULT", "foo");
    if ((leftCardinality > 0 && leftCardinality <= rightCardinality) || rightCardinality == 0) {
      card = leftCardinality;
      andRange.addAll(leftRange.getRanges());
    } else if ((rightCardinality > 0 && rightCardinality <= leftCardinality) || leftCardinality == 0) {
      card = rightCardinality;
      andRange.addAll(rightRange.getRanges());
    }
    if (log.isDebugEnabled())
      log.debug("[AND] results: " + andRange.getRanges().toString());
    ctx.lastRange = andRange;
    ctx.lastProcessedTerm = "AND_RESULT";
    this.termCardinalities.put("AND_RESULT", card);
    
    return null;
  }
  
  @Override
  public Object visit(ASTEQNode node, Object data) {
    StringBuilder fieldName = new StringBuilder();
    ObjectHolder value = new ObjectHolder();
    // Process both sides of this node.
    Object left = node.jjtGetChild(0).jjtAccept(this, data);
    Object right = node.jjtGetChild(1).jjtAccept(this, data);
    // Ignore functions in the query
    if (left instanceof FunctionResult || right instanceof FunctionResult)
      return null;
    decodeResults(left, right, fieldName, value);
    // We need to check to see if we are in a NOT context. If so,
    // then we need to reverse the negation.
    boolean negated = false;
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      if (ctx.inNotContext)
        negated = !negated;
    }
    QueryTerm term = new QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
    termsCopy.put(fieldName.toString(), term);
    // Get the terms from the global index
    // Remove the begin and end ' marks
    String termValue = null;
    if (((String) term.getValue()).startsWith("'") && ((String) term.getValue()).endsWith("'"))
      termValue = ((String) term.getValue()).substring(1, ((String) term.getValue()).length() - 1);
    else
      termValue = (String) term.getValue();
    // Get the values found in the index for this query term
    TermRange ranges = null;
    for (MapKey key : this.originalQueryValues.get(termValue)) {
      if (key.getFieldName().equalsIgnoreCase(fieldName.toString())) {
        ranges = this.globalIndexResults.get(key);
        if (log.isDebugEnabled())
          log.debug("Results for cached index ranges for key: " + key + " are " + ranges);
      }
    }
    // If no result for this field name and value, then add empty range
    if (null == ranges)
      ranges = new TermRange(fieldName.toString(), (String) term.getValue());
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      ctx.lastRange = ranges;
      ctx.lastProcessedTerm = fieldName.toString();
    }
    return null;
  }
  
  @Override
  public Object visit(ASTNENode node, Object data) {
    StringBuilder fieldName = new StringBuilder();
    ObjectHolder value = new ObjectHolder();
    // Process both sides of this node.
    Object left = node.jjtGetChild(0).jjtAccept(this, data);
    Object right = node.jjtGetChild(1).jjtAccept(this, data);
    // Ignore functions in the query
    if (left instanceof FunctionResult || right instanceof FunctionResult)
      return null;
    decodeResults(left, right, fieldName, value);
    // We need to check to see if we are in a NOT context. If so,
    // then we need to reverse the negation.
    boolean negated = true;
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      if (ctx.inNotContext)
        negated = !negated;
    }
    if (negated)
      negatedTerms.add(fieldName.toString());
    QueryTerm term = new QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
    termsCopy.put(fieldName.toString(), term);
    // We can only use the global index for equality, put in fake results
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      ctx.lastRange = new TermRange(fieldName.toString(), term.getValue());
      ctx.lastProcessedTerm = fieldName.toString();
      termCardinalities.put(fieldName.toString(), 0L);
    }
    return null;
  }
  
  @Override
  public Object visit(ASTLTNode node, Object data) {
    StringBuilder fieldName = new StringBuilder();
    ObjectHolder value = new ObjectHolder();
    // Process both sides of this node.
    Object left = node.jjtGetChild(0).jjtAccept(this, data);
    Object right = node.jjtGetChild(1).jjtAccept(this, data);
    // Ignore functions in the query
    if (left instanceof FunctionResult || right instanceof FunctionResult)
      return null;
    decodeResults(left, right, fieldName, value);
    // We need to check to see if we are in a NOT context. If so,
    // then we need to reverse the negation.
    boolean negated = false;
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      if (ctx.inNotContext)
        negated = !negated;
    }
    QueryTerm term = new QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
    termsCopy.put(fieldName.toString(), term);
    // Get the terms from the global index
    // Remove the begin and end ' marks
    String termValue = null;
    if (((String) term.getValue()).startsWith("'") && ((String) term.getValue()).endsWith("'"))
      termValue = ((String) term.getValue()).substring(1, ((String) term.getValue()).length() - 1);
    else
      termValue = (String) term.getValue();
    // Get the values found in the index for this query term
    TermRange ranges = null;
    for (MapKey key : this.originalQueryValues.get(termValue)) {
      if (key.getFieldName().equalsIgnoreCase(fieldName.toString())) {
        ranges = this.globalIndexResults.get(key);
        if (log.isDebugEnabled())
          log.debug("Results for cached index ranges for key: " + key + " are " + ranges);
      }
    }
    // If no result for this field name and value, then add empty range
    if (null == ranges)
      ranges = new TermRange(fieldName.toString(), (String) term.getValue());
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      ctx.lastRange = ranges;
      ctx.lastProcessedTerm = fieldName.toString();
    }
    return null;
  }
  
  @Override
  public Object visit(ASTGTNode node, Object data) {
    StringBuilder fieldName = new StringBuilder();
    ObjectHolder value = new ObjectHolder();
    // Process both sides of this node.
    Object left = node.jjtGetChild(0).jjtAccept(this, data);
    Object right = node.jjtGetChild(1).jjtAccept(this, data);
    // Ignore functions in the query
    if (left instanceof FunctionResult || right instanceof FunctionResult)
      return null;
    decodeResults(left, right, fieldName, value);
    // We need to check to see if we are in a NOT context. If so,
    // then we need to reverse the negation.
    boolean negated = false;
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      if (ctx.inNotContext)
        negated = !negated;
    }
    QueryTerm term = new QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
    termsCopy.put(fieldName.toString(), term);
    // Get the terms from the global index
    // Remove the begin and end ' marks
    String termValue = null;
    if (((String) term.getValue()).startsWith("'") && ((String) term.getValue()).endsWith("'"))
      termValue = ((String) term.getValue()).substring(1, ((String) term.getValue()).length() - 1);
    else
      termValue = (String) term.getValue();
    // Get the values found in the index for this query term
    TermRange ranges = null;
    for (MapKey key : this.originalQueryValues.get(termValue)) {
      if (key.getFieldName().equalsIgnoreCase(fieldName.toString())) {
        ranges = this.globalIndexResults.get(key);
        if (log.isDebugEnabled())
          log.debug("Results for cached index ranges for key: " + key + " are " + ranges);
      }
    }
    // If no result for this field name and value, then add empty range
    if (null == ranges)
      ranges = new TermRange(fieldName.toString(), (String) term.getValue());
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      ctx.lastRange = ranges;
      ctx.lastProcessedTerm = fieldName.toString();
    }
    return null;
  }
  
  @Override
  public Object visit(ASTLENode node, Object data) {
    StringBuilder fieldName = new StringBuilder();
    ObjectHolder value = new ObjectHolder();
    // Process both sides of this node.
    Object left = node.jjtGetChild(0).jjtAccept(this, data);
    Object right = node.jjtGetChild(1).jjtAccept(this, data);
    // Ignore functions in the query
    if (left instanceof FunctionResult || right instanceof FunctionResult)
      return null;
    decodeResults(left, right, fieldName, value);
    // We need to check to see if we are in a NOT context. If so,
    // then we need to reverse the negation.
    boolean negated = false;
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      if (ctx.inNotContext)
        negated = !negated;
    }
    QueryTerm term = new QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
    termsCopy.put(fieldName.toString(), term);
    // Get the terms from the global index
    // Remove the begin and end ' marks
    String termValue = null;
    if (((String) term.getValue()).startsWith("'") && ((String) term.getValue()).endsWith("'"))
      termValue = ((String) term.getValue()).substring(1, ((String) term.getValue()).length() - 1);
    else
      termValue = (String) term.getValue();
    // Get the values found in the index for this query term
    TermRange ranges = null;
    for (MapKey key : this.originalQueryValues.get(termValue)) {
      if (key.getFieldName().equalsIgnoreCase(fieldName.toString())) {
        ranges = this.globalIndexResults.get(key);
        if (log.isDebugEnabled())
          log.debug("Results for cached index ranges for key: " + key + " are " + ranges);
      }
    }
    // If no result for this field name and value, then add empty range
    if (null == ranges)
      ranges = new TermRange(fieldName.toString(), (String) term.getValue());
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      ctx.lastRange = ranges;
      ctx.lastProcessedTerm = fieldName.toString();
    }
    return null;
  }
  
  @Override
  public Object visit(ASTGENode node, Object data) {
    StringBuilder fieldName = new StringBuilder();
    ObjectHolder value = new ObjectHolder();
    // Process both sides of this node.
    Object left = node.jjtGetChild(0).jjtAccept(this, data);
    Object right = node.jjtGetChild(1).jjtAccept(this, data);
    // Ignore functions in the query
    if (left instanceof FunctionResult || right instanceof FunctionResult)
      return null;
    decodeResults(left, right, fieldName, value);
    // We need to check to see if we are in a NOT context. If so,
    // then we need to reverse the negation.
    boolean negated = false;
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      if (ctx.inNotContext)
        negated = !negated;
    }
    QueryTerm term = new QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
    termsCopy.put(fieldName.toString(), term);
    // Get the terms from the global index
    // Remove the begin and end ' marks
    String termValue = null;
    if (((String) term.getValue()).startsWith("'") && ((String) term.getValue()).endsWith("'"))
      termValue = ((String) term.getValue()).substring(1, ((String) term.getValue()).length() - 1);
    else
      termValue = (String) term.getValue();
    // Get the values found in the index for this query term
    TermRange ranges = null;
    for (MapKey key : this.originalQueryValues.get(termValue)) {
      if (key.getFieldName().equalsIgnoreCase(fieldName.toString())) {
        ranges = this.globalIndexResults.get(key);
        if (log.isDebugEnabled())
          log.debug("Results for cached index ranges for key: " + key + " are " + ranges);
      }
    }
    // If no result for this field name and value, then add empty range
    if (null == ranges)
      ranges = new TermRange(fieldName.toString(), (String) term.getValue());
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      ctx.lastRange = ranges;
      ctx.lastProcessedTerm = fieldName.toString();
    }
    return null;
  }
  
  @Override
  public Object visit(ASTERNode node, Object data) {
    StringBuilder fieldName = new StringBuilder();
    ObjectHolder value = new ObjectHolder();
    // Process both sides of this node.
    Object left = node.jjtGetChild(0).jjtAccept(this, data);
    Object right = node.jjtGetChild(1).jjtAccept(this, data);
    // Ignore functions in the query
    if (left instanceof FunctionResult || right instanceof FunctionResult)
      return null;
    decodeResults(left, right, fieldName, value);
    // We need to check to see if we are in a NOT context. If so,
    // then we need to reverse the negation.
    boolean negated = false;
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      if (ctx.inNotContext)
        negated = !negated;
    }
    QueryTerm term = new QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
    termsCopy.put(fieldName.toString(), term);
    // Get the terms from the global index
    // Remove the begin and end ' marks
    String termValue = null;
    if (((String) term.getValue()).startsWith("'") && ((String) term.getValue()).endsWith("'"))
      termValue = ((String) term.getValue()).substring(1, ((String) term.getValue()).length() - 1);
    else
      termValue = (String) term.getValue();
    // Get the values found in the index for this query term
    TermRange ranges = null;
    for (MapKey key : this.originalQueryValues.get(termValue)) {
      if (key.getFieldName().equalsIgnoreCase(fieldName.toString())) {
        ranges = this.globalIndexResults.get(key);
        if (log.isDebugEnabled())
          log.debug("Results for cached index ranges for key: " + key + " are " + ranges);
      }
    }
    // If no result for this field name and value, then add empty range
    if (null == ranges)
      ranges = new TermRange(fieldName.toString(), (String) term.getValue());
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      ctx.lastRange = ranges;
      ctx.lastProcessedTerm = fieldName.toString();
    }
    
    return null;
  }
  
  @Override
  public Object visit(ASTNRNode node, Object data) {
    StringBuilder fieldName = new StringBuilder();
    ObjectHolder value = new ObjectHolder();
    // Process both sides of this node.
    Object left = node.jjtGetChild(0).jjtAccept(this, data);
    Object right = node.jjtGetChild(1).jjtAccept(this, data);
    // Ignore functions in the query
    if (left instanceof FunctionResult || right instanceof FunctionResult)
      return null;
    decodeResults(left, right, fieldName, value);
    // We need to check to see if we are in a NOT context. If so,
    // then we need to reverse the negation.
    boolean negated = true;
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      if (ctx.inNotContext)
        negated = !negated;
    }
    if (negated)
      negatedTerms.add(fieldName.toString());
    QueryTerm term = new QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
    termsCopy.put(fieldName.toString(), term);
    // We can only use the global index for equality, put in fake results
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      ctx.lastRange = new TermRange(fieldName.toString(), term.getValue());
      ctx.lastProcessedTerm = fieldName.toString();
      termCardinalities.put(fieldName.toString(), 0L);
    }
    
    return null;
  }
  
  @Override
  public Object visit(ASTNullLiteral node, Object data) {
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      ctx.lastRange = new TermRange("null", "null");
      ctx.lastProcessedTerm = "null";
      termCardinalities.put("null", 0L);
    }
    return new LiteralResult(node.image);
  }
  
  @Override
  public Object visit(ASTTrueNode node, Object data) {
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      ctx.lastRange = new TermRange("true", "true");
      ctx.lastProcessedTerm = "true";
      termCardinalities.put("true", 0L);
    }
    return new LiteralResult(node.image);
  }
  
  @Override
  public Object visit(ASTFalseNode node, Object data) {
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      ctx.lastRange = new TermRange("false", "false");
      ctx.lastProcessedTerm = "false";
      termCardinalities.put("false", 0L);
    }
    return new LiteralResult(node.image);
  }
  
  @Override
  public Object visit(ASTFunctionNode node, Object data) {
    // objectNode 0 is the prefix
    // objectNode 1 is the identifier , the others are parameters.
    // process the remaining arguments
    FunctionResult fr = new FunctionResult();
    int argc = node.jjtGetNumChildren() - 2;
    for (int i = 0; i < argc; i++) {
      // Process both sides of this node.
      Object result = node.jjtGetChild(i + 2).jjtAccept(this, data);
      if (result instanceof TermResult) {
        TermResult tr = (TermResult) result;
        fr.getTerms().add(tr);
        termsCopy.put((String) tr.value, null);
      }
    }
    if (null != data && data instanceof EvaluationContext) {
      EvaluationContext ctx = (EvaluationContext) data;
      ctx.lastRange = new TermRange(node.jjtGetChild(0).image, node.jjtGetChild(1).image);
      ctx.lastProcessedTerm = node.jjtGetChild(0).image;
      termCardinalities.put(node.jjtGetChild(0).image, 0L);
    }
    return fr;
  }
  
}
