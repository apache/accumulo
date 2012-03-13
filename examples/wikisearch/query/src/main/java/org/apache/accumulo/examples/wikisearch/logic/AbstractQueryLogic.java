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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.examples.wikisearch.ingest.WikipediaMapper;
import org.apache.accumulo.examples.wikisearch.iterator.BooleanLogicIterator;
import org.apache.accumulo.examples.wikisearch.iterator.EvaluatingIterator;
import org.apache.accumulo.examples.wikisearch.iterator.OptimizedQueryIterator;
import org.apache.accumulo.examples.wikisearch.iterator.ReadAheadIterator;
import org.apache.accumulo.examples.wikisearch.normalizer.LcNoDiacriticsNormalizer;
import org.apache.accumulo.examples.wikisearch.normalizer.Normalizer;
import org.apache.accumulo.examples.wikisearch.parser.EventFields;
import org.apache.accumulo.examples.wikisearch.parser.EventFields.FieldValue;
import org.apache.accumulo.examples.wikisearch.parser.FieldIndexQueryReWriter;
import org.apache.accumulo.examples.wikisearch.parser.JexlOperatorConstants;
import org.apache.accumulo.examples.wikisearch.parser.QueryParser;
import org.apache.accumulo.examples.wikisearch.parser.QueryParser.QueryTerm;
import org.apache.accumulo.examples.wikisearch.parser.RangeCalculator;
import org.apache.accumulo.examples.wikisearch.sample.Document;
import org.apache.accumulo.examples.wikisearch.sample.Field;
import org.apache.accumulo.examples.wikisearch.sample.Results;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * <pre>
 * <h2>Overview</h2>
 * Query implementation that works with the JEXL grammar. This 
 * uses the metadata, global index, and partitioned table to return
 * results based on the query. Example queries:
 * 
 *  <b>Single Term Query</b>
 *  'foo' - looks in global index for foo, and if any entries are found, then the query
 *          is rewritten to be field1 == 'foo' or field2 == 'foo', etc. This is then passed
 *          down the optimized query path which uses the intersecting iterators on the partitioned
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
 *  rewriting the query in some cases and the current implementation is expecting a space on either side of the operator. If 
 *  an error occurs in the evaluation we are skipping the event.
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
public abstract class AbstractQueryLogic {
  
  protected static Logger log = Logger.getLogger(AbstractQueryLogic.class);
  
  /**
   * Set of datatypes to limit the query to.
   */
  public static final String DATATYPE_FILTER_SET = "datatype.filter.set";
  
  private static class DoNotPerformOptimizedQueryException extends Exception {
    private static final long serialVersionUID = 1L;
  }
  
  /**
   * Object that is used to hold ranges found in the index. Subclasses may compute the final range set in various ways.
   */
  public static abstract class IndexRanges {
    
    private Map<String,String> indexValuesToOriginalValues = null;
    private Multimap<String,String> fieldNamesAndValues = HashMultimap.create();
    private Map<String,Long> termCardinality = new HashMap<String,Long>();
    protected Map<String,TreeSet<Range>> ranges = new HashMap<String,TreeSet<Range>>();
    
    public Multimap<String,String> getFieldNamesAndValues() {
      return fieldNamesAndValues;
    }
    
    public void setFieldNamesAndValues(Multimap<String,String> fieldNamesAndValues) {
      this.fieldNamesAndValues = fieldNamesAndValues;
    }
    
    public final Map<String,Long> getTermCardinality() {
      return termCardinality;
    }
    
    public Map<String,String> getIndexValuesToOriginalValues() {
      return indexValuesToOriginalValues;
    }
    
    public void setIndexValuesToOriginalValues(Map<String,String> indexValuesToOriginalValues) {
      this.indexValuesToOriginalValues = indexValuesToOriginalValues;
    }
    
    public abstract void add(String term, Range r);
    
    public abstract Set<Range> getRanges();
  }
  
  /**
   * Object that computes the ranges by unioning all of the ranges for all of the terms together. In the case where ranges overlap, the largest range is used.
   */
  public static class UnionIndexRanges extends IndexRanges {
    
    public static String DEFAULT_KEY = "default";
    
    public UnionIndexRanges() {
      this.ranges.put(DEFAULT_KEY, new TreeSet<Range>());
    }
    
    public Set<Range> getRanges() {
      // So the set of ranges is ordered. It *should* be the case that
      // ranges with partition ids will sort before ranges that point to
      // a specific event. Populate a new set of ranges but don't add a
      // range for an event where that range is contained in a range already
      // added.
      Set<Text> shardsAdded = new HashSet<Text>();
      Set<Range> returnSet = new HashSet<Range>();
      for (Range r : ranges.get(DEFAULT_KEY)) {
        if (!shardsAdded.contains(r.getStartKey().getRow())) {
          // Only add ranges with a start key for the entire partition.
          if (r.getStartKey().getColumnFamily() == null) {
            shardsAdded.add(r.getStartKey().getRow());
          }
          returnSet.add(r);
        } else {
          // if (log.isTraceEnabled())
          log.info("Skipping event specific range: " + r.toString() + " because range has already been added: "
              + shardsAdded.contains(r.getStartKey().getRow()));
        }
      }
      return returnSet;
    }
    
    public void add(String term, Range r) {
      ranges.get(DEFAULT_KEY).add(r);
    }
  }
  
  private String metadataTableName;
  private String indexTableName;
  private String reverseIndexTableName;
  private String tableName;
  private int queryThreads = 8;
  private String readAheadQueueSize;
  private String readAheadTimeOut;
  private boolean useReadAheadIterator;
  private Kryo kryo = new Kryo();
  private EventFields eventFields = new EventFields();
  private List<String> unevaluatedFields = null;
  private Map<Class<? extends Normalizer>,Normalizer> normalizerCacheMap = new HashMap<Class<? extends Normalizer>,Normalizer>();
  private static final String NULL_BYTE = "\u0000";
  
  public AbstractQueryLogic() {
    super();
    EventFields.initializeKryo(kryo);
  }
  
  /**
   * Queries metadata table to determine which terms are indexed.
   * 
   * @param c
   * @param auths
   * @param queryLiterals
   * @param datatypes
   *          - optional list of types
   * @return map of indexed field names to types to normalizers used in this date range
   * @throws TableNotFoundException
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  protected Map<String,Multimap<String,Class<? extends Normalizer>>> findIndexedTerms(Connector c, Authorizations auths, Set<String> queryLiterals,
      Set<String> datatypes) throws TableNotFoundException, InstantiationException, IllegalAccessException {
    
    Map<String,Multimap<String,Class<? extends Normalizer>>> results = new HashMap<String,Multimap<String,Class<? extends Normalizer>>>();
    
    for (String literal : queryLiterals) {
      if (log.isDebugEnabled())
        log.debug("Querying " + this.getMetadataTableName() + " table for " + literal);
      Range range = new Range(literal.toUpperCase());
      Scanner scanner = c.createScanner(this.getMetadataTableName(), auths);
      scanner.setRange(range);
      scanner.fetchColumnFamily(new Text(WikipediaMapper.METADATA_INDEX_COLUMN_FAMILY));
      for (Entry<Key,Value> entry : scanner) {
        if (!results.containsKey(literal)) {
          Multimap<String,Class<? extends Normalizer>> m = HashMultimap.create();
          results.put(literal, m);
        }
        // Get the column qualifier from the key. It contains the datatype and normalizer class
        String colq = entry.getKey().getColumnQualifier().toString();
        if (null != colq && colq.contains("\0")) {
          int idx = colq.indexOf("\0");
          if (idx != -1) {
            String type = colq.substring(0, idx);
            // If types are specified and this type is not in the list then skip it.
            if (null != datatypes && !datatypes.contains(type))
              continue;
            try {
              @SuppressWarnings("unchecked")
              Class<? extends Normalizer> clazz = (Class<? extends Normalizer>) Class.forName(colq.substring(idx + 1));
              if (!normalizerCacheMap.containsKey(clazz))
                normalizerCacheMap.put(clazz, clazz.newInstance());
              results.get(literal).put(type, clazz);
            } catch (ClassNotFoundException e) {
              log.error("Unable to find normalizer on class path: " + colq.substring(idx + 1), e);
              results.get(literal).put(type, LcNoDiacriticsNormalizer.class);
            }
          } else {
            log.warn("EventMetadata entry did not contain NULL byte: " + entry.getKey().toString());
          }
        } else {
          log.warn("ColumnQualifier null in EventMetadata for key: " + entry.getKey().toString());
        }
      }
    }
    if (log.isDebugEnabled())
      log.debug("METADATA RESULTS: " + results.toString());
    return results;
  }
  
  /**
   * Performs a lookup in the global index for a single non-fielded term.
   * 
   * @param c
   * @param auths
   * @param value
   * @param datatypes
   *          - optional list of types
   * @return ranges that fit into the date range.
   */
  protected abstract IndexRanges getTermIndexInformation(Connector c, Authorizations auths, String value, Set<String> datatypes) throws TableNotFoundException;
  
  /**
   * Performs a lookup in the global index / reverse index and returns a RangeCalculator
   * 
   * @param c
   *          Accumulo connection
   * @param auths
   *          authset for queries
   * @param indexedTerms
   *          multimap of indexed field name and Normalizers used
   * @param terms
   *          multimap of field name and QueryTerm object
   * @param indexTableName
   * @param reverseIndexTableName
   * @param queryString
   *          original query string
   * @param queryThreads
   * @param datatypes
   *          - optional list of types
   * @return range calculator
   * @throws TableNotFoundException
   */
  protected abstract RangeCalculator getTermIndexInformation(Connector c, Authorizations auths, Multimap<String,Normalizer> indexedTerms,
      Multimap<String,QueryTerm> terms, String indexTableName, String reverseIndexTableName, String queryString, int queryThreads, Set<String> datatypes)
      throws TableNotFoundException, org.apache.commons.jexl2.parser.ParseException;
  
  protected abstract Collection<Range> getFullScanRange(Date begin, Date end, Multimap<String,QueryTerm> terms);
  
  public String getMetadataTableName() {
    return metadataTableName;
  }
  
  public String getIndexTableName() {
    return indexTableName;
  }
  
  public String getTableName() {
    return tableName;
  }
  
  public void setMetadataTableName(String metadataTableName) {
    this.metadataTableName = metadataTableName;
  }
  
  public void setIndexTableName(String indexTableName) {
    this.indexTableName = indexTableName;
  }
  
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }
  
  public int getQueryThreads() {
    return queryThreads;
  }
  
  public void setQueryThreads(int queryThreads) {
    this.queryThreads = queryThreads;
  }
  
  public String getReadAheadQueueSize() {
    return readAheadQueueSize;
  }
  
  public String getReadAheadTimeOut() {
    return readAheadTimeOut;
  }
  
  public boolean isUseReadAheadIterator() {
    return useReadAheadIterator;
  }
  
  public void setReadAheadQueueSize(String readAheadQueueSize) {
    this.readAheadQueueSize = readAheadQueueSize;
  }
  
  public void setReadAheadTimeOut(String readAheadTimeOut) {
    this.readAheadTimeOut = readAheadTimeOut;
  }
  
  public void setUseReadAheadIterator(boolean useReadAheadIterator) {
    this.useReadAheadIterator = useReadAheadIterator;
  }
  
  public String getReverseIndexTableName() {
    return reverseIndexTableName;
  }
  
  public void setReverseIndexTableName(String reverseIndexTableName) {
    this.reverseIndexTableName = reverseIndexTableName;
  }
  
  public List<String> getUnevaluatedFields() {
    return unevaluatedFields;
  }
  
  public void setUnevaluatedFields(List<String> unevaluatedFields) {
    this.unevaluatedFields = unevaluatedFields;
  }
  
  public void setUnevaluatedFields(String unevaluatedFieldList) {
    this.unevaluatedFields = new ArrayList<String>();
    for (String field : unevaluatedFieldList.split(","))
      this.unevaluatedFields.add(field);
  }
  
  public Document createDocument(Key key, Value value) {
    Document doc = new Document();

    eventFields.clear();
    ByteBuffer buf = ByteBuffer.wrap(value.get());
    eventFields.readObjectData(kryo, buf);
    
    // Set the id to the document id which is located in the colf
    String row = key.getRow().toString();
    String colf = key.getColumnFamily().toString();
    int idx = colf.indexOf(NULL_BYTE);
    String type = colf.substring(0, idx);
    String id = colf.substring(idx + 1);
    doc.setId(id);
    for (Entry<String,Collection<FieldValue>> entry : eventFields.asMap().entrySet()) {
      for (FieldValue fv : entry.getValue()) {
        Field val = new Field();
        val.setFieldName(entry.getKey());
        val.setFieldValue(new String(fv.getValue(), Charset.forName("UTF-8")));
        doc.getFields().add(val);
      }
    }
    
    // Add the pointer for the content.
    Field docPointer = new Field();
    docPointer.setFieldName("DOCUMENT");
    docPointer.setFieldValue("DOCUMENT:" + row + "/" + type + "/" + id);
    doc.getFields().add(docPointer);
    
    return doc;
  }
  
  public String getResultsKey(Entry<Key,Value> key) {
    // Use the colf from the table, it contains the uuid and datatype
    return key.getKey().getColumnFamily().toString();
  }
  
  public Results runQuery(Connector connector, List<String> authorizations, String query, Date beginDate, Date endDate, Set<String> types) {
    
    if (StringUtils.isEmpty(query)) {
      throw new IllegalArgumentException("NULL QueryNode reference passed to " + this.getClass().getSimpleName());
    }
    
    Set<Range> ranges = new HashSet<Range>();
    Set<String> typeFilter = types;
    String array[] = authorizations.toArray(new String[0]);
    Authorizations auths = new Authorizations(array);
    Results results = new Results();
    
    // Get the query string
    String queryString = query;
    
    StopWatch abstractQueryLogic = new StopWatch();
    StopWatch optimizedQuery = new StopWatch();
    StopWatch queryGlobalIndex = new StopWatch();
    StopWatch optimizedEventQuery = new StopWatch();
    StopWatch fullScanQuery = new StopWatch();
    StopWatch processResults = new StopWatch();
    
    abstractQueryLogic.start();
    
    StopWatch parseQuery = new StopWatch();
    parseQuery.start();
    
    QueryParser parser;
    try {
      if (log.isDebugEnabled()) {
        log.debug("ShardQueryLogic calling QueryParser.execute");
      }
      parser = new QueryParser();
      parser.execute(queryString);
    } catch (org.apache.commons.jexl2.parser.ParseException e1) {
      throw new IllegalArgumentException("Error parsing query", e1);
    }
    int hash = parser.getHashValue();
    parseQuery.stop();
    if (log.isDebugEnabled()) {
      log.debug(hash + " Query: " + queryString);
    }
    
    Set<String> fields = new HashSet<String>();
    for (String f : parser.getQueryIdentifiers()) {
      fields.add(f);
    }
    if (log.isDebugEnabled()) {
      log.debug("getQueryIdentifiers: " + parser.getQueryIdentifiers().toString());
    }
    // Remove any negated fields from the fields list, we don't want to lookup negated fields
    // in the index.
    fields.removeAll(parser.getNegatedTermsForOptimizer());
    
    if (log.isDebugEnabled()) {
      log.debug("getQueryIdentifiers: " + parser.getQueryIdentifiers().toString());
    }
    // Get the mapping of field name to QueryTerm object from the query. The query term object
    // contains the operator, whether its negated or not, and the literal to test against.
    Multimap<String,QueryTerm> terms = parser.getQueryTerms();
    
    // Find out which terms are indexed
    // TODO: Should we cache indexed terms or does that not make sense since we are always
    // loading data.
    StopWatch queryMetadata = new StopWatch();
    queryMetadata.start();
    Map<String,Multimap<String,Class<? extends Normalizer>>> metadataResults;
    try {
      metadataResults = findIndexedTerms(connector, auths, fields, typeFilter);
    } catch (Exception e1) {
      throw new RuntimeException("Error in metadata lookup", e1);
    }
    
    // Create a map of indexed term to set of normalizers for it
    Multimap<String,Normalizer> indexedTerms = HashMultimap.create();
    for (Entry<String,Multimap<String,Class<? extends Normalizer>>> entry : metadataResults.entrySet()) {
      // Get the normalizer from the normalizer cache
      for (Class<? extends Normalizer> clazz : entry.getValue().values()) {
        indexedTerms.put(entry.getKey(), normalizerCacheMap.get(clazz));
      }
    }
    queryMetadata.stop();
    if (log.isDebugEnabled()) {
      log.debug(hash + " Indexed Terms: " + indexedTerms.toString());
    }
    
    Set<String> orTerms = parser.getOrTermsForOptimizer();
    
    // Iterate over the query terms to get the operators specified in the query.
    ArrayList<String> unevaluatedExpressions = new ArrayList<String>();
    boolean unsupportedOperatorSpecified = false;
    for (Entry<String,QueryTerm> entry : terms.entries()) {
      if (null == entry.getValue()) {
        continue;
      }
      
      if (null != this.unevaluatedFields && this.unevaluatedFields.contains(entry.getKey().trim())) {
        unevaluatedExpressions.add(entry.getKey().trim() + " " + entry.getValue().getOperator() + " " + entry.getValue().getValue());
      }
      
      int operator = JexlOperatorConstants.getJJTNodeType(entry.getValue().getOperator());
      if (!(operator == ParserTreeConstants.JJTEQNODE || operator == ParserTreeConstants.JJTNENODE || operator == ParserTreeConstants.JJTLENODE
          || operator == ParserTreeConstants.JJTLTNODE || operator == ParserTreeConstants.JJTGENODE || operator == ParserTreeConstants.JJTGTNODE || operator == ParserTreeConstants.JJTERNODE)) {
        unsupportedOperatorSpecified = true;
        break;
      }
    }
    if (null != unevaluatedExpressions)
      unevaluatedExpressions.trimToSize();
    if (log.isDebugEnabled()) {
      log.debug(hash + " unsupportedOperators: " + unsupportedOperatorSpecified + " indexedTerms: " + indexedTerms.toString() + " orTerms: "
          + orTerms.toString() + " unevaluatedExpressions: " + unevaluatedExpressions.toString());
    }
    
    // We can use the intersecting iterator over the field index as an optimization under the
    // following conditions
    //
    // 1. No unsupported operators in the query.
    // 2. No 'or' operators and at least one term indexed
    // or
    // 1. No unsupported operators in the query.
    // 2. and all terms indexed
    // or
    // 1. All or'd terms are indexed. NOTE, this will potentially skip some queries and push to a full table scan
    // // WE should look into finding a better way to handle whether we do an optimized query or not.
    boolean optimizationSucceeded = false;
    boolean orsAllIndexed = false;
    if (orTerms.isEmpty()) {
      orsAllIndexed = false;
    } else {
      orsAllIndexed = indexedTerms.keySet().containsAll(orTerms);
    }
    
    if (log.isDebugEnabled()) {
      log.debug("All or terms are indexed");
    }
    
    if (!unsupportedOperatorSpecified
        && (((null == orTerms || orTerms.isEmpty()) && indexedTerms.size() > 0) || (fields.size() > 0 && indexedTerms.size() == fields.size()) || orsAllIndexed)) {
      optimizedQuery.start();
      // Set up intersecting iterator over field index.
      
      // Get information from the global index for the indexed terms. The results object will contain the term
      // mapped to an object that contains the total count, and partitions where this term is located.
      
      // TODO: Should we cache indexed term information or does that not make sense since we are always loading data
      queryGlobalIndex.start();
      IndexRanges termIndexInfo;
      try {
        // If fields is null or zero, then it's probably the case that the user entered a value
        // to search for with no fields. Check for the value in index.
        if (fields.isEmpty()) {
          termIndexInfo = this.getTermIndexInformation(connector, auths, queryString, typeFilter);
          if (null != termIndexInfo && termIndexInfo.getRanges().isEmpty()) {
            // Then we didn't find anything in the index for this query. This may happen for an indexed term that has wildcards
            // in unhandled locations.
            // Break out of here by throwing a named exception and do full scan
            throw new DoNotPerformOptimizedQueryException();
          }
          // We need to rewrite the query string here so that it's valid.
          if (termIndexInfo instanceof UnionIndexRanges) {
            UnionIndexRanges union = (UnionIndexRanges) termIndexInfo;
            StringBuilder buf = new StringBuilder();
            String sep = "";
            for (String fieldName : union.getFieldNamesAndValues().keySet()) {
              buf.append(sep).append(fieldName).append(" == ");
              if (!(queryString.startsWith("'") && queryString.endsWith("'"))) {
                buf.append("'").append(queryString).append("'");
              } else {
                buf.append(queryString);
              }
              sep = " or ";
            }
            if (log.isDebugEnabled()) {
              log.debug("Rewrote query for non-fielded single term query: " + queryString + " to " + buf.toString());
            }
            queryString = buf.toString();
          } else {
            throw new RuntimeException("Unexpected IndexRanges implementation");
          }
        } else {
          RangeCalculator calc = this.getTermIndexInformation(connector, auths, indexedTerms, terms, this.getIndexTableName(), this.getReverseIndexTableName(),
              queryString, this.queryThreads, typeFilter);
          if (null == calc.getResult() || calc.getResult().isEmpty()) {
            // Then we didn't find anything in the index for this query. This may happen for an indexed term that has wildcards
            // in unhandled locations.
            // Break out of here by throwing a named exception and do full scan
            throw new DoNotPerformOptimizedQueryException();
          }
          termIndexInfo = new UnionIndexRanges();
          termIndexInfo.setIndexValuesToOriginalValues(calc.getIndexValues());
          termIndexInfo.setFieldNamesAndValues(calc.getIndexEntries());
          termIndexInfo.getTermCardinality().putAll(calc.getTermCardinalities());
          for (Range r : calc.getResult()) {
            // foo is a placeholder and is ignored.
            termIndexInfo.add("foo", r);
          }
        }
      } catch (TableNotFoundException e) {
        log.error(this.getIndexTableName() + "not found", e);
        throw new RuntimeException(this.getIndexTableName() + "not found", e);
      } catch (org.apache.commons.jexl2.parser.ParseException e) {
        throw new RuntimeException("Error determining ranges for query: " + queryString, e);
      } catch (DoNotPerformOptimizedQueryException e) {
        log.info("Indexed fields not found in index, performing full scan");
        termIndexInfo = null;
      }
      queryGlobalIndex.stop();
      
      // Determine if we should proceed with optimized query based on results from the global index
      boolean proceed = false;
      if (null == termIndexInfo || termIndexInfo.getFieldNamesAndValues().values().size() == 0) {
        proceed = false;
      } else if (null != orTerms && orTerms.size() > 0 && (termIndexInfo.getFieldNamesAndValues().values().size() == indexedTerms.size())) {
        proceed = true;
      } else if (termIndexInfo.getFieldNamesAndValues().values().size() > 0) {
        proceed = true;
      } else if (orsAllIndexed) {
        proceed = true;
      } else {
        proceed = false;
      }
      if (log.isDebugEnabled()) {
        log.debug("Proceed with optimized query: " + proceed);
        if (null != termIndexInfo)
          log.debug("termIndexInfo.getTermsFound().size(): " + termIndexInfo.getFieldNamesAndValues().values().size() + " indexedTerms.size: "
              + indexedTerms.size() + " fields.size: " + fields.size());
      }
      if (proceed) {
        
        if (log.isDebugEnabled()) {
          log.debug(hash + " Performing optimized query");
        }
        // Use the scan ranges from the GlobalIndexRanges object as the ranges for the batch scanner
        ranges = termIndexInfo.getRanges();
        if (log.isDebugEnabled()) {
          log.info(hash + " Ranges: count: " + ranges.size() + ", " + ranges.toString());
        }
        
        // Create BatchScanner, set the ranges, and setup the iterators.
        optimizedEventQuery.start();
        BatchScanner bs = null;
        try {
          bs = connector.createBatchScanner(this.getTableName(), auths, queryThreads);
          bs.setRanges(ranges);
          IteratorSetting si = new IteratorSetting(21, "eval", OptimizedQueryIterator.class);
          
          if (log.isDebugEnabled()) {
            log.debug("Setting scan option: " + EvaluatingIterator.QUERY_OPTION + " to " + queryString);
          }
          // Set the query option
          si.addOption(EvaluatingIterator.QUERY_OPTION, queryString);
          // Set the Indexed Terms List option. This is the field name and normalized field value pair separated
          // by a comma.
          StringBuilder buf = new StringBuilder();
          String sep = "";
          for (Entry<String,String> entry : termIndexInfo.getFieldNamesAndValues().entries()) {
            buf.append(sep);
            buf.append(entry.getKey());
            buf.append(":");
            buf.append(termIndexInfo.getIndexValuesToOriginalValues().get(entry.getValue()));
            buf.append(":");
            buf.append(entry.getValue());
            if (sep.equals("")) {
              sep = ";";
            }
          }
          if (log.isDebugEnabled()) {
            log.debug("Setting scan option: " + FieldIndexQueryReWriter.INDEXED_TERMS_LIST + " to " + buf.toString());
          }
          FieldIndexQueryReWriter rewriter = new FieldIndexQueryReWriter();
          String q = "";
          try {
            q = queryString;
            q = rewriter.applyCaseSensitivity(q, true, false);// Set upper/lower case for fieldname/fieldvalue
            Map<String,String> opts = new HashMap<String,String>();
            opts.put(FieldIndexQueryReWriter.INDEXED_TERMS_LIST, buf.toString());
            q = rewriter.removeNonIndexedTermsAndInvalidRanges(q, opts);
            q = rewriter.applyNormalizedTerms(q, opts);
            if (log.isDebugEnabled()) {
              log.debug("runServerQuery, FieldIndex Query: " + q);
            }
          } catch (org.apache.commons.jexl2.parser.ParseException ex) {
            log.error("Could not parse query, Jexl ParseException: " + ex);
          } catch (Exception ex) {
            log.error("Problem rewriting query, Exception: " + ex.getMessage());
          }
          si.addOption(BooleanLogicIterator.FIELD_INDEX_QUERY, q);
          
          // Set the term cardinality option
          sep = "";
          buf.delete(0, buf.length());
          for (Entry<String,Long> entry : termIndexInfo.getTermCardinality().entrySet()) {
            buf.append(sep);
            buf.append(entry.getKey());
            buf.append(":");
            buf.append(entry.getValue());
            sep = ",";
          }
          if (log.isDebugEnabled())
            log.debug("Setting scan option: " + BooleanLogicIterator.TERM_CARDINALITIES + " to " + buf.toString());
          si.addOption(BooleanLogicIterator.TERM_CARDINALITIES, buf.toString());
          if (this.useReadAheadIterator) {
            if (log.isDebugEnabled()) {
              log.debug("Enabling read ahead iterator with queue size: " + this.readAheadQueueSize + " and timeout: " + this.readAheadTimeOut);
            }
            si.addOption(ReadAheadIterator.QUEUE_SIZE, this.readAheadQueueSize);
            si.addOption(ReadAheadIterator.TIMEOUT, this.readAheadTimeOut);
            
          }
          
          if (null != unevaluatedExpressions) {
            StringBuilder unevaluatedExpressionList = new StringBuilder();
            String sep2 = "";
            for (String exp : unevaluatedExpressions) {
              unevaluatedExpressionList.append(sep2).append(exp);
              sep2 = ",";
            }
            if (log.isDebugEnabled())
              log.debug("Setting scan option: " + EvaluatingIterator.UNEVALUTED_EXPRESSIONS + " to " + unevaluatedExpressionList.toString());
            si.addOption(EvaluatingIterator.UNEVALUTED_EXPRESSIONS, unevaluatedExpressionList.toString());
          }
          
          bs.addScanIterator(si);
          
          processResults.start();
          processResults.suspend();
          long count = 0;
          for (Entry<Key,Value> entry : bs) {
            count++;
            // The key that is returned by the EvaluatingIterator is not the same key that is in
            // the table. The value that is returned by the EvaluatingIterator is a kryo
            // serialized EventFields object.
            processResults.resume();
            Document d = this.createDocument(entry.getKey(), entry.getValue());
            results.getResults().add(d);
            processResults.suspend();
          }
          log.info(count + " matching entries found in optimized query.");
          optimizationSucceeded = true;
          processResults.stop();
        } catch (TableNotFoundException e) {
          log.error(this.getTableName() + "not found", e);
          throw new RuntimeException(this.getIndexTableName() + "not found", e);
        } finally {
          if (bs != null) {
            bs.close();
          }
        }
        optimizedEventQuery.stop();
      }
      optimizedQuery.stop();
    }
    
    // WE should look into finding a better way to handle whether we do an optimized query or not.
    // We are not setting up an else condition here because we may have aborted the logic early in the if statement.
    if (!optimizationSucceeded || ((null != orTerms && orTerms.size() > 0) && (indexedTerms.size() != fields.size()) && !orsAllIndexed)) {
      // if (!optimizationSucceeded || ((null != orTerms && orTerms.size() > 0) && (indexedTerms.size() != fields.size()))) {
      fullScanQuery.start();
      if (log.isDebugEnabled()) {
        log.debug(hash + " Performing full scan query");
      }
      
      // Set up a full scan using the date ranges from the query
      // Create BatchScanner, set the ranges, and setup the iterators.
      BatchScanner bs = null;
      try {
        // The ranges are the start and end dates
        Collection<Range> r = getFullScanRange(beginDate, endDate, terms);
        ranges.addAll(r);
        
        if (log.isDebugEnabled()) {
          log.debug(hash + " Ranges: count: " + ranges.size() + ", " + ranges.toString());
        }
        
        bs = connector.createBatchScanner(this.getTableName(), auths, queryThreads);
        bs.setRanges(ranges);
        IteratorSetting si = new IteratorSetting(22, "eval", EvaluatingIterator.class);
        // Create datatype regex if needed
        if (null != typeFilter) {
          StringBuilder buf = new StringBuilder();
          String s = "";
          for (String type : typeFilter) {
            buf.append(s).append(type).append(".*");
            s = "|";
          }
          if (log.isDebugEnabled())
            log.debug("Setting colf regex iterator to: " + buf.toString());
          IteratorSetting ri = new IteratorSetting(21, "typeFilter", RegExFilter.class);
          RegExFilter.setRegexs(ri, null, buf.toString(), null, null, false);
          bs.addScanIterator(ri);
        }
        if (log.isDebugEnabled()) {
          log.debug("Setting scan option: " + EvaluatingIterator.QUERY_OPTION + " to " + queryString);
        }
        si.addOption(EvaluatingIterator.QUERY_OPTION, queryString);
        if (null != unevaluatedExpressions) {
          StringBuilder unevaluatedExpressionList = new StringBuilder();
          String sep2 = "";
          for (String exp : unevaluatedExpressions) {
            unevaluatedExpressionList.append(sep2).append(exp);
            sep2 = ",";
          }
          if (log.isDebugEnabled())
            log.debug("Setting scan option: " + EvaluatingIterator.UNEVALUTED_EXPRESSIONS + " to " + unevaluatedExpressionList.toString());
          si.addOption(EvaluatingIterator.UNEVALUTED_EXPRESSIONS, unevaluatedExpressionList.toString());
        }
        bs.addScanIterator(si);
        long count = 0;
        processResults.start();
        processResults.suspend();
        for (Entry<Key,Value> entry : bs) {
          count++;
          // The key that is returned by the EvaluatingIterator is not the same key that is in
          // the partition table. The value that is returned by the EvaluatingIterator is a kryo
          // serialized EventFields object.
          processResults.resume();
          Document d = this.createDocument(entry.getKey(), entry.getValue());
          results.getResults().add(d);
          processResults.suspend();
        }
        processResults.stop();
        log.info(count + " matching entries found in full scan query.");
      } catch (TableNotFoundException e) {
        log.error(this.getTableName() + "not found", e);
      } finally {
        if (bs != null) {
          bs.close();
        }
      }
      fullScanQuery.stop();
    }
    
    log.info("AbstractQueryLogic: " + queryString + " " + timeString(abstractQueryLogic.getTime()));
    log.info("  1) parse query " + timeString(parseQuery.getTime()));
    log.info("  2) query metadata " + timeString(queryMetadata.getTime()));
    log.info("  3) full scan query " + timeString(fullScanQuery.getTime()));
    log.info("  3) optimized query " + timeString(optimizedQuery.getTime()));
    log.info("  1) process results " + timeString(processResults.getTime()));
    log.info("      1) query global index " + timeString(queryGlobalIndex.getTime()));
    log.info(hash + " Query completed.");
    
    return results;
  }
  
  private static String timeString(long millis) {
    return String.format("%4.2f", millis / 1000.);
  }
  
}
