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
package org.apache.accumulo.core.client.admin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.AccumuloServerException;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.impl.TabletLocator.TabletLocation;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.TableConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.aggregation.conf.AggregatorConfiguration;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.BulkImportHelper;
import org.apache.accumulo.core.util.BulkImportHelper.AssignmentStats;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.MetadataTable;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.accumulo.core.util.StringUtil;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransportException;

/**
 * Provides a class for administering tables
 * 
 */
public class TableOperationsImpl extends TableOperations {
  private Instance instance;
  private AuthInfo credentials;
  
  private static final Logger log = Logger.getLogger(TableOperations.class);
  
  /**
   * @param instance
   *          the connection information for this instance
   * @param credentials
   *          the username/password for this connection
   */
  public TableOperationsImpl(Instance instance, AuthInfo credentials) {
    ArgumentChecker.notNull(instance, credentials);
    this.instance = instance;
    this.credentials = credentials;
  }
  
  /**
   * Retrieve a list of tables in Accumulo.
   * 
   * @return List of tables in accumulo
   */
  public SortedSet<String> list() {
    OpTimer opTimer = new OpTimer(log, Level.TRACE).start("Fetching list of tables...");
    TreeSet<String> tableNames = new TreeSet<String>(Tables.getNameToIdMap(instance).keySet());
    opTimer.stop("Fetched " + tableNames.size() + " table names in %DURATION%");
    return tableNames;
  }
  
  /**
   * A method to check if a table exists in Accumulo.
   * 
   * @param tableName
   *          the name of the table
   * @return true if the table exists
   */
  public boolean exists(String tableName) {
    ArgumentChecker.notNull(tableName);
    if (tableName.equals(Constants.METADATA_TABLE_NAME))
      return true;
    
    OpTimer opTimer = new OpTimer(log, Level.TRACE).start("Checking if table " + tableName + "exists...");
    boolean exists = Tables.getNameToIdMap(instance).containsKey(tableName);
    opTimer.stop("Checked existance of " + exists + " in %DURATION%");
    return exists;
  }
  
  /**
   * Create a table with no special configuration
   * 
   * @param tableName
   *          the name of the table
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableExistsException
   *           if the table already exists
   */
  public void create(String tableName) throws AccumuloException, AccumuloSecurityException, TableExistsException {
    create(tableName, TimeType.MILLIS);
  }
  
  /**
   * @param tableName
   *          the name of the table
   * @param timeType
   *          specifies logical or real-time based time recording for entries in the table
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableExistsException
   *           if the table already exists
   */
  public void create(String tableName, TimeType timeType) throws AccumuloException, AccumuloSecurityException, TableExistsException {
    ArgumentChecker.notNull(tableName);
    MasterClientService.Iface client = null;
    try {
      org.apache.accumulo.core.master.thrift.TimeType ttt = null;
      
      switch (timeType) {
        case LOGICAL:
          ttt = org.apache.accumulo.core.master.thrift.TimeType.LOGICAL;
          break;
        case MILLIS:
          ttt = org.apache.accumulo.core.master.thrift.TimeType.MILLIS;
          break;
      }
      
      client = MasterClient.getConnection(instance);
      List<byte[]> emptySplits = Collections.emptyList();
      List<AggregatorConfiguration> emptyAggs = Collections.emptyList();
      client.createTable(null, credentials, tableName, emptySplits, IteratorUtil.generateInitialTableProperties(emptyAggs), ttt);
      Tables.clearCache(instance);
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (ThriftTableOperationException e) {
      switch (e.getType()) {
        case EXISTS:
          throw new TableExistsException(e);
        case OTHER:
        default:
          throw new AccumuloException(e.getMessage(), e);
      }
    } catch (Exception e) {
      throw new AccumuloException(e.getMessage(), e);
    } finally {
      MasterClient.close(client);
    }
  }
  
  /**
   * @param tableName
   *          the name of the table
   * @param aggregators
   *          List of aggregators to add
   * @throws AccumuloSecurityException
   *           if insufficient permissions to do action
   * @throws TableNotFoundException
   *           if table does not exist
   * @throws AccumuloException
   *           if a general error occurs
   */
  public void addAggregators(String tableName, List<AggregatorConfiguration> aggregators) throws AccumuloSecurityException, TableNotFoundException,
      AccumuloException {
    ArgumentChecker.notNull(tableName, aggregators);
    MasterClientService.Iface client = null;
    try {
      client = MasterClient.getConnection(instance);
      for (Entry<String,String> entry : IteratorUtil.generateInitialTableProperties(aggregators).entrySet()) {
        client.setTableProperty(null, credentials, tableName, entry.getKey(), entry.getValue());
      }
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (ThriftTableOperationException e) {
      switch (e.getType()) {
        case EXISTS:
          throw new TableNotFoundException(e);
        case OTHER:
        default:
          throw new AccumuloException(e.getMessage(), e);
      }
    } catch (Exception e) {
      throw new AccumuloException(e.getMessage(), e);
    } finally {
      MasterClient.close(client);
    }
  }
  
  /**
   * @param tableName
   *          the name of the table
   * @param partitionKeys
   *          a sorted set of row key values to pre-split the table on
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNotFoundException
   *           if the table does not exist
   */
  public void addSplits(String tableName, SortedSet<Text> partitionKeys) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    TabletLocator tabLocator = TabletLocator.getInstance(instance, credentials, new Text(Tables.getTableId(instance, tableName)));
    
    for (Text split : partitionKeys) {
      boolean successful = false;
      int attempt = 0;
      
      while (!successful) {
        
        if (attempt > 0)
          UtilWaitThread.sleep(100);
        
        attempt++;
        
        TabletLocation tl = tabLocator.locateTablet(split, false, false);
        
        if (tl == null)
          continue;
        
        try {
          TabletClientService.Iface client = ThriftUtil.getTServerClient(tl.tablet_location, instance.getConfiguration());
          try {
            OpTimer opTimer = null;
            if (log.isTraceEnabled())
              opTimer = new OpTimer(log, Level.TRACE).start("Splitting tablet " + tl.tablet_extent + " on " + tl.tablet_location + " at " + split);
            
            client.splitTablet(null, credentials, tl.tablet_extent.toThrift(), TextUtil.getBytes(split));
            
            // just split it, might as well invalidate it in the cache
            tabLocator.invalidateCache(tl.tablet_extent);
            
            if (opTimer != null)
              opTimer.stop("Split tablet in %DURATION%");
          } finally {
            ThriftUtil.returnClient((TServiceClient) client);
          }
          
        } catch (TApplicationException tae) {
          throw new AccumuloServerException(tl.tablet_location, tae);
        } catch (TTransportException e) {
          tabLocator.invalidateCache(tl.tablet_location);
          continue;
        } catch (ThriftSecurityException e) {
          throw new AccumuloSecurityException(e.user, e.code, e);
        } catch (NotServingTabletException e) {
          tabLocator.invalidateCache(tl.tablet_extent);
          continue;
        } catch (TException e) {
          tabLocator.invalidateCache(tl.tablet_location);
          continue;
        }
        
        successful = true;
      }
    }
    
  }
  
  /**
   * @param tableName
   *          the name of the table
   * @return the split points (end-row names) for the table's current split profile
   */
  @Override
  public Collection<Text> getSplits(String tableName) throws TableNotFoundException {
    
    if (!exists(tableName)) {
      throw new TableNotFoundException(null, tableName, "Unknown table for getSplits");
    }
    
    SortedSet<KeyExtent> tablets = new TreeSet<KeyExtent>();
    Map<KeyExtent,String> locations = new TreeMap<KeyExtent,String>();
    
    while (true) {
      try {
        tablets.clear();
        locations.clear();
        MetadataTable.getEntries(instance, credentials, tableName, false, locations, tablets);
        break;
      } catch (Throwable t) {
        log.info(t.getMessage() + " ... retrying ...");
        UtilWaitThread.sleep(3000);
      }
    }
    
    ArrayList<Text> endRows = new ArrayList<Text>(tablets.size());
    
    for (KeyExtent ke : tablets)
      if (ke.getEndRow() != null)
        endRows.add(ke.getEndRow());
    
    return endRows;
  }
  
  /**
   * @param tableName
   *          the name of the table
   * @param maxSplits
   *          specifies the maximum number of splits to return
   * @return the split points (end-row names) for the table's current split profile, grouped into fewer splits so as not to exceed maxSplits
   * @throws TableNotFoundException
   */
  @Override
  public Collection<Text> getSplits(String tableName, int maxSplits) throws TableNotFoundException {
    Collection<Text> endRows = getSplits(tableName);
    
    if (endRows.size() <= maxSplits)
      return endRows;
    
    double r = (maxSplits + 1) / (double) (endRows.size());
    double pos = 0;
    
    ArrayList<Text> subset = new ArrayList<Text>(maxSplits);
    
    int j = 0;
    for (int i = 0; i < endRows.size() && j < maxSplits; i++) {
      pos += r;
      while (pos > 1) {
        subset.add(((ArrayList<Text>) endRows).get(i));
        j++;
        pos -= 1;
      }
    }
    
    return subset;
  }
  
  /**
   * Delete a table
   * 
   * @param tableName
   *          the name of the table
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNotFoundException
   *           if the table does not exist
   */
  public void delete(String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    ArgumentChecker.notNull(tableName);
    MasterClientService.Iface client = null;
    try {
      client = MasterClient.getConnection(instance);
      client.deleteTable(null, credentials, tableName);
      Tables.clearCache(instance);
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (ThriftTableOperationException e) {
      switch (e.getType()) {
        case NOTFOUND:
          throw new TableNotFoundException(e);
        case OTHER:
        default:
          throw new AccumuloException(e.getMessage(), e);
      }
    } catch (Exception e) {
      throw new AccumuloException(e.getMessage(), e);
    } finally {
      MasterClient.close(client);
    }
  }
  
  /**
   * Rename a table
   * 
   * @param oldTableName
   *          the old table name
   * @param newTableName
   *          the new table name
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNotFoundException
   *           if the old table name does not exist
   * @throws TableExistsException
   *           if the new table name already exists
   */
  public void rename(String oldTableName, String newTableName) throws AccumuloSecurityException, TableNotFoundException, AccumuloException,
      TableExistsException {
    MasterClientService.Iface client = null;
    try {
      client = MasterClient.getConnection(instance);
      client.renameTable(null, credentials, oldTableName, newTableName);
      Tables.clearCache(instance);
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (ThriftTableOperationException e) {
      switch (e.getType()) {
        case EXISTS:
          throw new TableExistsException(e);
        case NOTFOUND:
          throw new TableNotFoundException(e);
        case OTHER:
        default:
          throw new AccumuloException(e.getMessage(), e);
      }
    } catch (Exception e) {
      throw new AccumuloException(e.getMessage(), e);
    } finally {
      MasterClient.close(client);
    }
  }
  
  /**
   * Flush a table
   * 
   * @param tableName
   *          the name of the table
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   */
  public void flush(String tableName) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(tableName);
    MasterClientService.Iface client = null;
    try {
      client = MasterClient.getConnection(instance);
      client.flushTable(null, credentials, tableName);
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (Exception e) {
      throw new AccumuloException(e);
    } finally {
      MasterClient.close(client);
    }
  }
  
  /**
   * Sets a property on a table
   * 
   * @param tableName
   *          the name of the table
   * @param property
   *          the name of a per-table property
   * @param value
   *          the value to set a per-table property to
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   */
  public void setProperty(String tableName, String property, String value) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(tableName, property, value);
    MasterClientService.Iface client = null;
    try {
      client = MasterClient.getConnection(instance);
      client.setTableProperty(null, credentials, tableName, property, value);
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (Throwable t) {
      throw new AccumuloException(t);
    } finally {
      MasterClient.close(client);
    }
    TableConfiguration.invalidateCache();
  }
  
  /**
   * Removes a property from a table
   * 
   * @param tableName
   *          the name of the table
   * @param property
   *          the name of a per-table property
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   */
  public void removeProperty(String tableName, String property) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(tableName, property);
    MasterClientService.Iface client = null;
    try {
      client = MasterClient.getConnection(instance);
      client.removeTableProperty(null, credentials, tableName, property);
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (Throwable t) {
      throw new AccumuloException(t);
    } finally {
      MasterClient.close(client);
    }
    TableConfiguration.invalidateCache();
  }
  
  /**
   * Gets properties of a table
   * 
   * @param tableName
   *          the name of the table
   * @return all properties visible by this table (system and per-table properties)
   * @throws TableNotFoundException
   *           if the table does not exist
   */
  public Iterable<Entry<String,String>> getProperties(String tableName) throws TableNotFoundException {
    ArgumentChecker.notNull(tableName);
    return AccumuloConfiguration.getTableConfiguration(instance.getInstanceID(), Tables.getTableId(instance, tableName));
  }
  
  /**
   * Sets a tables locality groups. A tables locality groups can be changed at any time.
   * 
   * @param tableName
   *          the name of the table
   * @param groups
   *          mapping of locality group names to column families in the locality group
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNotFoundException
   *           if the table does not exist
   */
  public void setLocalityGroups(String tableName, Map<String,Set<Text>> groups) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    // ensure locality groups do not overlap
    HashSet<Text> all = new HashSet<Text>();
    for (Entry<String,Set<Text>> entry : groups.entrySet()) {
      
      if (!Collections.disjoint(all, entry.getValue())) {
        throw new IllegalArgumentException("Group " + entry.getKey() + " overlaps with another group");
      }
      
      all.addAll(entry.getValue());
    }
    
    for (Entry<String,Set<Text>> entry : groups.entrySet()) {
      Set<Text> colFams = entry.getValue();
      String value = LocalityGroupUtil.encodeColumnFamilies(colFams);
      setProperty(tableName, Property.TABLE_LOCALITY_GROUP_PREFIX + entry.getKey(), value);
    }
    
    setProperty(tableName, Property.TABLE_LOCALITY_GROUPS.getKey(), StringUtil.join(groups.keySet(), ","));
    
    // remove anything extraneous
    String prefix = Property.TABLE_LOCALITY_GROUP_PREFIX.getKey();
    for (Entry<String,String> entry : getProperties(tableName)) {
      String property = entry.getKey();
      if (property.startsWith(prefix)) {
        // this property configures a locality group, find out which
        // one:
        String[] parts = property.split("\\.");
        String group = parts[parts.length - 1];
        
        if (!groups.containsKey(group)) {
          removeProperty(tableName, property);
        }
      }
    }
  }
  
  /**
   * 
   * Gets the locality groups currently set for a table.
   * 
   * @param tableName
   *          the name of the table
   * @return mapping of locality group names to column families in the locality group
   * @throws AccumuloException
   *           if a general error occurs
   * @throws TableNotFoundException
   *           if the table does not exist
   */
  public Map<String,Set<Text>> getLocalityGroups(String tableName) throws AccumuloException, TableNotFoundException {
    Map<String,Set<ByteSequence>> groups = LocalityGroupUtil.getLocalityGroups(AccumuloConfiguration.getTableConfiguration(instance.getInstanceID(),
        Tables.getTableId(instance, tableName)));
    
    Map<String,Set<Text>> groups2 = new HashMap<String,Set<Text>>();
    for (Entry<String,Set<ByteSequence>> entry : groups.entrySet()) {
      
      HashSet<Text> colFams = new HashSet<Text>();
      
      for (ByteSequence bs : entry.getValue()) {
        colFams.add(new Text(bs.toArray()));
      }
      
      groups2.put(entry.getKey(), colFams);
    }
    
    return groups2;
  }
  
  /**
   * @param tableName
   *          the name of the table
   * @param range
   *          a range to split
   * @param maxSplits
   *          the maximum number of splits
   * @return the range, split into smaller ranges that fall on boundaries of the table's split points as evenly as possible
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNotFoundException
   *           if the table does not exist
   */
  public Set<Range> splitRangeByTablets(String tableName, Range range, int maxSplits) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException {
    ArgumentChecker.notNull(tableName, range);
    if (maxSplits < 1)
      throw new IllegalArgumentException("maximum splits must be >= 1");
    if (maxSplits == 1)
      return Collections.singleton(range);
    
    Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<String,Map<KeyExtent,List<Range>>>();
    TabletLocator tl = TabletLocator.getInstance(instance, credentials, new Text(Tables.getTableId(instance, tableName)));
    while (!tl.binRanges(Collections.singletonList(range), binnedRanges).isEmpty()) {
      log.warn("Unable to locate bins for specified range. Retrying.");
      // sleep randomly between 100 and 200ms
      UtilWaitThread.sleep(100 + (int) (Math.random() * 100));
    }
    
    // group key extents to get <= maxSplits
    LinkedList<KeyExtent> unmergedExtents = new LinkedList<KeyExtent>();
    List<KeyExtent> mergedExtents = new ArrayList<KeyExtent>();
    
    for (Map<KeyExtent,List<Range>> map : binnedRanges.values())
      unmergedExtents.addAll(map.keySet());
    
    // the sort method is efficient for linked list
    Collections.sort(unmergedExtents);
    
    while (unmergedExtents.size() + mergedExtents.size() > maxSplits) {
      if (unmergedExtents.size() >= 2) {
        KeyExtent first = unmergedExtents.removeFirst();
        KeyExtent second = unmergedExtents.removeFirst();
        first.setEndRow(second.getEndRow());
        mergedExtents.add(first);
      } else {
        mergedExtents.addAll(unmergedExtents);
        unmergedExtents.clear();
        unmergedExtents.addAll(mergedExtents);
        mergedExtents.clear();
      }
      
    }
    
    mergedExtents.addAll(unmergedExtents);
    
    Set<Range> ranges = new HashSet<Range>();
    for (KeyExtent k : mergedExtents)
      ranges.add(k.toDataRange().clip(range));
    
    return ranges;
  }
  
  /**
   * @param tableName
   *          the name of the table
   * @param dir
   *          the HDFS directory to find files for importing
   * @param failureDir
   *          the HDFS directory to place files that failed to be imported
   * @param numThreads
   *          the number of threads to use to process the files
   * @param numAssignThreads
   *          the number of threads to use when assigning the files
   * @param disableGC
   *          prevents the garbage collector from cleaning up files that were bulk imported
   * @return the statistics for the operation
   * @throws IOException
   *           when there is an error reading/writing to HDFS
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   */
  public AssignmentStats importDirectory(String tableName, String dir, String failureDir, int numThreads, int numAssignThreads, boolean disableGC)
      throws IOException, AccumuloException, AccumuloSecurityException {
    return new BulkImportHelper(instance, credentials, tableName).importDirectory(new Path(dir), new Path(failureDir), numThreads, numAssignThreads, disableGC);
  }
  
  /**
   * 
   * @param tableName
   *          the table to take offline
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   */
  public void offline(String tableName) throws AccumuloSecurityException, AccumuloException {
    ArgumentChecker.notNull(tableName);
    MasterClientService.Iface client = null;
    try {
      client = MasterClient.getConnection(instance);
      client.offlineTable(null, credentials, tableName);
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (Throwable t) {
      throw new AccumuloException(t);
    } finally {
      MasterClient.close(client);
    }
  }
  
  /**
   * 
   * @param tableName
   *          the table to take online
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   */
  public void online(String tableName) throws AccumuloSecurityException, AccumuloException {
    ArgumentChecker.notNull(tableName);
    MasterClientService.Iface client = null;
    try {
      client = MasterClient.getConnection(instance);
      client.onlineTable(null, credentials, tableName);
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (Throwable t) {
      throw new AccumuloException(t);
    } finally {
      MasterClient.close(client);
    }
  }
  
  /**
   * Clears the tablet locator cache for a specified table
   * 
   * @param tableName
   *          the name of the table
   * @throws TableNotFoundException
   *           if table does not exist
   */
  public void clearLocatorCache(String tableName) throws TableNotFoundException {
    ArgumentChecker.notNull(tableName);
    TabletLocator tabLocator = TabletLocator.getInstance(instance, credentials, new Text(Tables.getTableId(instance, tableName)));
    tabLocator.invalidateCache();
  }
  
  /**
   * Get a mapping of table name to internal table id.
   * 
   * @return the map from table name to internal table id
   */
  public Map<String,String> tableIdMap() {
    return Tables.getNameToIdMap(instance);
  }
}
