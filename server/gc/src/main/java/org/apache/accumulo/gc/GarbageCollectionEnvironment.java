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
package org.apache.accumulo.gc;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ScanFileColumnFamily;
import org.apache.accumulo.server.replication.proto.Replication.Status;

/**
 *
 */
public interface GarbageCollectionEnvironment {

  /**
   * Return a list of paths to files and dirs which are candidates for deletion from a given table, {@link RootTable#NAME} or {@link MetadataTable#NAME}
   *
   * @param continuePoint
   *          A row to resume from if a previous invocation was stopped due to finding an extremely large number of candidates to remove which would have
   *          exceeded memory limitations
   * @param candidates
   *          A collection of candidates files for deletion, may not be the complete collection of files for deletion at this point in time
   * @return true if the results are short due to insufficient memory, otherwise false
   */
  boolean getCandidates(String continuePoint, List<String> candidates) throws TableNotFoundException, AccumuloException, AccumuloSecurityException;

  /**
   * Fetch a list of paths for all bulk loads in progress (blip) from a given table, {@link RootTable#NAME} or {@link MetadataTable#NAME}
   *
   * @return The list of files for each bulk load currently in progress.
   */
  Iterator<String> getBlipIterator() throws TableNotFoundException, AccumuloException, AccumuloSecurityException;

  /**
   * Fetches the references to files, {@link DataFileColumnFamily#NAME} or {@link ScanFileColumnFamily#NAME}, from tablets
   *
   * @return An {@link Iterator} of {@link Entry}&lt;{@link Key}, {@link Value}&gt; which constitute a reference to a file.
   */
  Iterator<Entry<Key,Value>> getReferenceIterator() throws TableNotFoundException, AccumuloException, AccumuloSecurityException;

  /**
   * Return the set of tableIDs for the given instance this GarbageCollector is running over
   *
   * @return The valueSet for the table name to table id map.
   */
  Set<Table.ID> getTableIDs();

  /**
   * Delete the given files from the provided {@link Map} of relative path to absolute path for each file that should be deleted
   *
   * @param candidateMap
   *          A Map from relative path to absolute path for files to be deleted.
   */
  void delete(SortedMap<String,String> candidateMap) throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException;

  /**
   * Delete a table's directory if it is empty.
   *
   * @param tableID
   *          The id of the table whose directory we are to operate on
   */
  void deleteTableDirIfEmpty(Table.ID tableID) throws IOException;

  /**
   * Increment the number of candidates for deletion for the current garbage collection run
   *
   * @param i
   *          Value to increment the deletion candidates by
   */
  void incrementCandidatesStat(long i);

  /**
   * Increment the number of files still in use for the current garbage collection run
   *
   * @param i
   *          Value to increment the still-in-use count by.
   */
  void incrementInUseStat(long i);

  /**
   * Determine if the given absolute file is still pending replication
   *
   * @return True if the file still needs to be replicated
   */
  Iterator<Entry<String,Status>> getReplicationNeededIterator() throws AccumuloException, AccumuloSecurityException;
}
