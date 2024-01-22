/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.gc;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.gc.GcCandidate;
import org.apache.accumulo.core.gc.Reference;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.schema.Ample.GcCandidateType;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ScanFileColumnFamily;

public interface GarbageCollectionEnvironment {

  /**
   * Return an iterator which points to a list of paths to files and dirs which are candidates for
   * deletion from a given table, {@link org.apache.accumulo.core.metadata.AccumuloTable#ROOT} or
   * {@link org.apache.accumulo.core.metadata.AccumuloTable#METADATA}
   *
   * @return an iterator referencing a List containing deletion candidates
   */
  Iterator<GcCandidate> getCandidates() throws TableNotFoundException;

  /**
   * Used for determining if deletion of InUse candidates is enabled.
   *
   * @return value of {@link Property#GC_REMOVE_IN_USE_CANDIDATES}
   */
  boolean canRemoveInUseCandidates();

  /**
   * Given an iterator to a deletion candidate list, return a sub-list of candidates which fit
   * within provided memory constraints.
   *
   * @param candidatesIter iterator referencing a List of possible deletion candidates
   * @return a List of possible deletion candidates
   */
  List<GcCandidate> readCandidatesThatFitInMemory(Iterator<GcCandidate> candidatesIter);

  /**
   * Fetch a list of paths for all bulk loads in progress (blip) from a given table,
   * {@link org.apache.accumulo.core.metadata.AccumuloTable#ROOT} or
   * {@link org.apache.accumulo.core.metadata.AccumuloTable#METADATA}
   *
   * @return The list of files for each bulk load currently in progress.
   */
  Stream<String> getBlipPaths() throws TableNotFoundException;

  /**
   * Fetches the references to files, {@link DataFileColumnFamily#NAME} or
   * {@link ScanFileColumnFamily#NAME}, from tablets and tablet directories.
   *
   * @return An {@link Stream} of {@link Reference} objects, that will need to be closed.
   */
  Stream<Reference> getReferences();

  /**
   * Return a set of all TableIDs that should be seen in {@link #getReferences()} at the current
   * time. Immediately after this method returns the information it produced may be out of date
   * relative to {@link #getReferences()}. See also the javadoc for
   * ({@link GCRun#getCandidateTableIDs()}.
   *
   * @return set of table ids
   * @throws InterruptedException if interrupted when calling ZooKeeper
   */
  Set<TableId> getCandidateTableIDs() throws InterruptedException;

  /**
   * Return the map of tableIDs and TableStates for the given instance this GarbageCollector is
   * running over
   *
   * @return The valueSet for the table name to table id map.
   * @throws InterruptedException if interrupted when calling ZooKeeper
   */
  Map<TableId,TableState> getTableIDs() throws InterruptedException;

  /**
   * Delete the given files from the provided {@link Map} of relative path to absolute path for each
   * file that should be deleted. The candidates should already be confirmed for deletion.
   *
   * @param candidateMap A Map from relative path to absolute path for files to be deleted.
   */
  void deleteConfirmedCandidates(SortedMap<String,GcCandidate> candidateMap)
      throws TableNotFoundException;

  /**
   * Delete in-use reference candidates based on property settings
   *
   * @param GcCandidates Collection of deletion reference candidates to remove.
   */
  void deleteGcCandidates(Collection<GcCandidate> GcCandidates, GcCandidateType type);

  /**
   * Delete a table's directory if it is empty.
   *
   * @param tableID The id of the table whose directory we are to operate on
   */
  void deleteTableDirIfEmpty(TableId tableID) throws IOException;

  /**
   * Increment the number of candidates for deletion for the current garbage collection run
   *
   * @param i Value to increment the deletion candidates by
   */
  void incrementCandidatesStat(long i);

  /**
   * Increment the number of files still in use for the current garbage collection run
   *
   * @param i Value to increment the still-in-use count by.
   */
  void incrementInUseStat(long i);

}
