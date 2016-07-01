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

import java.util.List;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.TabletId;

/**
 *
 * @since 1.5.0
 */
public abstract class ActiveCompaction {

  public static enum CompactionType {
    /**
     * compaction to flush a tablets memory
     */
    MINOR,
    /**
     * compaction to flush a tablets memory and merge it with the tablets smallest file. This type compaction is done when a tablet has too many files
     */
    MERGE,
    /**
     * compaction that merges a subset of a tablets files into one file
     */
    MAJOR,
    /**
     * compaction that merges all of a tablets files into one file
     */
    FULL
  }

  public static enum CompactionReason {
    /**
     * compaction initiated by user
     */
    USER,
    /**
     * Compaction initiated by system
     */
    SYSTEM,
    /**
     * Compaction initiated by merge operation
     */
    CHOP,
    /**
     * idle compaction
     */
    IDLE,
    /**
     * Compaction initiated to close a unload a tablet
     */
    CLOSE
  }

  /**
   *
   * @return name of the table the compaction is running against
   */
  public abstract String getTable() throws TableNotFoundException;

  /**
   * @return tablet thats is compacting
   * @since 1.7.0
   */
  public abstract TabletId getTablet();

  /**
   * @return how long the compaction has been running in milliseconds
   */
  public abstract long getAge();

  /**
   * @return the files the compaction is reading from
   */
  public abstract List<String> getInputFiles();

  /**
   * @return file compactions is writing too
   */
  public abstract String getOutputFile();

  /**
   * @return the type of compaction
   */
  public abstract CompactionType getType();

  /**
   * @return the reason the compaction was started
   */
  public abstract CompactionReason getReason();

  /**
   * @return the locality group that is compacting
   */
  public abstract String getLocalityGroup();

  /**
   * @return the number of key/values read by the compaction
   */
  public abstract long getEntriesRead();

  /**
   * @return the number of key/values written by the compaction
   */
  public abstract long getEntriesWritten();

  /**
   * @return the per compaction iterators configured
   */
  public abstract List<IteratorSetting> getIterators();
}
