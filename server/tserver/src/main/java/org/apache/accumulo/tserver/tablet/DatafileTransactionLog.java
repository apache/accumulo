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
package org.apache.accumulo.tserver.tablet;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.StoredTabletFile;

import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class DatafileTransactionLog {
    private final KeyExtent extent;
    private Set<StoredTabletFile> initialFiles = new HashSet<>();
    private long initialTs = System.currentTimeMillis();
    private List<DatafileTransaction> tabletLogs = Collections.synchronizedList(new LinkedList<>());
    private final int maxSize;

    public DatafileTransactionLog(KeyExtent extent, Set<StoredTabletFile> initialFiles, int maxSize) {
        this.extent = extent;
        this.maxSize = maxSize;
        this.initialFiles.addAll(initialFiles);
    }

    public Date getInitialDate() {
        return Date.from(Instant.ofEpochSecond(initialTs));
    }

    private void checkSize() {
        flush(maxSize);
    }

    public void flush(int size) {
        while (tabletLogs.size() > size) {
            applyTransaction();
        }
    }

    private void applyTransaction() {
        // synchronize to keep both the remove and apply atomic
        synchronized(tabletLogs) {
            tabletLogs.remove(0).apply(initialFiles);
        }
    }

    public Set<StoredTabletFile> getExpectedFiles() {
        Set<StoredTabletFile> files = new HashSet<>();
        // synchronize to ensure consistency between initialFiles and the logs
        synchronized(tabletLogs) {
            files.addAll(initialFiles);
            tabletLogs.stream().forEach(t -> t.apply(files));
        }
        return files;
    }

    public void compacted(Set<StoredTabletFile> files, Optional<StoredTabletFile> output) {
        tabletLogs.add(new DatafileTransaction.Compacted(files, output));
        checkSize();
    }

    public void flushed(Optional<StoredTabletFile> newDatafile) {
        tabletLogs.add(new DatafileTransaction.Flushed(newDatafile));
        checkSize();
    }

    public void bulkImported(StoredTabletFile file) {
        tabletLogs.add(new DatafileTransaction.BulkImported(file));
        checkSize();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        synchronized(tabletLogs) {
            builder.append(String.format("%s: Initial files : %s\n", getInitialDate(), initialFiles));
            tabletLogs.stream().forEach(t -> builder.append(t).append('\n'));
            builder.append("Final files: ").append(getExpectedFiles());
        }
        return builder.toString();
    }

}
