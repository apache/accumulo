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

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.AbstractCollection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Consumer;

import org.apache.accumulo.core.gc.GcCandidate;
import org.apache.accumulo.core.gc.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

/**
 * Code that supports efficient removal of references from a set of gc candidates.
 */
public class GcCandidates extends AbstractCollection<GcCandidate> {

  private static final Logger log = LoggerFactory.getLogger(GcCandidates.class);

  private final SortedMap<String,GcCandidate> fileCandidates = new TreeMap<>();
  private final HashMap<String,GcCandidate> tabletDirCandidates = new HashMap<>();
  private final HashMap<String,GcCandidate> bulkDirCandidates = new HashMap<>();
  private final HashMap<String,GcCandidate> tableDirCandidates = new HashMap<>();

  private record LastThree(String maybeATableId, String dir, String name) {
  }

  private LastThree lastThree(String strPath, int minPathLength) {
    return lastThree(strPath, true, minPathLength, Integer.MAX_VALUE);
  }

  private LastThree lastThree(String strPath, boolean expectScheme, int minPathLength,
      int maxPathLength) {
    URI uri = null;
    try {
      uri = new URI(strPath);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }

    if (expectScheme && uri.getScheme() == null) {
      throw new IllegalArgumentException("Missing scheme in URI " + strPath);
    }

    // TODO what about other parts of the URI?
    var path = Path.of(uri.getPath());

    int count = path.getNameCount();
    if (count < minPathLength) {
      throw new IllegalArgumentException("Path too short " + strPath);
    }

    if (count > maxPathLength) {
      throw new IllegalArgumentException("Path too long " + strPath);
    }

    if (count == 1) {
      return new LastThree(null, null, path.getName(count - 1).toString());
    } else if (count == 2) {
      return new LastThree(null, path.getName(count - 2).toString(),
          path.getName(count - 1).toString());
    } else if (count >= 3) {
      return new LastThree(path.getName(count - 3).toString(), path.getName(count - 2).toString(),
          path.getName(count - 1).toString());
    } else {
      throw new IllegalArgumentException("Bad path " + path);
    }
  }

  public GcCandidates(Iterable<GcCandidate> candidates) {
    candidates.forEach(candidate -> {
      try {
        switch (candidate.getType()) {
          case TABLET_FILE -> {
            var lastThree = lastThree(candidate.getPath(), 2);
            fileCandidates.put("/" + lastThree.dir + "/" + lastThree.name, candidate);
          }
          case TABLET_DIR -> {
            var lastThree = lastThree(candidate.getPath(), false, 2, 2);
            tabletDirCandidates.put("/" + lastThree.dir + "/" + lastThree.name, candidate);
          }
          case BULK_DIR -> {
            var lastThree = lastThree(candidate.getPath(), 1);
            bulkDirCandidates.put(lastThree.name, candidate);
          }
          case TABLE_DIR -> {
            var lastThree = lastThree(candidate.getPath(), false, 1, 1);
            tableDirCandidates.put(lastThree.name, candidate);
          }
        }
      } catch (IllegalArgumentException e) {
        log.warn("Ignoring invalid deletion candidate {}", candidate);
      }
    });
  }

  @Override
  public Iterator<GcCandidate> iterator() {
    return Iterators.unmodifiableIterator(Iterators.concat(fileCandidates.values().iterator(),
        tabletDirCandidates.values().iterator(), bulkDirCandidates.values().iterator(),
        tableDirCandidates.values().iterator()));
  }

  public int removeBlip(String blip) {
    var lastThree = lastThree(blip, false, 2, 2);

    String blipPrefix = "/" + lastThree.name;

    bulkDirCandidates.remove(lastThree.name);

    var tailIter = fileCandidates.tailMap(blipPrefix).keySet().iterator();
    int removeCount = 0;
    while (tailIter.hasNext()) {
      if (tailIter.next().startsWith(blipPrefix)) {
        tailIter.remove();
        removeCount++;
      } else {
        break;
      }
    }

    return removeCount;
  }

  public void remove(Reference reference, Consumer<GcCandidate> removedConsumer) {

    Consumer<GcCandidate> nonNullConsumer = gcc -> {
      if (gcc != null) {
        removedConsumer.accept(gcc);
      }
    };

    if (reference.isDirectory()) {
      var lastThree = lastThree(reference.getMetadataPath(), false, 1, 1);
      nonNullConsumer
          .accept(tabletDirCandidates.remove("/" + reference.getTableId() + "/" + lastThree.name));
      nonNullConsumer.accept(tableDirCandidates.remove(reference.getTableId().toString()));
    } else {
      var lastThree = lastThree(reference.getMetadataPath(), 2);
      if (lastThree.maybeATableId != null) {
        nonNullConsumer.accept(
            tabletDirCandidates.remove("/" + lastThree.maybeATableId + "/" + lastThree.dir));
        nonNullConsumer.accept(tableDirCandidates.remove(lastThree.maybeATableId));
      }
      nonNullConsumer.accept(bulkDirCandidates.remove(lastThree.dir));
      nonNullConsumer.accept(fileCandidates.remove("/" + lastThree.dir + "/" + lastThree.name));
    }
  }

  @Override
  public int size() {
    return tabletDirCandidates.size() + fileCandidates.size() + bulkDirCandidates.size()
        + tableDirCandidates.size();
  }
}
