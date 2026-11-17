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

import java.util.AbstractCollection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Consumer;

import org.apache.accumulo.core.gc.GcCandidate;
import org.apache.accumulo.core.gc.Reference;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Code that supports efficient removal of references from a set of gc candidates.
 */
public class GcCandidates extends AbstractCollection<GcCandidate> {

  private static final Logger log = LoggerFactory.getLogger(GcCandidates.class);

  // Absolute file paths must retain their complete URI identity. Relative paths are legacy
  // candidates and are tracked separately so they can be conservatively matched to managed files.
  private final SortedMap<String,GcCandidate> absoluteFileCandidates = new TreeMap<>();
  private final SortedMap<String,GcCandidate> relativeFileCandidates = new TreeMap<>();
  private final SortedMap<String,Set<String>> absoluteFilesByManagedPath = new TreeMap<>();
  private final Map<String,GcCandidate> tabletDirCandidates = new HashMap<>();
  private final SortedMap<String,GcCandidate> bulkDirCandidates = new TreeMap<>();
  private final SortedMap<String,Set<String>> bulkDirsByManagedPath = new TreeMap<>();
  private final Map<String,GcCandidate> tableDirCandidates = new HashMap<>();

  public GcCandidates(Iterable<GcCandidate> candidates) {
    for (GcCandidate candidate : candidates) {
      try {
        switch (candidate.getType()) {
          case TABLET_FILE -> addFileCandidate(candidate);
          case TABLET_DIR ->
            tabletDirCandidates.put(relativePath(candidate.getPath(), 2), candidate);
          case BULK_DIR -> addBulkDirCandidate(candidate);
          case TABLE_DIR -> tableDirCandidates.put(relativePath(candidate.getPath(), 1), candidate);
        }
      } catch (IllegalArgumentException e) {
        log.warn("Ignoring invalid deletion candidate {}", candidate);
      }
    }
  }

  private void addFileCandidate(GcCandidate candidate) {
    Path path = new Path(candidate.getPath());
    if (path.toUri().getScheme() != null) {
      String normalized = path.toString();
      absoluteFileCandidates.put(normalized, candidate);
      String managedPath = managedPath(path, 3);
      if (managedPath != null) {
        addIndex(absoluteFilesByManagedPath, managedPath, normalized);
      }
    } else {
      String normalized = relativePath(candidate.getPath(), 3);
      relativeFileCandidates.put(normalized, candidate);
    }
  }

  private void addBulkDirCandidate(GcCandidate candidate) {
    Path path = new Path(candidate.getPath());
    if (path.toUri().getScheme() == null) {
      throw new IllegalArgumentException("Bulk directory candidate must be an absolute URI");
    }
    String normalized = path.toString();
    bulkDirCandidates.put(normalized, candidate);
    String managedPath = managedPath(path, 2);
    if (managedPath != null) {
      addIndex(bulkDirsByManagedPath, managedPath, normalized);
    }
  }

  private static void addIndex(SortedMap<String,Set<String>> index, String key, String path) {
    index.computeIfAbsent(key, ignored -> new HashSet<>()).add(path);
  }

  private static String relativePath(String path, int expectedComponents) {
    String candidatePath = path.startsWith("../") ? path.substring(3) : path;
    Path parsed = new Path(candidatePath);
    if (parsed.toUri().getScheme() != null) {
      throw new IllegalArgumentException("Expected a relative candidate path: " + path);
    }
    String normalized = parsed.toString();
    while (normalized.startsWith("/")) {
      normalized = normalized.substring(1);
    }
    if (normalized.isEmpty()) {
      throw new IllegalArgumentException("Empty candidate path");
    }
    if (componentCount(normalized) != expectedComponents) {
      throw new IllegalArgumentException("Unexpected candidate path depth: " + path);
    }
    return "/" + normalized;
  }

  private static int componentCount(String path) {
    int count = 0;
    boolean inComponent = false;
    for (int i = 0; i < path.length(); i++) {
      char c = path.charAt(i);
      if (c == '/') {
        inComponent = false;
      } else if (!inComponent) {
        count++;
        inComponent = true;
      }
    }
    return count;
  }

  /**
   * Extracts the table-relative tablet file or directory path if the URI follows the normal
   * {@code .../tables/<tableId>/<tabletDir>/...} layout.
   */
  private static String managedPath(Path path, int componentsAfterTables) {
    Path current = path;
    String[] suffix = new String[componentsAfterTables];
    for (int i = componentsAfterTables - 1; i >= 0; i--) {
      if (current == null || current.getName().isEmpty()) {
        return null;
      }
      suffix[i] = current.getName();
      current = current.getParent();
    }
    if (current == null || !"tables".equals(current.getName())) {
      return null;
    }
    StringBuilder result = new StringBuilder();
    for (String component : suffix) {
      result.append('/').append(component);
    }
    return result.toString();
  }

  @Override
  public Iterator<GcCandidate> iterator() {
    return new Iterator<>() {
      private final Iterator<GcCandidate> files = concat(absoluteFileCandidates.values().iterator(),
          relativeFileCandidates.values().iterator());
      private final Iterator<GcCandidate> tabletDirs = tabletDirCandidates.values().iterator();
      private final Iterator<GcCandidate> bulkDirs = bulkDirCandidates.values().iterator();
      private final Iterator<GcCandidate> tableDirs = tableDirCandidates.values().iterator();
      private Iterator<GcCandidate> current = files;

      private boolean advance() {
        while (!current.hasNext()) {
          if (current == files) {
            current = tabletDirs;
          } else if (current == tabletDirs) {
            current = bulkDirs;
          } else if (current == bulkDirs) {
            current = tableDirs;
          } else {
            return false;
          }
        }
        return true;
      }

      @Override
      public boolean hasNext() {
        return advance();
      }

      @Override
      public GcCandidate next() {
        if (!advance()) {
          throw new java.util.NoSuchElementException();
        }
        return current.next();
      }
    };
  }

  private static Iterator<GcCandidate> concat(Iterator<GcCandidate> first,
      Iterator<GcCandidate> second) {
    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        return first.hasNext() || second.hasNext();
      }

      @Override
      public GcCandidate next() {
        return first.hasNext() ? first.next() : second.next();
      }
    };
  }

  public int removeBlip(String blip) {
    try {
      Path blipPath = new Path(blip);
      String normalized = blipPath.toString();
      int removed = 0;

      if (blipPath.toUri().getScheme() == null) {
        String managed = relativePath(blip, 2);
        String relativePrefix = withTrailingSlash(managed);
        removed += removeSortedPrefix(relativeFileCandidates, relativePrefix, null);
        removed += removeManagedPrefix(absoluteFilesByManagedPath, relativeFileCandidates,
            absoluteFileCandidates, relativePrefix);
        removeManagedBulkDirs(managed);
        tabletDirCandidates.remove(managed);
      } else {
        String managed = managedPath(blipPath, 2);
        removed += removePathAndDescendants(absoluteFileCandidates, normalized);
        if (managed != null) {
          removed += removeSortedPrefix(relativeFileCandidates, withTrailingSlash(managed), null);
          tabletDirCandidates.remove(managed);
        }
        removePathAndDescendants(bulkDirCandidates, normalized);
      }
      return removed;
    } catch (IllegalArgumentException e) {
      log.warn("Ignoring invalid bulk processing flag path {}", blip, e);
      return 0;
    }
  }

  private int removeManagedPrefix(SortedMap<String,Set<String>> index,
      SortedMap<String,GcCandidate> relativeCandidates,
      SortedMap<String,GcCandidate> absoluteCandidates, String prefix) {
    int removed = removeSortedPrefix(relativeCandidates, prefix, null);
    Iterator<Map.Entry<String,Set<String>>> entries = index.tailMap(prefix).entrySet().iterator();
    while (entries.hasNext()) {
      Map.Entry<String,Set<String>> entry = entries.next();
      if (!entry.getKey().startsWith(prefix)) {
        break;
      }
      for (String absolutePath : entry.getValue()) {
        if (absoluteCandidates.remove(absolutePath) != null) {
          removed++;
        }
      }
      entries.remove();
    }
    return removed;
  }

  private void removeManagedBulkDirs(String managedPrefix) {
    Set<String> exact = bulkDirsByManagedPath.remove(managedPrefix);
    if (exact != null) {
      for (String absolutePath : exact) {
        bulkDirCandidates.remove(absolutePath);
      }
    }
    String prefix = withTrailingSlash(managedPrefix);
    Iterator<Map.Entry<String,Set<String>>> entries =
        bulkDirsByManagedPath.tailMap(prefix).entrySet().iterator();
    while (entries.hasNext()) {
      Map.Entry<String,Set<String>> entry = entries.next();
      if (!entry.getKey().startsWith(prefix)) {
        break;
      }
      for (String absolutePath : entry.getValue()) {
        bulkDirCandidates.remove(absolutePath);
      }
      entries.remove();
    }
  }

  private static String withTrailingSlash(String path) {
    return path.endsWith("/") ? path : path + "/";
  }

  private static int removePathAndDescendants(SortedMap<String,GcCandidate> candidates,
      String path) {
    int removed = 0;
    if (candidates.remove(path) != null) {
      removed++;
    }
    removed += removeSortedPrefix(candidates, withTrailingSlash(path), null);
    return removed;
  }

  private static int removeSortedPrefix(SortedMap<String,GcCandidate> candidates, String prefix,
      Consumer<GcCandidate> removedConsumer) {
    Iterator<Map.Entry<String,GcCandidate>> entries =
        candidates.tailMap(prefix).entrySet().iterator();
    int removed = 0;
    while (entries.hasNext()) {
      Map.Entry<String,GcCandidate> entry = entries.next();
      if (!entry.getKey().startsWith(prefix)) {
        break;
      }
      if (removedConsumer != null) {
        removedConsumer.accept(entry.getValue());
      }
      entries.remove();
      removed++;
    }
    return removed;
  }

  public void remove(Reference reference, Consumer<GcCandidate> removedConsumer) {
    Consumer<GcCandidate> nonNullConsumer = candidate -> {
      if (candidate != null) {
        removedConsumer.accept(candidate);
      }
    };

    if (reference.isDirectory()) {
      Path dir = new Path(reference.getMetadataPath());
      String dirName = dir.getName();
      String tableId = reference.getTableId().canonical();
      nonNullConsumer.accept(tabletDirCandidates.remove("/" + tableId + "/" + dirName));
      nonNullConsumer.accept(tableDirCandidates.remove("/" + tableId));
      return;
    }

    Path referencePath = new Path(reference.getMetadataPath());
    if (!referencePath.isAbsolute() || referencePath.toUri().getScheme() == null) {
      throw new IllegalArgumentException(
          "File reference must be an absolute URI with a scheme: " + reference.getMetadataPath());
    }

    String normalized = referencePath.toString();
    nonNullConsumer.accept(absoluteFileCandidates.remove(normalized));

    String managed = managedPath(referencePath, 3);
    if (managed != null) {
      nonNullConsumer.accept(relativeFileCandidates.remove(managed));
      nonNullConsumer.accept(tabletDirCandidates.remove(parentPath(managed)));
      nonNullConsumer.accept(tableDirCandidates.remove(tablePath(managed)));
    }

    Path parent = referencePath.getParent();
    while (parent != null) {
      nonNullConsumer.accept(bulkDirCandidates.remove(parent.toString()));
      parent = parent.getParent();
    }
  }

  private static String parentPath(String path) {
    int slash = path.lastIndexOf('/');
    return slash <= 0 ? "" : path.substring(0, slash);
  }

  private static String tablePath(String path) {
    int slash = path.indexOf('/', 1);
    return slash < 0 ? path : path.substring(0, slash);
  }

  @Override
  public int size() {
    return absoluteFileCandidates.size() + relativeFileCandidates.size()
        + tabletDirCandidates.size() + bulkDirCandidates.size() + tableDirCandidates.size();
  }
}
