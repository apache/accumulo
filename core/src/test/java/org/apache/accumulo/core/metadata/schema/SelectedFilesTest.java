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
package org.apache.accumulo.core.metadata.schema;

import static org.apache.accumulo.core.util.LazySingletons.RANDOM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.util.time.SteadyTime;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class SelectedFilesTest {

  final String filePathPrefix = "hdfs://namenode:9020/accumulo/tables/1/default_tablet/";

  /**
   * Make sure a SelectedFiles object is equal to another SelectedFiles object that was created
   * using its metadata json
   */
  @Test
  public void testSerializationDeserialization() {
    Set<StoredTabletFile> files = getStoredTabletFiles(2);
    FateId fateId = FateId.from(FateInstanceType.META, UUID.randomUUID());

    SelectedFiles original =
        new SelectedFiles(files, true, fateId, SteadyTime.from(100_100, TimeUnit.NANOSECONDS));

    String json = original.getMetadataValue();
    SelectedFiles deserialized = SelectedFiles.from(json);

    assertEquals(original, deserialized);
    assertEquals(json, deserialized.getMetadataValue());
  }

  /**
   * Ensure two SelectedFiles objects created with the same parameters are equal
   */
  @Test
  public void testEqualSerialization() {
    Set<StoredTabletFile> files = getStoredTabletFiles(16);
    FateId fateId = FateId.from(FateInstanceType.META, UUID.randomUUID());

    SelectedFiles sf1 =
        new SelectedFiles(files, true, fateId, SteadyTime.from(100_100, TimeUnit.NANOSECONDS));
    SelectedFiles sf2 =
        new SelectedFiles(files, true, fateId, SteadyTime.from(100_100, TimeUnit.NANOSECONDS));

    assertEquals(sf1.getMetadataValue(), sf2.getMetadataValue());
    assertEquals(sf1, sf2);
  }

  /**
   * Ensure that SelectedFiles objects created with file arrays of differing order yet equal entries
   * are correctly serialized
   */
  @Test
  public void testDifferentFilesOrdering() {
    Set<StoredTabletFile> files = getStoredTabletFiles(16);
    SortedSet<StoredTabletFile> sortedFiles = new TreeSet<>(files);
    FateId fateId = FateId.from(FateInstanceType.META, UUID.randomUUID());

    assertEquals(files, sortedFiles, "Entries in test file sets should be the same");
    assertNotEquals(files.toString(), sortedFiles.toString(),
        "Order of files set should differ for this test case");

    SelectedFiles sf1 =
        new SelectedFiles(files, false, fateId, SteadyTime.from(100_100, TimeUnit.NANOSECONDS));
    SelectedFiles sf2 = new SelectedFiles(sortedFiles, false, fateId,
        SteadyTime.from(100_100, TimeUnit.NANOSECONDS));

    assertEquals(sf1.getMetadataValue(), sf2.getMetadataValue());
    assertEquals(sf1, sf2);
  }

  /**
   * Ensure that two SelectedFiles objects, one created with a subset of files present in the other,
   * are not equal
   */
  @Test
  public void testJsonSuperSetSubset() {
    Set<StoredTabletFile> filesSuperSet = getStoredTabletFiles(3);
    Set<StoredTabletFile> filesSubSet = new HashSet<>(filesSuperSet);
    FateId fateId = FateId.from(FateInstanceType.META, UUID.randomUUID());
    // Remove an element to create a subset
    filesSubSet.remove(filesSubSet.iterator().next());

    SelectedFiles superSetSelectedFiles = new SelectedFiles(filesSuperSet, true, fateId,
        SteadyTime.from(100_100, TimeUnit.NANOSECONDS));
    SelectedFiles subSetSelectedFiles = new SelectedFiles(filesSubSet, true, fateId,
        SteadyTime.from(100_100, TimeUnit.NANOSECONDS));

    String superSetJson = superSetSelectedFiles.getMetadataValue();
    String subSetJson = subSetSelectedFiles.getMetadataValue();

    assertNotEquals(superSetJson, subSetJson, "The metadata jsons should have differing file sets");

    // Deserialize the JSON strings back to SelectedFiles and check for equality with the original
    // objects
    SelectedFiles superSetDeserialized = SelectedFiles.from(superSetJson);
    SelectedFiles subSetDeserialized = SelectedFiles.from(subSetJson);

    assertEquals(superSetSelectedFiles, superSetDeserialized);
    assertEquals(subSetSelectedFiles, subSetDeserialized);

    assertNotEquals(superSetDeserialized, subSetDeserialized,
        "The objects should have differing file sets");
  }

  private static Stream<Arguments> provideTestJsons() {
    return Stream.of(
        Arguments.of("FATE:META:12345678-9abc-def1-2345-6789abcdef12", true, 0, 1000, 12),
        Arguments.of("FATE:META:12345678-9abc-def1-2345-6789abcdef12", false, 1, 2000, 12),
        Arguments.of("FATE:META:12345678-9abc-def1-2345-6789abcdef12", false, 2, 3000, 23),
        Arguments.of("FATE:META:abcdef12-3456-789a-bcde-f123456789ab", false, 2, 4000, 23),
        Arguments.of("FATE:META:41b40c7c-55e5-4d3b-8d21-1b70d1e7f3fb", false, 2, 5000, 23));
  }

  /**
   * Ensure that SelectedFiles objects that are created using {@link SelectedFiles#from(String)} are
   * created correctly
   */
  @ParameterizedTest
  @MethodSource("provideTestJsons")
  public void testJsonStrings(FateId fateId, boolean selAll, int compJobs, long selTimeNanos,
      int numPaths) {
    List<String> paths = getFilePaths(numPaths);

    // should be resilient to unordered file arrays
    Collections.shuffle(paths, RANDOM.get());

    // construct a json from the given parameters
    String json = getJson(fateId, selAll, compJobs, selTimeNanos, paths);

    System.out.println(json);

    // create a SelectedFiles object using the json
    SelectedFiles selectedFiles = SelectedFiles.from(json);

    // ensure all parts of the SelectedFiles object are correct
    assertEquals(fateId, selectedFiles.getFateId());
    assertEquals(selAll, selectedFiles.initiallySelectedAll());
    Set<StoredTabletFile> expectedStoredTabletFiles = filePathsToStoredTabletFiles(paths);
    assertEquals(expectedStoredTabletFiles, selectedFiles.getFiles());

    Collections.sort(paths);
    String jsonWithSortedFiles = getJson(fateId, selAll, compJobs, selTimeNanos, paths);
    assertEquals(jsonWithSortedFiles, selectedFiles.getMetadataValue());
  }

  /**
   * Creates a json suitable to create a SelectedFiles object from. The given parameters will be
   * inserted into the returned json String. Example of returned json String:
   *
   * <pre>
   * {
   *   "fateId": "FATE:META:12345678-9abc-def1-2345-6789abcdef12",
   *   "selAll": true,
   *   "files": ["/path/to/file1.rf", "/path/to/file2.rf"]
   * }
   * </pre>
   */
  private static String getJson(FateId fateId, boolean selAll, int compJobs, long selTimeNanos,
      List<String> paths) {
    String filesJsonArray =
        paths.stream().map(path -> new ReferencedTabletFile(new Path(path)).insert().getMetadata())
            .map(path -> path.replace("\"", "\\\"")).map(path -> "'" + path + "'")
            .collect(Collectors.joining(","));
    return ("{'fateId':'" + fateId + "','selAll':" + selAll + ",'compJobs':" + compJobs
        + ",'selTimeNanos':" + selTimeNanos + ",'files':[" + filesJsonArray + "]}")
        .replace('\'', '\"');
  }

  /**
   * Generate the given number of rfile path strings
   */
  private List<String> getFilePaths(int numOfFiles) {
    List<String> paths = new ArrayList<>(numOfFiles);
    for (int i = 0; i < numOfFiles; i++) {
      paths.add(filePathPrefix + "F0000" + i + ".rf");
    }
    return paths;
  }

  /**
   * Convert a list of file paths to StoredTabletFile objects
   */
  private Set<StoredTabletFile> filePathsToStoredTabletFiles(List<String> filePaths) {
    return filePaths.stream().map(Path::new).map(ReferencedTabletFile::new)
        .map(ReferencedTabletFile::insert).collect(Collectors.toSet());
  }

  /**
   * Generate the given number of StoredTabletFile objects
   */
  private Set<StoredTabletFile> getStoredTabletFiles(int numOfFiles) {
    return filePathsToStoredTabletFiles(getFilePaths(numOfFiles));
  }

}
