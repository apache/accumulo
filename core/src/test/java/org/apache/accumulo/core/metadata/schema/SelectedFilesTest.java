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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
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

    SelectedFiles original = new SelectedFiles(files, true, 12345L);

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

    SelectedFiles sf1 = new SelectedFiles(files, true, 12345L);
    SelectedFiles sf2 = new SelectedFiles(files, true, 12345L);

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

    assertEquals(files, sortedFiles, "Entries in test file sets should be the same");
    assertNotEquals(files.toString(), sortedFiles.toString(),
        "Order of files set should differ for this test case");

    SelectedFiles sf1 = new SelectedFiles(files, false, 654123L);
    SelectedFiles sf2 = new SelectedFiles(sortedFiles, false, 654123L);

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
    // Remove an element to create a subset
    filesSubSet.remove(filesSubSet.iterator().next());

    SelectedFiles superSetSelectedFiles = new SelectedFiles(filesSuperSet, true, 123456L);
    SelectedFiles subSetSelectedFiles = new SelectedFiles(filesSubSet, true, 123456L);

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
    return Stream.of(Arguments.of("123456", true, 12), Arguments.of("123456", false, 12),
        Arguments.of("123456", false, 23), Arguments.of("654321", false, 23),
        Arguments.of("AE56E", false, 23));
  }

  /**
   * Ensure that SelectedFiles objects that are created using {@link SelectedFiles#from(String)} are
   * created correctly
   */
  @ParameterizedTest
  @MethodSource("provideTestJsons")
  public void testJsonStrings(String txid, boolean selAll, int numPaths) {
    List<String> paths = getFilePaths(numPaths);

    // should be resilient to unordered file arrays
    Collections.shuffle(paths, RANDOM.get());

    // construct a json from the given parameters
    String json = getJson(txid, selAll, paths);

    System.out.println(json);

    // create a SelectedFiles object using the json
    SelectedFiles selectedFiles = SelectedFiles.from(json);

    // ensure all parts of the SelectedFiles object are correct
    assertEquals(Long.parseLong(txid, 16), selectedFiles.getFateTxId());
    assertEquals(selAll, selectedFiles.initiallySelectedAll());
    Set<StoredTabletFile> expectedStoredTabletFiles = filePathsToStoredTabletFiles(paths);
    assertEquals(expectedStoredTabletFiles, selectedFiles.getFiles());

    Collections.sort(paths);
    String jsonWithSortedFiles = getJson(txid, selAll, paths);
    assertEquals(jsonWithSortedFiles, selectedFiles.getMetadataValue());
  }

  /**
   * Creates a json suitable to create a SelectedFiles object from. The given parameters will be
   * inserted into the returned json String. Example of returned json String:
   *
   * <pre>
   * {
   *   "txid": "FATE[123456]",
   *   "selAll": true,
   *   "files": ["/path/to/file1.rf", "/path/to/file2.rf"]
   * }
   * </pre>
   */
  private static String getJson(String txid, boolean selAll, List<String> paths) {
    String filesJsonArray =
        paths.stream().map(path -> new ReferencedTabletFile(new Path(path)).insert().getMetadata())
            .map(path -> path.replace("\"", "\\\"")).map(path -> "'" + path + "'")
            .collect(Collectors.joining(","));
    return ("{'txid':'" + FateTxId.formatTid(Long.parseLong(txid, 16)) + "','selAll':" + selAll
        + ",'files':[" + filesJsonArray + "]}").replace('\'', '\"');
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
