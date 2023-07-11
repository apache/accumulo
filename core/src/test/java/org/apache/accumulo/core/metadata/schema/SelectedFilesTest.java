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

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class SelectedFilesTest {

  final String filePathPrefix = "hdfs://namenode:9020/accumulo/tables/1/default_tablet/";

  @Test
  public void testSerializationDeserialization() {
    Set<StoredTabletFile> files = getStoredTabletFiles(2);

    SelectedFiles original = new SelectedFiles(files, true, 12345L);

    String json = original.getMetadataValue();
    SelectedFiles deserialized = SelectedFiles.from(json);

    assertEquals(original.getFiles(), deserialized.getFiles(),
        "Set of files differ between original and deserialized objects");
    assertEquals(original.initiallySelectedAll(), deserialized.initiallySelectedAll(),
        "Initially selected all boolean values differ between original and deserialized objects");
    assertEquals(original.getFateTxId(), deserialized.getFateTxId(),
        "Transaction id differ between original and deserialized objects");
  }

  @Test
  public void testEqualSerialization() {
    Set<StoredTabletFile> files = getStoredTabletFiles(16);

    SelectedFiles sf1 = new SelectedFiles(files, true, 12345L);
    SelectedFiles sf2 = new SelectedFiles(files, true, 12345L);

    assertEquals(sf1.getMetadataValue(), sf2.getMetadataValue());
  }

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
    return Stream.of(Arguments.of("123456", true, 2), Arguments.of("123456", false, 2),
        Arguments.of("123456", true, 3), Arguments.of("ABCDEF", false, 3));
  }

  @ParameterizedTest
  @MethodSource("provideTestJsons")
  public void testJsonStrings(String txid, boolean selAll, int numPaths) {
    List<String> paths = getFilePaths(numPaths);

    // construct a json from the given parameters
    String filesJsonArray =
        paths.stream().map(path -> "'" + path + "'").collect(Collectors.joining(","));
    String json =
        ("{'txid':'FATE[" + txid + "]','selAll':" + selAll + ",'files':[" + filesJsonArray + "]}")
            .replace('\'', '\"');

    // create a SelectedFiles object using the json
    SelectedFiles selectedFiles = SelectedFiles.from(json);

    // ensure all parts of the SelectedFiles object are correct
    assertEquals(Long.parseLong(txid, 16), selectedFiles.getFateTxId());
    assertEquals(selAll, selectedFiles.initiallySelectedAll());
    Set<StoredTabletFile> expectedStoredTabletFiles = filePathsToStoredTabletFiles(paths);
    assertEquals(expectedStoredTabletFiles, selectedFiles.getFiles());
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
    return filePaths.stream().map(StoredTabletFile::new).collect(Collectors.toSet());
  }

  /**
   * Generate the given number of StoredTabletFile objects
   */
  private Set<StoredTabletFile> getStoredTabletFiles(int numOfFiles) {
    return filePathsToStoredTabletFiles(getFilePaths(numOfFiles));
  }

}
