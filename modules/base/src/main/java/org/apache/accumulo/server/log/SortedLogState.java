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
package org.apache.accumulo.server.log;

import org.apache.hadoop.fs.Path;

/**
 * A file is written in the destination directory for the sorting of write-ahead logs that need recovering. The value of {@link #getMarker()} is the name of the
 * file that will exist in the sorted output directory.
 */
public enum SortedLogState {
  FINISHED("finished"), FAILED("failed");

  private String marker;

  private SortedLogState(String marker) {
    this.marker = marker;
  }

  public String getMarker() {
    return marker;
  }

  public static boolean isFinished(String fileName) {
    return FINISHED.getMarker().equals(fileName);
  }

  public static Path getFinishedMarkerPath(String rootPath) {
    return new Path(rootPath, FINISHED.getMarker());
  }

  public static Path getFinishedMarkerPath(Path rootPath) {
    return new Path(rootPath, FINISHED.getMarker());
  }

  public static Path getFailedMarkerPath(String rootPath) {
    return new Path(rootPath, FAILED.getMarker());
  }

  @Override
  public String toString() {
    return marker;
  }
}
