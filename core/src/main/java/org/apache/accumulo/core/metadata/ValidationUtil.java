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
package org.apache.accumulo.core.metadata;

import java.util.Objects;

import org.apache.accumulo.core.gc.ReferenceFile;
import org.apache.hadoop.fs.Path;

/**
 * Utility class for validation of tablet file paths.
 */
public class ValidationUtil {

  /**
   * Validate if string is a valid path. Return normalized string or throw exception if not valid.
   * This was added to facilitate more use of TabletFile over String but this puts the validation in
   * one location in the case where TabletFile can't be used. The Garbage Collector is optimized to
   * store a directory for Tablet File so a String is used.
   */
  public static String validate(String path) {
    Path p = new Path(path);
    return validate(p).toString();
  }

  public static ReferenceFile validate(ReferenceFile reference) {
    validate(new Path(reference.getMetadataEntry()));
    return reference;
  }

  public static Path validate(Path path) {
    if (path.toUri().getScheme() == null) {
      throw new IllegalArgumentException("Invalid path provided, no scheme in " + path);
    }
    return path;
  }

  public static void validateRFileName(String fileName) {
    Objects.requireNonNull(fileName);
    if (!fileName.endsWith(".rf") && !fileName.endsWith("_tmp")) {
      throw new IllegalArgumentException(
          "Provided filename (" + fileName + ") does not end with '.rf' or '_tmp'");
    }
  }

  public static void validateFileName(String fileName) {
    Objects.requireNonNull(fileName);
    if (!fileName.matches("[\\dA-Za-z._-]+")) {
      throw new IllegalArgumentException(
          "Provided filename (" + fileName + ") contains invalid characters.");
    }
  }
}
