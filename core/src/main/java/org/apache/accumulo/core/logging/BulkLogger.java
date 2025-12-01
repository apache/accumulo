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
package org.apache.accumulo.core.logging;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.FateId;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkLogger {
  private static final Logger log = LoggerFactory.getLogger(Logging.PREFIX + "bulk");

  public static void initiating(FateId fateId, TableId tableId, boolean setTime, String sourceDir,
      String destinationDir) {
    // Log the key pieces of information about a bulk import in a single log message to tie them all
    // together.
    log.info("{} initiating bulk import, tableId:{} setTime:{} source:{} destination:{}", fateId,
        tableId, setTime, sourceDir, destinationDir);
  }

  public static void renamed(FateId fateId, Path source, Path destination) {
    // The initiating message logged the full directory paths, so do not need to repeat that
    // information here. Log the bulk destination directory as it is unique and easy to search for.
    log.debug("{} renamed {} to {}/{}", fateId, source.getName(), destination.getParent().getName(),
        destination.getName());
  }
}
