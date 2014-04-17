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
package org.apache.accumulo.core.replication;

import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.schema.Section;
import org.apache.hadoop.io.Text;

/**
 * 
 */
public class ReplicationSchema {

  public static class StatusSection {
    public static final Text NAME = new Text("repl");
  }

  public static class WorkSection {
  }

  /**
   * Holds replication markers tracking status for files
   */
  public static class ReplicationSection {
    private static final Section section = new Section(MetadataSchema.RESERVED_PREFIX + "repl", true, MetadataSchema.RESERVED_PREFIX + "repm", false);

    public static Range getRange() {
      return section.getRange();
    }

    public static String getRowPrefix() {
      return section.getRowPrefix();
    }

  }
}
