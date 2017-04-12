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
package org.apache.accumulo.server.fs;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ViewFSUtilsTest {

  private String[] shuffle(String... inputs) {
    // code below will modify array
    Collections.shuffle(Arrays.asList(inputs));
    return inputs;
  }

  @Test
  public void testDisjointMountPoints() throws IllegalArgumentException, IOException {
    if (ViewFSUtils.isViewFSSupported()) {
      Configuration conf = new Configuration(false);
      conf.set("fs.viewfs.mounttable.default.link./ns", "file:///tmp/ns");
      conf.set("fs.viewfs.mounttable.default.link./ns1", "file:///tmp/ns1");
      conf.set("fs.viewfs.mounttable.default.link./ns2", "file:///tmp/ns2");
      conf.set("fs.viewfs.mounttable.default.link./ns22", "file:///tmp/ns22");

      String[] tablesDirs1 = shuffle("viewfs:///ns1/accumulo/tables", "viewfs:///ns2/accumulo/tables", "viewfs:///ns22/accumulo/tables",
          "viewfs:///ns/accumulo/tables");
      String[] tablesDirs2 = shuffle("viewfs:/ns1/accumulo/tables", "viewfs:/ns2/accumulo/tables", "viewfs:/ns22/accumulo/tables", "viewfs:/ns/accumulo/tables");

      for (String ns : Arrays.asList("ns1", "ns2", "ns22", "ns")) {
        Path match = ViewFSUtils.matchingFileSystem(new Path("viewfs:/" + ns + "/bulk_import_01"), tablesDirs2, conf);
        Assert.assertEquals(new Path("viewfs:/" + ns + "/accumulo/tables"), match);

        match = ViewFSUtils.matchingFileSystem(new Path("viewfs:///" + ns + "/bulk_import_01"), tablesDirs1, conf);
        Assert.assertEquals(new Path("viewfs:/" + ns + "/accumulo/tables"), match);

        match = ViewFSUtils.matchingFileSystem(new Path("viewfs:/" + ns + "/bulk_import_01"), tablesDirs2, conf);
        Assert.assertEquals(new Path("viewfs:/" + ns + "/accumulo/tables"), match);

        match = ViewFSUtils.matchingFileSystem(new Path("viewfs:///" + ns + "/bulk_import_01"), tablesDirs1, conf);
        Assert.assertEquals(new Path("viewfs:/" + ns + "/accumulo/tables"), match);
      }
    }
  }

  @Test
  public void testOverlappingMountPoints() throws IllegalArgumentException, IOException {
    if (ViewFSUtils.isViewFSSupported()) {
      Configuration conf = new Configuration(false);
      conf.set("fs.viewfs.mounttable.default.link./", "file:///tmp/0");
      conf.set("fs.viewfs.mounttable.default.link./ns1", "file:///tmp/1");
      conf.set("fs.viewfs.mounttable.default.link./ns1/A", "file:///tmp/2");
      conf.set("fs.viewfs.mounttable.default.link./ns1/AA", "file:///tmp/3");
      conf.set("fs.viewfs.mounttable.default.link./ns1/C", "file:///tmp/3");
      conf.set("fs.viewfs.mounttable.default.link./ns2", "file:///tmp/3");

      String[] tablesDirs1 = shuffle("viewfs:///ns1/accumulo/tables", "viewfs:///ns1/A/accumulo/tables", "viewfs:///ns1/AA/accumulo/tables",
          "viewfs:///ns1/C/accumulo/tables", "viewfs:///ns2/accumulo/tables", "viewfs:///accumulo/tables");
      String[] tablesDirs2 = shuffle("viewfs:/ns1/accumulo/tables", "viewfs:/ns1/A/accumulo/tables", "viewfs:/ns1/AA/accumulo/tables",
          "viewfs:/ns1/C/accumulo/tables", "viewfs:/ns2/accumulo/tables", "viewfs:/accumulo/tables");

      for (String ns : Arrays.asList("", "/ns1", "/ns1/A", "/ns1/AA", "/ns1/C", "/ns2")) {
        Path match = ViewFSUtils.matchingFileSystem(new Path("viewfs:" + ns + "/bulk_import_01"), tablesDirs2, conf);
        Assert.assertEquals(new Path("viewfs:" + ns + "/accumulo/tables"), match);

        match = ViewFSUtils.matchingFileSystem(new Path("viewfs://" + ns + "/bulk_import_01"), tablesDirs1, conf);
        Assert.assertEquals(new Path("viewfs:" + ns + "/accumulo/tables"), match);

        match = ViewFSUtils.matchingFileSystem(new Path("viewfs:" + ns + "/bulk_import_01"), tablesDirs2, conf);
        Assert.assertEquals(new Path("viewfs:" + ns + "/accumulo/tables"), match);

        match = ViewFSUtils.matchingFileSystem(new Path("viewfs://" + ns + "/bulk_import_01"), tablesDirs1, conf);
        Assert.assertEquals(new Path("viewfs:" + ns + "/accumulo/tables"), match);
      }
    }
  }
}
