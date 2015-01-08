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
package org.apache.accumulo.test.randomwalk.shard;

import static com.google.common.base.Charsets.UTF_8;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

/**
 *
 */
public class ExportIndex extends Test {

  @Override
  public void visit(State state, Properties props) throws Exception {

    String indexTableName = (String) state.get("indexTableName");
    String tmpIndexTableName = indexTableName + "_tmp";

    String exportDir = "/tmp/shard_export/" + indexTableName;
    String copyDir = "/tmp/shard_export/" + tmpIndexTableName;

    FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());

    fs.delete(new Path("/tmp/shard_export/" + indexTableName), true);
    fs.delete(new Path("/tmp/shard_export/" + tmpIndexTableName), true);

    // disable spits, so that splits can be compared later w/o worrying one table splitting and the other not
    state.getConnector().tableOperations().setProperty(indexTableName, Property.TABLE_SPLIT_THRESHOLD.getKey(), "20G");

    long t1 = System.currentTimeMillis();

    state.getConnector().tableOperations().flush(indexTableName, null, null, true);
    state.getConnector().tableOperations().offline(indexTableName);

    long t2 = System.currentTimeMillis();

    state.getConnector().tableOperations().exportTable(indexTableName, exportDir);

    long t3 = System.currentTimeMillis();

    // copy files
    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(exportDir, "distcp.txt")), UTF_8));
    String file = null;
    while ((file = reader.readLine()) != null) {
      Path src = new Path(file);
      Path dest = new Path(new Path(copyDir), src.getName());
      FileUtil.copy(fs, src, fs, dest, false, true, CachedConfiguration.getInstance());
    }

    reader.close();

    long t4 = System.currentTimeMillis();

    state.getConnector().tableOperations().online(indexTableName);
    state.getConnector().tableOperations().importTable(tmpIndexTableName, copyDir);

    long t5 = System.currentTimeMillis();

    fs.delete(new Path(exportDir), true);
    fs.delete(new Path(copyDir), true);

    HashSet<Text> splits1 = new HashSet<Text>(state.getConnector().tableOperations().listSplits(indexTableName));
    HashSet<Text> splits2 = new HashSet<Text>(state.getConnector().tableOperations().listSplits(tmpIndexTableName));

    if (!splits1.equals(splits2))
      throw new Exception("Splits not equals " + indexTableName + " " + tmpIndexTableName);

    HashMap<String,String> props1 = new HashMap<String,String>();
    for (Entry<String,String> entry : state.getConnector().tableOperations().getProperties(indexTableName))
      props1.put(entry.getKey(), entry.getValue());

    HashMap<String,String> props2 = new HashMap<String,String>();
    for (Entry<String,String> entry : state.getConnector().tableOperations().getProperties(tmpIndexTableName))
      props2.put(entry.getKey(), entry.getValue());

    if (!props1.equals(props2))
      throw new Exception("Props not equals " + indexTableName + " " + tmpIndexTableName);

    // unset the split threshold
    state.getConnector().tableOperations().removeProperty(indexTableName, Property.TABLE_SPLIT_THRESHOLD.getKey());
    state.getConnector().tableOperations().removeProperty(tmpIndexTableName, Property.TABLE_SPLIT_THRESHOLD.getKey());

    log.debug("Imported " + tmpIndexTableName + " from " + indexTableName + " flush: " + (t2 - t1) + "ms export: " + (t3 - t2) + "ms copy:" + (t4 - t3)
        + "ms import:" + (t5 - t4) + "ms");

  }

}
