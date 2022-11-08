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
package org.apache.accumulo.server.init;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.spi.compaction.SimpleCompactionDispatcher;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.accumulo.server.constraints.MetadataConstraints;
import org.apache.accumulo.server.iterators.MetadataBulkLoadFilter;
import org.apache.accumulo.server.util.ReplicationTableUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import com.google.common.base.Joiner;

class InitialConfiguration {

  // config only for root table
  private final HashMap<String,String> initialRootConf = new HashMap<>();
  // config for root and metadata table
  private final HashMap<String,String> initialRootMetaConf = new HashMap<>();
  // config for only metadata table
  private final HashMap<String,String> initialMetaConf = new HashMap<>();
  private final HashMap<String,String> initialReplicationTableConf = new HashMap<>();
  private final Configuration hadoopConf;
  private final SiteConfiguration siteConf;

  InitialConfiguration(Configuration hadoopConf, SiteConfiguration siteConf) {
    this.hadoopConf = hadoopConf;
    this.siteConf = siteConf;
    initialRootConf.put(Property.TABLE_COMPACTION_DISPATCHER.getKey(),
        SimpleCompactionDispatcher.class.getName());
    initialRootConf.put(Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey() + "service", "root");

    initialRootMetaConf.put(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "32K");
    initialRootMetaConf.put(Property.TABLE_FILE_REPLICATION.getKey(), "5");
    initialRootMetaConf.put(Property.TABLE_DURABILITY.getKey(), "sync");
    initialRootMetaConf.put(Property.TABLE_MAJC_RATIO.getKey(), "1");
    initialRootMetaConf.put(Property.TABLE_SPLIT_THRESHOLD.getKey(), "64M");
    initialRootMetaConf.put(Property.TABLE_CONSTRAINT_PREFIX.getKey() + "1",
        MetadataConstraints.class.getName());
    initialRootMetaConf.put(Property.TABLE_ITERATOR_PREFIX.getKey() + "scan.vers",
        "10," + VersioningIterator.class.getName());
    initialRootMetaConf.put(Property.TABLE_ITERATOR_PREFIX.getKey() + "scan.vers.opt.maxVersions",
        "1");
    initialRootMetaConf.put(Property.TABLE_ITERATOR_PREFIX.getKey() + "minc.vers",
        "10," + VersioningIterator.class.getName());
    initialRootMetaConf.put(Property.TABLE_ITERATOR_PREFIX.getKey() + "minc.vers.opt.maxVersions",
        "1");
    initialRootMetaConf.put(Property.TABLE_ITERATOR_PREFIX.getKey() + "majc.vers",
        "10," + VersioningIterator.class.getName());
    initialRootMetaConf.put(Property.TABLE_ITERATOR_PREFIX.getKey() + "majc.vers.opt.maxVersions",
        "1");
    initialRootMetaConf.put(Property.TABLE_ITERATOR_PREFIX.getKey() + "majc.bulkLoadFilter",
        "20," + MetadataBulkLoadFilter.class.getName());
    initialRootMetaConf.put(Property.TABLE_FAILURES_IGNORE.getKey(), "false");
    initialRootMetaConf.put(Property.TABLE_LOCALITY_GROUP_PREFIX.getKey() + "tablet",
        String.format("%s,%s", MetadataSchema.TabletsSection.TabletColumnFamily.NAME,
            MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME));
    initialRootMetaConf.put(Property.TABLE_LOCALITY_GROUP_PREFIX.getKey() + "server",
        String.format("%s,%s,%s,%s", MetadataSchema.TabletsSection.DataFileColumnFamily.NAME,
            MetadataSchema.TabletsSection.LogColumnFamily.NAME,
            MetadataSchema.TabletsSection.ServerColumnFamily.NAME,
            MetadataSchema.TabletsSection.FutureLocationColumnFamily.NAME));
    initialRootMetaConf.put(Property.TABLE_LOCALITY_GROUPS.getKey(), "tablet,server");
    initialRootMetaConf.put(Property.TABLE_DEFAULT_SCANTIME_VISIBILITY.getKey(), "");
    initialRootMetaConf.put(Property.TABLE_INDEXCACHE_ENABLED.getKey(), "true");
    initialRootMetaConf.put(Property.TABLE_BLOCKCACHE_ENABLED.getKey(), "true");

    initialMetaConf.put(Property.TABLE_COMPACTION_DISPATCHER.getKey(),
        SimpleCompactionDispatcher.class.getName());
    initialMetaConf.put(Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey() + "service", "meta");

    // ACCUMULO-3077 Set the combiner on accumulo.metadata during init to reduce the likelihood of a
    // race condition where a tserver compacts away Status updates because it didn't see the
    // Combiner
    // configured
    @SuppressWarnings("deprecation")
    var statusCombinerClass = org.apache.accumulo.server.replication.StatusCombiner.class;
    IteratorSetting setting =
        new IteratorSetting(9, ReplicationTableUtil.COMBINER_NAME, statusCombinerClass);
    Combiner.setColumns(setting, Collections
        .singletonList(new IteratorSetting.Column(MetadataSchema.ReplicationSection.COLF)));
    for (IteratorUtil.IteratorScope scope : IteratorUtil.IteratorScope.values()) {
      String root = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX,
          scope.name().toLowerCase(), setting.getName());
      for (Map.Entry<String,String> prop : setting.getOptions().entrySet()) {
        initialMetaConf.put(root + ".opt." + prop.getKey(), prop.getValue());
      }
      initialMetaConf.put(root, setting.getPriority() + "," + setting.getIteratorClass());
    }

    // add combiners to replication table
    @SuppressWarnings("deprecation")
    String replicationCombinerName =
        org.apache.accumulo.core.replication.ReplicationTable.COMBINER_NAME;
    setting = new IteratorSetting(30, replicationCombinerName, statusCombinerClass);
    setting.setPriority(30);
    @SuppressWarnings("deprecation")
    Text statusSectionName =
        org.apache.accumulo.core.replication.ReplicationSchema.StatusSection.NAME;
    @SuppressWarnings("deprecation")
    Text workSectionName = org.apache.accumulo.core.replication.ReplicationSchema.WorkSection.NAME;
    Combiner.setColumns(setting, Arrays.asList(new IteratorSetting.Column(statusSectionName),
        new IteratorSetting.Column(workSectionName)));
    for (IteratorUtil.IteratorScope scope : EnumSet.allOf(IteratorUtil.IteratorScope.class)) {
      String root = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX,
          scope.name().toLowerCase(), setting.getName());
      for (Map.Entry<String,String> prop : setting.getOptions().entrySet()) {
        initialReplicationTableConf.put(root + ".opt." + prop.getKey(), prop.getValue());
      }
      initialReplicationTableConf.put(root,
          setting.getPriority() + "," + setting.getIteratorClass());
    }
    // add locality groups to replication table
    @SuppressWarnings("deprecation")
    Map<String,Set<Text>> replicationLocalityGroups =
        org.apache.accumulo.core.replication.ReplicationTable.LOCALITY_GROUPS;
    for (Map.Entry<String,Set<Text>> g : replicationLocalityGroups.entrySet()) {
      initialReplicationTableConf.put(Property.TABLE_LOCALITY_GROUP_PREFIX + g.getKey(),
          LocalityGroupUtil.encodeColumnFamilies(g.getValue()));
    }
    initialReplicationTableConf.put(Property.TABLE_LOCALITY_GROUPS.getKey(),
        Joiner.on(",").join(replicationLocalityGroups.keySet()));
    // add formatter to replication table
    @SuppressWarnings("deprecation")
    String replicationFormatterClassName =
        org.apache.accumulo.server.replication.ReplicationUtil.STATUS_FORMATTER_CLASS_NAME;
    initialReplicationTableConf.put(Property.TABLE_FORMATTER_CLASS.getKey(),
        replicationFormatterClassName);

    int max = hadoopConf.getInt("dfs.replication.max", 512);
    // Hadoop 0.23 switched the min value configuration name
    int min = Math.max(hadoopConf.getInt("dfs.replication.min", 1),
        hadoopConf.getInt("dfs.namenode.replication.min", 1));
    if (max < 5) {
      setMetadataReplication(max, "max");
    }
    if (min > 5) {
      setMetadataReplication(min, "min");
    }
  }

  private void setMetadataReplication(int replication, String reason) {
    String rep = System.console()
        .readLine("Your HDFS replication " + reason + " is not compatible with our default "
            + MetadataTable.NAME + " replication of 5. What do you want to set your "
            + MetadataTable.NAME + " replication to? (" + replication + ") ");
    if (rep == null || rep.isEmpty()) {
      rep = Integer.toString(replication);
    } else {
      // Lets make sure it's a number
      Integer.parseInt(rep);
    }
    initialRootMetaConf.put(Property.TABLE_FILE_REPLICATION.getKey(), rep);
  }

  HashMap<String,String> getRootTableConf() {
    return initialRootConf;
  }

  HashMap<String,String> getRootMetaConf() {
    return initialRootMetaConf;
  }

  HashMap<String,String> getMetaTableConf() {
    return initialMetaConf;
  }

  HashMap<String,String> getReplTableConf() {
    return initialReplicationTableConf;
  }

  Configuration getHadoopConf() {
    return hadoopConf;
  }

  SiteConfiguration getSiteConf() {
    return siteConf;
  }

  Set<String> getVolumeUris() {
    return VolumeConfiguration.getVolumeUris(siteConf);
  }

  String get(Property property) {
    return siteConf.get(property);
  }

  boolean getBoolean(Property property) {
    return siteConf.getBoolean(property);
  }

  void getProperties(Map<String,String> props, Predicate<String> filter, boolean defaults) {
    siteConf.getProperties(props, filter, defaults);
  }
}
