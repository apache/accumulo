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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.spi.compaction.SimpleCompactionDispatcher;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.accumulo.server.constraints.MetadataConstraints;
import org.apache.accumulo.server.iterators.MetadataBulkLoadFilter;
import org.apache.hadoop.conf.Configuration;

class InitialConfiguration {

  // config only for root table
  private final HashMap<String,String> initialRootConf = new HashMap<>();
  // config for root and metadata table
  private final HashMap<String,String> initialRootMetaConf = new HashMap<>();
  // config for only metadata table
  private final HashMap<String,String> initialMetaConf = new HashMap<>();
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
