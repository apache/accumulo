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
package org.apache.accumulo.server.upgrade;

import java.util.SortedMap;

import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class RenameFiles {
    public static void main(String[] args) throws Exception {
        String tablesDir = ServerConstants.getTablesDir();
        System.out.println(" " + tablesDir);
        SortedMap<String,String> tableIds = Tables.getNameToIdMap(HdfsZooInstance.getInstance());
        
        Configuration conf = CachedConfiguration.getInstance();
        FileSystem fs = FileSystem.get(conf);
        
        FileStatus[] tables = fs.listStatus(new Path(tablesDir));
        
        for (FileStatus tableStatus : tables) {
            String tid = tableIds.get(tableStatus.getPath().getName());
            Path newPath = new Path(tableStatus.getPath().getParent(), tid);
            fs.rename(tableStatus.getPath(), newPath);
        }
        
        tables = fs.listStatus(new Path(tablesDir));
        
        for (FileStatus tableStatus : tables) {
            FileStatus[] tablets = fs.listStatus(tableStatus.getPath());
            for (FileStatus tabletStatus : tablets) {
                FileStatus[] files = fs.listStatus(tabletStatus.getPath());
                for (FileStatus fileStatus : files) {
                    String name = fileStatus.getPath().getName();
                    if (name.matches("map_\\d+_\\d+")) {
                        fs.rename(fileStatus.getPath(), new Path(fileStatus.getPath().getParent(), name.substring(4) + ".map"));
                    }
                }
                
            }
        }
        
    }
}
