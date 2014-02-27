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
package org.apache.accumulo.core.file;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class VolumeConfiguration {

  public static FileSystem getFileSystem(String path, Configuration conf, AccumuloConfiguration acuconf) throws IOException {
    if (path.contains(":"))
      return new Path(path).getFileSystem(conf);
    else
      return getDefaultFilesystem(conf, acuconf);
  }

  public static FileSystem getDefaultFilesystem(Configuration conf, AccumuloConfiguration acuconf) throws IOException {
    String uri = acuconf.get(Property.INSTANCE_DFS_URI);
    if ("".equals(uri))
      return FileSystem.get(conf);
    else
      try {
        return FileSystem.get(new URI(uri), conf);
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
  }

  public static String getConfiguredBaseDir(AccumuloConfiguration conf) {
    String singleNamespace = conf.get(Property.INSTANCE_DFS_DIR);
    String dfsUri = conf.get(Property.INSTANCE_DFS_URI);
    String baseDir;
  
    if (dfsUri == null || dfsUri.isEmpty()) {
      Configuration hadoopConfig = CachedConfiguration.getInstance();
      try {
        baseDir = FileSystem.get(hadoopConfig).getUri().toString() + singleNamespace;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      if (!dfsUri.contains(":"))
        throw new IllegalArgumentException("Expected fully qualified URI for " + Property.INSTANCE_DFS_URI.getKey() + " got " + dfsUri);
      baseDir = dfsUri + singleNamespace;
    }
    return baseDir;
  }

  public static String[] getConfiguredBaseDirs(AccumuloConfiguration conf) {
    String singleNamespace = conf.get(Property.INSTANCE_DFS_DIR);
    String ns = conf.get(Property.INSTANCE_VOLUMES);
  
    String configuredBaseDirs[];
  
    if (ns == null || ns.isEmpty()) {
      configuredBaseDirs = new String[] {getConfiguredBaseDir(conf)};
    } else {
      String namespaces[] = ns.split(",");
      String unescapedNamespaces[] = new String[namespaces.length];
      int i = 0;
      for (String namespace : namespaces) {
        if (!namespace.contains(":")) {
          throw new IllegalArgumentException("Expected fully qualified URI for " + Property.INSTANCE_VOLUMES.getKey() + " got " + namespace);
        }
  
        try {
          // pass through URI to unescape hex encoded chars (e.g. convert %2C to "," char)
          unescapedNamespaces[i++] = new Path(new URI(namespace)).toString();
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException(Property.INSTANCE_VOLUMES.getKey() + " contains " + namespace + " which has a syntax error", e);
        }
      }
  
      configuredBaseDirs = prefix(unescapedNamespaces, singleNamespace);
    }
  
    return configuredBaseDirs;
  }

  public static String[] prefix(String bases[], String suffix) {
    if (suffix.startsWith("/"))
      suffix = suffix.substring(1);
    String result[] = new String[bases.length];
    for (int i = 0; i < bases.length; i++) {
      result[i] = bases[i] + "/" + suffix;
    }
    return result;
  }

}
