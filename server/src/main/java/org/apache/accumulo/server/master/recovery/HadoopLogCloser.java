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
package org.apache.accumulo.server.master.recovery;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.Logger;

public class HadoopLogCloser implements LogCloser {
  
  private static Logger log = Logger.getLogger(HadoopLogCloser.class);

  @Override
  public long close(FileSystem fs, Path source) throws IOException {
    
    if (fs instanceof DistributedFileSystem) {
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      try {
        if (!dfs.recoverLease(source)) {
          log.info("Waiting for file to be closed " + source.toString());
          return 1000;
        }
        log.info("Recovered lease on " + source.toString());
        return 0;
      } catch (FileNotFoundException ex) {
        throw ex;
      } catch (Exception ex) {
        log.warn("Error recovery lease on " + source.toString(), ex);
      }
    } else if (fs instanceof LocalFileSystem) {
      // ignore
    } else {
      throw new IllegalStateException("Don't know how to recover a lease for " + fs.getClass().getName());
    }
    fs.append(source).close();
    log.info("Recovered lease on " + source.toString() + " using append");
    return 0;
  }
  
}
