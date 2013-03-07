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
package org.apache.accumulo.server.tabletserver;

import java.io.IOException;
import java.util.Iterator;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.file.FileUtil;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.data.ServerMutation;
import org.apache.accumulo.server.trace.TraceFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MutationLog {
  private FSDataOutputStream logout;
  
  public static final byte MUTATION_EVENT = 1;
  public static final byte CLOSE_EVENT = 2;
  
  public MutationLog(Path logfile) throws IOException {
    
    Configuration conf = CachedConfiguration.getInstance();
    FileSystem fs = TraceFileSystem.wrap(FileUtil.getFileSystem(conf, ServerConfiguration.getSiteConfiguration()));
    
    if (!fs.exists(logfile))
      logout = fs.create(logfile);
  }
  
  public void log(Mutation m) throws IOException {
    // write event type
    logout.writeByte(MUTATION_EVENT);
    
    // write event
    m.write(logout);
    logout.flush();
  }
  
  public void close() throws IOException {
    logout.writeByte(CLOSE_EVENT);
    logout.close();
  }
  
  public static Iterator<Mutation> replay(Path logfile, Tablet t, long min_timestamp) throws IOException {
    Configuration conf = CachedConfiguration.getInstance();
    FileSystem fs = TraceFileSystem.wrap(FileUtil.getFileSystem(conf, ServerConfiguration.getSiteConfiguration()));
    
    final FSDataInputStream login = fs.open(logfile);
    
    final Mutation mutation = new ServerMutation();
    
    return new Iterator<Mutation>() {
      
      byte eventType;
      
      {
        eventType = login.readByte();
      }
      
      public boolean hasNext() {
        return eventType != CLOSE_EVENT;
      }
      
      public Mutation next() {
        try {
          mutation.readFields(login);
          eventType = login.readByte();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        
        return mutation;
      }
      
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }
}
