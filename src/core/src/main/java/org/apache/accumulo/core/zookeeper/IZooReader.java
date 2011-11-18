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
package org.apache.accumulo.core.zookeeper;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

public interface IZooReader {
  
  public abstract byte[] getData(String zPath, Stat stat) throws KeeperException, InterruptedException;
  
  public abstract Stat getStatus(String zPath) throws KeeperException, InterruptedException;
  
  public abstract Stat getStatus(String zPath, Watcher watcher) throws KeeperException, InterruptedException;
  
  public abstract List<String> getChildren(String zPath) throws KeeperException, InterruptedException;
  
  public abstract List<String> getChildren(String zPath, Watcher watcher) throws KeeperException, InterruptedException;
  
  public abstract boolean exists(String zPath) throws KeeperException, InterruptedException;
  
  public abstract boolean exists(String zPath, Watcher watcher) throws KeeperException, InterruptedException;
  
}
