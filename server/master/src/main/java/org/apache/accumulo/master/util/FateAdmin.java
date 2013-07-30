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
package org.apache.accumulo.master.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.AdminUtil;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

/**
 * A utility to administer FATE operations
 */
public class FateAdmin {
  
  static class TxOpts {
    @Parameter(description = "<txid>", required = true)
    List<String> args = new ArrayList<String>();
  }
  
  @Parameters(commandDescription = "Stop an existing FATE by transaction id")
  static class FailOpts extends TxOpts {}
  
  @Parameters(commandDescription = "Delete an existing FATE by transaction id")
  static class DeleteOpts extends TxOpts {}
  
  @Parameters(commandDescription = "List the existing FATE transactions")
  static class PrintOpts {}
  
  public static void main(String[] args) throws Exception {
    Help opts = new Help();
    JCommander jc = new JCommander(opts);
    jc.setProgramName(FateAdmin.class.getName());
    jc.addCommand("fail", new FailOpts());
    jc.addCommand("delete", new DeleteOpts());
    jc.addCommand("print", new PrintOpts());
    jc.parse(args);
    if (opts.help || jc.getParsedCommand() == null) {
      jc.usage();
      System.exit(1);
    }
    
    System.err.printf("This tool has been deprecated%nFATE administration now available within 'accumulo shell'%n$ fate fail <txid>... | delete <txid>... | print [<txid>...]%n%n");
    
    AdminUtil<Master> admin = new AdminUtil<Master>();
    
    Instance instance = HdfsZooInstance.getInstance();
    String path = ZooUtil.getRoot(instance) + Constants.ZFATE;
    String masterPath = ZooUtil.getRoot(instance) + Constants.ZMASTER_LOCK;
    IZooReaderWriter zk = ZooReaderWriter.getRetryingInstance();
    ZooStore<Master> zs = new ZooStore<Master>(path, zk);
    
    if (jc.getParsedCommand().equals("fail")) {
      admin.prepFail(zs, zk, masterPath, args[1]);
    } else if (jc.getParsedCommand().equals("delete")) {
      admin.prepDelete(zs, zk, masterPath, args[1]);
      admin.deleteLocks(zs, zk, ZooUtil.getRoot(instance) + Constants.ZTABLE_LOCKS, args[1]);
    } else if (jc.getParsedCommand().equals("print")) {
      admin.print(zs, zk, ZooUtil.getRoot(instance) + Constants.ZTABLE_LOCKS);
    }
  }
}
