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
package org.apache.accumulo.server.test.randomwalk.security;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.SecurityErrorCode;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class TableOp extends Test {
  
  @Override
  public void visit(State state, Properties props) throws Exception {
    boolean userExists = SecurityHelper.getTabUserExists(state);
    Connector conn;
    try {
      conn = state.getInstance().getConnector(SecurityHelper.getTabUserName(state), SecurityHelper.getTabUserPass(state));
    } catch (AccumuloSecurityException ae) {
      if (ae.getErrorCode().equals(SecurityErrorCode.BAD_CREDENTIALS)) {
        if (userExists)
          throw new AccumuloException("User didn't exist when they should (or worse- password mismatch)", ae);
        else
          return;
      }
      throw new AccumuloException("Unexpected exception!", ae);
    }
    String action = props.getProperty("action", "_random");
    TablePermission tp;
    if ("_random".equalsIgnoreCase(action)) {
      Random r = new Random();
      tp = TablePermission.values()[r.nextInt(TablePermission.values().length)];
    } else {
      tp = TablePermission.valueOf(action);
    }
    
    boolean tableExists = SecurityHelper.getTableExists(state);
    boolean hasPerm = SecurityHelper.getTabPerm(state, SecurityHelper.getTabUserName(state), tp);
    
    String tableName = state.getString("secTableName");
    
    switch (tp) {
      case READ:
        Authorizations auths = SecurityHelper.getUserAuths(state, SecurityHelper.getTabUserName(state));
        boolean canRead = SecurityHelper.getTabPerm(state, SecurityHelper.getTabUserName(state), TablePermission.READ);
        try {
          Scanner scan = conn.createScanner(tableName, conn.securityOperations().getUserAuthorizations(SecurityHelper.getTabUserName(state)));
          int seen = 0;
          Iterator<Entry<Key,Value>> iter = scan.iterator();
          while (iter.hasNext()) {
            Entry<Key,Value> entry = iter.next();
            Key k = entry.getKey();
            seen++;
            if (!auths.contains(k.getColumnVisibilityData()))
              throw new AccumuloException("Got data I should not be capable of seeing: " + k + " table " + tableName);
          }
          if (!canRead)
            throw new AccumuloException("Was able to read when I shouldn't have had the perm with connection user " + conn.whoami() + " table " + tableName);
          for (Entry<String,Integer> entry : SecurityHelper.getAuthsMap(state).entrySet()) {
            if (auths.contains(entry.getKey().getBytes()))
              seen = seen - entry.getValue();
          }
          if (seen != 0)
            throw new AccumuloException("Got mismatched amounts of data");
        } catch (TableNotFoundException tnfe) {
          if (tableExists)
            throw new AccumuloException("Accumulo and test suite out of sync: table " + tableName, tnfe);
          return;
        } catch (AccumuloSecurityException ae) {
          if (ae.getErrorCode().equals(SecurityErrorCode.PERMISSION_DENIED)) {
            if (canRead)
              throw new AccumuloException("Table read permission out of sync with Accumulo: table " + tableName, ae);
            else
              return;
          }
          throw new AccumuloException("Unexpected exception!", ae);
        } catch (RuntimeException re) {
          if (re.getCause() instanceof AccumuloSecurityException
              && ((AccumuloSecurityException) re.getCause()).getErrorCode().equals(SecurityErrorCode.PERMISSION_DENIED)) {
            if (canRead)
              throw new AccumuloException("Table read permission out of sync with Accumulo: table " + tableName, re.getCause());
            else
              return;
          }
          throw new AccumuloException("Unexpected exception!", re);
        }
        
        break;
      case WRITE:
        String key = SecurityHelper.getLastKey(state) + "1";
        Mutation m = new Mutation(new Text(key));
        for (String s : SecurityHelper.getAuthsArray()) {
          m.put(new Text(), new Text(), new ColumnVisibility(s), new Value("value".getBytes()));
        }
        BatchWriter writer;
        try {
          writer = conn.createBatchWriter(tableName, 9000l, 0l, 1);
        } catch (TableNotFoundException tnfe) {
          if (tableExists)
            throw new AccumuloException("Table didn't exist when it should have: " + tableName);
          return;
        }
        boolean works = true;
        try {
          writer.addMutation(m);
        } catch (MutationsRejectedException mre) {
          throw new AccumuloException("Mutation exception!", mre);
        }
        if (works)
          for (String s : SecurityHelper.getAuthsArray())
            SecurityHelper.increaseAuthMap(state, s, 1);
        break;
      case BULK_IMPORT:
        key = SecurityHelper.getLastKey(state) + "1";
        SortedSet<Key> keys = new TreeSet<Key>();
        for (String s : SecurityHelper.getAuthsArray()) {
          Key k = new Key(key, "", "", s);
          keys.add(k);
        }
        Path dir = new Path("/tmp", "bulk_" + UUID.randomUUID().toString());
        Path fail = new Path(dir.toString() + "_fail");
        FileSystem fs = SecurityHelper.getFs(state);
        FileSKVWriter f = FileOperations.getInstance().openWriter(dir + "/securityBulk." + RFile.EXTENSION, fs, fs.getConf(),
            AccumuloConfiguration.getDefaultConfiguration());
        f.startDefaultLocalityGroup();
        fs.mkdirs(fail);
        for (Key k : keys)
          f.append(k, new Value("Value".getBytes()));
        f.close();
        try {
          conn.tableOperations().importDirectory(tableName, dir.toString(), fail.toString(), true);
        } catch (TableNotFoundException tnfe) {
          if (tableExists)
            throw new AccumuloException("Table didn't exist when it should have: " + tableName);
          return;
        } catch (AccumuloSecurityException ae) {
          if (ae.getErrorCode().equals(SecurityErrorCode.PERMISSION_DENIED)) {
            if (hasPerm)
              throw new AccumuloException("Bulk Import failed when it should have worked: " + tableName);
            return;
          }
          throw new AccumuloException("Unexpected exception!", ae);
        }
        for (String s : SecurityHelper.getAuthsArray())
          SecurityHelper.increaseAuthMap(state, s, 1);
        fs.delete(dir, true);
        fs.delete(fail, true);

        if (!hasPerm)
          throw new AccumuloException("Bulk Import succeeded when it should have failed: " + dir + " table " + tableName);
        break;
      case ALTER_TABLE:
        AlterTable.renameTable(conn, state, tableName, tableName + "plus", hasPerm, tableExists);
        break;
      
      case GRANT:
        props.setProperty("task", "grant");
        props.setProperty("perm", "random");
        props.setProperty("source", "table");
        props.setProperty("target", "system");
        AlterTablePerm.alter(state, props);
        break;
      
      case DROP_TABLE:
        props.setProperty("source", "table");
        DropTable.dropTable(state, props);
        break;
    }
  }
}
