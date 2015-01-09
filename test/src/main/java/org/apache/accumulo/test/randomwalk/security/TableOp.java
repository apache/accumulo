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
package org.apache.accumulo.test.randomwalk.security;

import static com.google.common.base.Charsets.UTF_8;

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
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
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
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class TableOp extends Test {

  @Override
  public void visit(State state, Properties props) throws Exception {
    Connector conn = state.getInstance().getConnector(WalkingSecurity.get(state).getTabUserName(), WalkingSecurity.get(state).getTabToken());

    String action = props.getProperty("action", "_random");
    TablePermission tp;
    if ("_random".equalsIgnoreCase(action)) {
      Random r = new Random();
      tp = TablePermission.values()[r.nextInt(TablePermission.values().length)];
    } else {
      tp = TablePermission.valueOf(action);
    }

    boolean tableExists = WalkingSecurity.get(state).getTableExists();
    String tableName = WalkingSecurity.get(state).getTableName();
    String namespaceName = WalkingSecurity.get(state).getNamespaceName();

    switch (tp) {
      case READ: {
        boolean canRead = WalkingSecurity.get(state).canScan(WalkingSecurity.get(state).getTabCredentials(), tableName, namespaceName);
        Authorizations auths = WalkingSecurity.get(state).getUserAuthorizations(WalkingSecurity.get(state).getTabCredentials());
        boolean ambiguousZone = WalkingSecurity.get(state).inAmbiguousZone(conn.whoami(), tp);
        boolean ambiguousAuths = WalkingSecurity.get(state).ambiguousAuthorizations(conn.whoami());

        Scanner scan = null;
        try {
          scan = conn.createScanner(tableName, conn.securityOperations().getUserAuthorizations(conn.whoami()));
          int seen = 0;
          Iterator<Entry<Key,Value>> iter = scan.iterator();
          while (iter.hasNext()) {
            Entry<Key,Value> entry = iter.next();
            Key k = entry.getKey();
            seen++;
            if (!auths.contains(k.getColumnVisibilityData()) && !ambiguousAuths)
              throw new AccumuloException("Got data I should not be capable of seeing: " + k + " table " + tableName);
          }
          if (!canRead && !ambiguousZone)
            throw new AccumuloException("Was able to read when I shouldn't have had the perm with connection user " + conn.whoami() + " table " + tableName);
          for (Entry<String,Integer> entry : WalkingSecurity.get(state).getAuthsMap().entrySet()) {
            if (auths.contains(entry.getKey().getBytes(UTF_8)))
              seen = seen - entry.getValue();
          }
          if (seen != 0 && !ambiguousAuths)
            throw new AccumuloException("Got mismatched amounts of data");
        } catch (TableNotFoundException tnfe) {
          if (tableExists)
            throw new AccumuloException("Accumulo and test suite out of sync: table " + tableName, tnfe);
          return;
        } catch (AccumuloSecurityException ae) {
          if (ae.getSecurityErrorCode().equals(SecurityErrorCode.PERMISSION_DENIED)) {
            if (canRead && !ambiguousZone)
              throw new AccumuloException("Table read permission out of sync with Accumulo: table " + tableName, ae);
            else
              return;
          }
          if (ae.getSecurityErrorCode().equals(SecurityErrorCode.BAD_AUTHORIZATIONS)) {
            if (ambiguousAuths)
              return;
            else
              throw new AccumuloException("Mismatched authorizations! ", ae);
          }
          throw new AccumuloException("Unexpected exception!", ae);
        } catch (RuntimeException re) {
          if (re.getCause() instanceof AccumuloSecurityException
              && ((AccumuloSecurityException) re.getCause()).getSecurityErrorCode().equals(SecurityErrorCode.PERMISSION_DENIED)) {
            if (canRead && !ambiguousZone)
              throw new AccumuloException("Table read permission out of sync with Accumulo: table " + tableName, re.getCause());
            else
              return;
          }
          if (re.getCause() instanceof AccumuloSecurityException
              && ((AccumuloSecurityException) re.getCause()).getSecurityErrorCode().equals(SecurityErrorCode.BAD_AUTHORIZATIONS)) {
            if (ambiguousAuths)
              return;
            else
              throw new AccumuloException("Mismatched authorizations! ", re.getCause());
          }

          throw new AccumuloException("Unexpected exception!", re);
        } finally {
          if (scan != null) {
            scan.close();
            scan = null;
          }

        }

        break;
      }
      case WRITE:
        boolean canWrite = WalkingSecurity.get(state).canWrite(WalkingSecurity.get(state).getTabCredentials(), tableName, namespaceName);
        boolean ambiguousZone = WalkingSecurity.get(state).inAmbiguousZone(conn.whoami(), tp);

        String key = WalkingSecurity.get(state).getLastKey() + "1";
        Mutation m = new Mutation(new Text(key));
        for (String s : WalkingSecurity.get(state).getAuthsArray()) {
          m.put(new Text(), new Text(), new ColumnVisibility(s), new Value("value".getBytes(UTF_8)));
        }
        BatchWriter writer = null;
        try {
          try {
            writer = conn.createBatchWriter(tableName, new BatchWriterConfig().setMaxMemory(9000l).setMaxWriteThreads(1));
          } catch (TableNotFoundException tnfe) {
            if (tableExists)
              throw new AccumuloException("Table didn't exist when it should have: " + tableName);
            return;
          }
          boolean works = true;
          try {
            writer.addMutation(m);
            writer.close();
          } catch (MutationsRejectedException mre) {
            // Currently no method for detecting reason for mre. Waiting on ACCUMULO-670
            // For now, just wait a second and go again if they can write!
            if (!canWrite)
              return;

            if (ambiguousZone) {
              Thread.sleep(1000);
              try {
                writer = conn.createBatchWriter(tableName, new BatchWriterConfig().setMaxWriteThreads(1));
                writer.addMutation(m);
                writer.close();
                writer = null;
              } catch (MutationsRejectedException mre2) {
                throw new AccumuloException("Mutation exception!", mre2);
              }
            }
          }
          if (works)
            for (String s : WalkingSecurity.get(state).getAuthsArray())
              WalkingSecurity.get(state).increaseAuthMap(s, 1);
        } finally {
          if (writer != null) {
            writer.close();
            writer = null;
          }
        }
        break;
      case BULK_IMPORT:
        key = WalkingSecurity.get(state).getLastKey() + "1";
        SortedSet<Key> keys = new TreeSet<Key>();
        for (String s : WalkingSecurity.get(state).getAuthsArray()) {
          Key k = new Key(key, "", "", s);
          keys.add(k);
        }
        Path dir = new Path("/tmp", "bulk_" + UUID.randomUUID().toString());
        Path fail = new Path(dir.toString() + "_fail");
        FileSystem fs = WalkingSecurity.get(state).getFs();
        FileSKVWriter f = FileOperations.getInstance().openWriter(dir + "/securityBulk." + RFile.EXTENSION, fs, fs.getConf(),
            AccumuloConfiguration.getDefaultConfiguration());
        f.startDefaultLocalityGroup();
        fs.mkdirs(fail);
        for (Key k : keys)
          f.append(k, new Value("Value".getBytes(UTF_8)));
        f.close();
        try {
          conn.tableOperations().importDirectory(tableName, dir.toString(), fail.toString(), true);
        } catch (TableNotFoundException tnfe) {
          if (tableExists)
            throw new AccumuloException("Table didn't exist when it should have: " + tableName);
          return;
        } catch (AccumuloSecurityException ae) {
          if (ae.getSecurityErrorCode().equals(SecurityErrorCode.PERMISSION_DENIED)) {
            if (WalkingSecurity.get(state).canBulkImport(WalkingSecurity.get(state).getTabCredentials(), tableName, namespaceName))
              throw new AccumuloException("Bulk Import failed when it should have worked: " + tableName);
            return;
          } else if (ae.getSecurityErrorCode().equals(SecurityErrorCode.BAD_CREDENTIALS)) {
            if (WalkingSecurity.get(state).userPassTransient(conn.whoami()))
              return;
          }
          throw new AccumuloException("Unexpected exception!", ae);
        }
        for (String s : WalkingSecurity.get(state).getAuthsArray())
          WalkingSecurity.get(state).increaseAuthMap(s, 1);
        fs.delete(dir, true);
        fs.delete(fail, true);

        if (!WalkingSecurity.get(state).canBulkImport(WalkingSecurity.get(state).getTabCredentials(), tableName, namespaceName))
          throw new AccumuloException("Bulk Import succeeded when it should have failed: " + dir + " table " + tableName);
        break;
      case ALTER_TABLE:
        AlterTable.renameTable(conn, state, tableName, tableName + "plus",
            WalkingSecurity.get(state).canAlterTable(WalkingSecurity.get(state).getTabCredentials(), tableName, namespaceName), tableExists);
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
