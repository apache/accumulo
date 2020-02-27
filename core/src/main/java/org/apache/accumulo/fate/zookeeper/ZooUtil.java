/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.fate.zookeeper;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;

public class ZooUtil {

  public enum NodeExistsPolicy {
    SKIP, OVERWRITE, FAIL
  }

  public enum NodeMissingPolicy {
    SKIP, CREATE, FAIL
  }

  public static class LockID {
    public long eid;
    public String path;
    public String node;

    public LockID(String root, String serializedLID) {
      String[] sa = serializedLID.split("\\$");
      int lastSlash = sa[0].lastIndexOf('/');

      if (sa.length != 2 || lastSlash < 0) {
        throw new IllegalArgumentException("Malformed serialized lock id " + serializedLID);
      }

      if (lastSlash == 0)
        path = root;
      else
        path = root + "/" + sa[0].substring(0, lastSlash);
      node = sa[0].substring(lastSlash + 1);
      eid = new BigInteger(sa[1], 16).longValue();
    }

    public LockID(String path, String node, long eid) {
      this.path = path;
      this.node = node;
      this.eid = eid;
    }

    public String serialize(String root) {

      return path.substring(root.length()) + "/" + node + "$" + Long.toHexString(eid);
    }

    @Override
    public String toString() {
      return " path = " + path + " node = " + node + " eid = " + Long.toHexString(eid);
    }
  }

  public static final List<ACL> PRIVATE;
  public static final List<ACL> PUBLIC;

  static {
    PRIVATE = new ArrayList<>();
    PRIVATE.addAll(Ids.CREATOR_ALL_ACL);
    PUBLIC = new ArrayList<>();
    PUBLIC.addAll(PRIVATE);
    PUBLIC.add(new ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE));
  }

  public static String getRoot(final String instanceId) {
    return Constants.ZROOT + "/" + instanceId;
  }

}
