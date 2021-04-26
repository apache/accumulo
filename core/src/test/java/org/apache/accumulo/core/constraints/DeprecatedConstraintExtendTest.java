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
package org.apache.accumulo.core.constraints;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This tests the case where a user extended a Constraint class before it was deprecated to make
 * sure the old Constraint will still work with the API migration changes.
 *
 * @since 2.1.0
 */
@SuppressWarnings("deprecation")
public class DeprecatedConstraintExtendTest {

  Constraint constraint = new MinKeySizeConstraint();

  byte[] min = new byte[1024];
  byte[] oversized = new byte[1048577];

  @Test
  public void testMinKeySizeConstraint() {

    // pass constraints
    Mutation m = new Mutation(min);
    m.put("colf", "colq", new Value(new byte[] {}));
    assertEquals(Collections.emptyList(), constraint.check(null, m));

    // test with row id < 1KB
    m = new Mutation("rowid");
    m.put("colf", "colq", new Value(new byte[] {}));
    assertEquals(Collections.singletonList(MinKeySizeConstraint.MIN_KEY_SIZE_EXCEEDED_VIOLATION),
        constraint.check(null, m));

    // test with colf > 1mb
    m = new Mutation("rowid");
    m.put(new Text(oversized), new Text("colq"), new Value(new byte[] {}));
    assertEquals(
        Collections.singletonList(DefaultKeySizeConstraint.MAX__KEY_SIZE_EXCEEDED_VIOLATION),
        constraint.check(null, m));
  }

  /**
   * Limit the size of 1mb but also a minimum of 1KB
   */
  @SuppressFBWarnings(value = "NM_WRONG_PACKAGE",
      justification = "Same name used for compatibility during deprecation cycle")
  private static class MinKeySizeConstraint extends DefaultKeySizeConstraint {
    protected static final short MIN_KEY_SIZE_EXCEEDED_VIOLATION = 2;
    protected static final long minSize = 1024; // 1MB default size

    @Override
    public List<Short> check(Constraint.Environment env, Mutation mutation) {
      List<Short> violations = super.check(env, mutation);
      if (!violations.isEmpty())
        return violations;

      for (ColumnUpdate cu : mutation.getUpdates()) {
        int size = mutation.getRow().length;
        size += cu.getColumnFamily().length;
        size += cu.getColumnQualifier().length;
        size += cu.getColumnVisibility().length;

        if (size < minSize)
          violations.add(MIN_KEY_SIZE_EXCEEDED_VIOLATION);
      }
      return violations;
    }
  }
}
