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

package org.apache.accumulo.core.clientImpl;

import static org.powermock.api.easymock.PowerMock.createPartialMock;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.PropertyType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@RunWith(PowerMockRunner.class)
@PrepareForTest(NamespaceOperationsImpl.class)
public class NamespaceOperationsImplTest {
  private NamespaceOperationsImpl namespaceOpsImpl;

  @Before
  public void setup() {
    namespaceOpsImpl = createPartialMock(NamespaceOperationsImpl.class, "setPropertyNoChecks",
        "checkLocalityGroups");

  }

  @Test(expected = IllegalArgumentException.class)
  public void setNullNamespaceThrowsExcept()
      throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException {
    namespaceOpsImpl.setProperty(null, Property.INSTANCE_VOLUMES.getKey(), "none");
  }

  @SuppressFBWarnings(value = "NP_NULL_PARAM_DEREF_ALL_TARGETS_DANGEROUS",
      justification = "testing null value")
  @Test(expected = IllegalArgumentException.class)
  public void setNullKeyThrowsExcept()
      throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException {
    namespaceOpsImpl.setProperty("foo", null, "none");
  }

  @Test(expected = IllegalArgumentException.class)
  public void setNullValueThrowsExcept()
      throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException {
    namespaceOpsImpl.setProperty(null, Property.INSTANCE_VOLUMES.getKey(), null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void setInvalidKeyThrowsExcept()
      throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException {
    namespaceOpsImpl.setProperty("foo", "nosuchproperty", "none");
  }

  @Test(expected = IllegalArgumentException.class)
  public void setInvalidValueThrowsExcept()
      throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException {
    namespaceOpsImpl.setProperty("foo", Property.TABLE_STORAGE_POLICY.getKey(), "SPICY");
  }

  @Test
  public void testSetDefaults()
      throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException {
    for (Property p : Property.values()) {
      // only need to test table properties
      if (p.getType().equals(PropertyType.PREFIX) || !Property.isValidTablePropertyKey(p.getKey()))
        continue;

      namespaceOpsImpl.setProperty("foo", p.getKey(), p.getDefaultValue());
    }
  }

  @Test
  public void setCustomProperty()
      throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException {
    namespaceOpsImpl.setProperty("foo", "table.custom.myproperty", "foo");
  }
}
