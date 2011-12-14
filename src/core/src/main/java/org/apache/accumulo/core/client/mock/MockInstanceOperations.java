/**
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
package org.apache.accumulo.core.client.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.ActiveScan;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.start.classloader.AccumuloClassLoader;

/**
 * 
 */
public class MockInstanceOperations implements InstanceOperations {
  MockAccumulo acu;
  
  /**
   * @param acu
   */
  public MockInstanceOperations(MockAccumulo acu) {
    this.acu = acu;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.accumulo.core.client.admin.InstanceOperations#setProperty(java.lang.String, java.lang.String)
   */
  @Override
  public void setProperty(String property, String value) throws AccumuloException, AccumuloSecurityException {
    acu.setProperty(property, value);
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.accumulo.core.client.admin.InstanceOperations#removeProperty(java.lang.String)
   */
  @Override
  public void removeProperty(String property) throws AccumuloException, AccumuloSecurityException {
    acu.removeProperty(property);
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.accumulo.core.client.admin.InstanceOperations#getSystemConfiguration()
   */
  @Override
  public Map<String,String> getSystemConfiguration() throws AccumuloException, AccumuloSecurityException {
    return acu.systemProperties;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.accumulo.core.client.admin.InstanceOperations#getSiteConfiguration()
   */
  @Override
  public Map<String,String> getSiteConfiguration() throws AccumuloException, AccumuloSecurityException {
    return acu.systemProperties;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.accumulo.core.client.admin.InstanceOperations#getTabletServers()
   */
  @Override
  public List<String> getTabletServers() {
    return new ArrayList<String>();
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.accumulo.core.client.admin.InstanceOperations#getActiveScans(java.lang.String)
   */
  @Override
  public List<ActiveScan> getActiveScans(String tserver) throws AccumuloException, AccumuloSecurityException {
    return new ArrayList<ActiveScan>();
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.accumulo.core.client.admin.InstanceOperations#testClassLoad(java.lang.String, java.lang.String)
   */
  @Override
  public boolean testClassLoad(String className, String asTypeName) throws AccumuloException, AccumuloSecurityException {
    try {
      AccumuloClassLoader.loadClass(className, Class.forName(asTypeName));
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }
}
