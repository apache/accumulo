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
package org.apache.accumulo.server.constraints;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.constraints.Constraint;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.start.classloader.AccumuloClassLoader;
import org.apache.log4j.Logger;

public class ConstraintLoader {
  private static final Logger log = Logger.getLogger(ConstraintLoader.class);
  
  public static ConstraintChecker load(TableConfiguration conf) throws IOException {
    try {
      ConstraintChecker cc = new ConstraintChecker();
      
      for (Entry<String,String> entry : conf) {
        if (entry.getKey().startsWith(Property.TABLE_CONSTRAINT_PREFIX.getKey())) {
          String className = entry.getValue();
          Class<? extends Constraint> clazz = AccumuloClassLoader.loadClass(className, Constraint.class);
          log.debug("Loaded constraint " + clazz.getName() + " for " + conf.getTableId());
          cc.addConstraint(clazz.newInstance());
        }
      }
      
      return cc;
    } catch (ClassNotFoundException e) {
      log.error(e.toString());
      throw new IOException(e);
    } catch (InstantiationException e) {
      log.error(e.toString());
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      log.error(e.toString());
      throw new IOException(e);
    }
  }
}
