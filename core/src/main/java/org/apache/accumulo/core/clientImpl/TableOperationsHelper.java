/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.clientImpl;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.accumulo.core.util.Validators.EXISTING_TABLE_NAME;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;

public abstract class TableOperationsHelper implements TableOperations {

  @Override
  public void attachIterator(String tableName, IteratorSetting setting)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    attachIterator(tableName, setting, EnumSet.allOf(IteratorScope.class));
  }

  @Override
  public void attachIterator(String tableName, IteratorSetting setting,
      EnumSet<IteratorScope> scopes)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    EXISTING_TABLE_NAME.validate(tableName);
    checkArgument(setting != null, "setting is null");
    checkArgument(scopes != null, "scopes is null");
    checkIteratorConflicts(tableName, setting, scopes);

    for (IteratorScope scope : scopes) {
      String root = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX,
          scope.name().toLowerCase(), setting.getName());
      this.modifyProperties(tableName, properties -> {
        for (Entry<String,String> prop : setting.getOptions().entrySet()) {
          properties.put(root + ".opt." + prop.getKey(), prop.getValue());
        }
        properties.put(root, setting.getPriority() + "," + setting.getIteratorClass());
      });
    }
  }

  @Override
  public void removeIterator(String tableName, String name, EnumSet<IteratorScope> scopes)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    EXISTING_TABLE_NAME.validate(tableName);

    Map<String,String> copy = Map.copyOf(this.getConfiguration(tableName));
    for (IteratorScope scope : scopes) {
      String root = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX,
          scope.name().toLowerCase(), name);
      this.modifyProperties(tableName,
          properties -> copy.keySet().stream()
              .filter(prop -> prop.equals(root) || prop.startsWith(root + ".opt."))
              .forEach(properties::remove));
    }
  }

  @Override
  public IteratorSetting getIteratorSetting(String tableName, String name, IteratorScope scope)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    EXISTING_TABLE_NAME.validate(tableName);
    checkArgument(name != null, "name is null");
    checkArgument(scope != null, "scope is null");

    int priority = -1;
    String classname = null;
    Map<String,String> settings = new HashMap<>();

    String root =
        String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scope.name().toLowerCase(), name);
    String opt = root + ".opt.";
    for (Entry<String,String> property : this.getProperties(tableName)) {
      if (property.getKey().equals(root)) {
        String[] parts = property.getValue().split(",");
        if (parts.length != 2) {
          throw new AccumuloException("Bad value for iterator setting: " + property.getValue());
        }
        priority = Integer.parseInt(parts[0]);
        classname = parts[1];
      } else if (property.getKey().startsWith(opt)) {
        settings.put(property.getKey().substring(opt.length()), property.getValue());
      }
    }
    if (priority <= 0 || classname == null) {
      return null;
    }
    return new IteratorSetting(priority, name, classname, settings);
  }

  @Override
  public Map<String,EnumSet<IteratorScope>> listIterators(String tableName)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    EXISTING_TABLE_NAME.validate(tableName);

    Map<String,EnumSet<IteratorScope>> result = new TreeMap<>();
    for (Entry<String,String> property : this.getProperties(tableName)) {
      String name = property.getKey();
      String[] parts = name.split("\\.");
      if (parts.length == 4) {
        if (parts[0].equals("table") && parts[1].equals("iterator")) {
          IteratorScope scope = IteratorScope.valueOf(parts[2]);
          if (!result.containsKey(parts[3])) {
            result.put(parts[3], EnumSet.noneOf(IteratorScope.class));
          }
          result.get(parts[3]).add(scope);
        }
      }
    }
    return result;
  }

  public static void checkIteratorConflicts(Map<String,String> props, IteratorSetting setting,
      EnumSet<IteratorScope> scopes) throws AccumuloException {
    checkArgument(setting != null, "setting is null");
    checkArgument(scopes != null, "scopes is null");
    for (IteratorScope scope : scopes) {
      String scopeStr =
          String.format("%s%s", Property.TABLE_ITERATOR_PREFIX, scope.name().toLowerCase());
      String nameStr = String.format("%s.%s", scopeStr, setting.getName());
      String optStr = String.format("%s.opt.", nameStr);
      Map<String,String> optionConflicts = new TreeMap<>();
      for (Entry<String,String> property : props.entrySet()) {
        if (property.getKey().startsWith(scopeStr)) {
          if (property.getKey().equals(nameStr)) {
            throw new AccumuloException(new IllegalArgumentException("iterator name conflict for "
                + setting.getName() + ": " + property.getKey() + "=" + property.getValue()));
          }
          if (property.getKey().startsWith(optStr)) {
            optionConflicts.put(property.getKey(), property.getValue());
          }
          if (property.getKey().contains(".opt.")) {
            continue;
          }
          String[] parts = property.getValue().split(",");
          if (parts.length != 2) {
            throw new AccumuloException("Bad value for existing iterator setting: "
                + property.getKey() + "=" + property.getValue());
          }
          try {
            if (Integer.parseInt(parts[0]) == setting.getPriority()) {
              throw new AccumuloException(new IllegalArgumentException(
                  "iterator priority conflict: " + property.getKey() + "=" + property.getValue()));
            }
          } catch (NumberFormatException e) {
            throw new AccumuloException("Bad value for existing iterator setting: "
                + property.getKey() + "=" + property.getValue());
          }
        }
      }
      if (!optionConflicts.isEmpty()) {
        throw new AccumuloException(new IllegalArgumentException(
            "iterator options conflict for " + setting.getName() + ": " + optionConflicts));
      }
    }
  }

  @Override
  public void checkIteratorConflicts(String tableName, IteratorSetting setting,
      EnumSet<IteratorScope> scopes) throws AccumuloException, TableNotFoundException {
    EXISTING_TABLE_NAME.validate(tableName);

    Map<String,String> iteratorProps = Map.copyOf(this.getConfiguration(tableName));
    checkIteratorConflicts(iteratorProps, setting, scopes);
  }

  @Override
  public int addConstraint(String tableName, String constraintClassName)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    EXISTING_TABLE_NAME.validate(tableName);

    TreeSet<Integer> constraintNumbers = new TreeSet<>();
    TreeMap<String,Integer> constraintClasses = new TreeMap<>();
    int i;
    for (Entry<String,String> property : this.getProperties(tableName)) {
      if (property.getKey().startsWith(Property.TABLE_CONSTRAINT_PREFIX.toString())) {
        try {
          i = Integer.parseInt(
              property.getKey().substring(Property.TABLE_CONSTRAINT_PREFIX.toString().length()));
        } catch (NumberFormatException e) {
          throw new AccumuloException("Bad key for existing constraint: " + property);
        }
        constraintNumbers.add(i);
        constraintClasses.put(property.getValue(), i);
      }
    }
    i = 1;
    while (constraintNumbers.contains(i)) {
      i++;
    }
    if (constraintClasses.containsKey(constraintClassName)) {
      throw new AccumuloException("Constraint " + constraintClassName + " already exists for table "
          + tableName + " with number " + constraintClasses.get(constraintClassName));
    }
    this.setProperty(tableName, Property.TABLE_CONSTRAINT_PREFIX.toString() + i,
        constraintClassName);
    return i;
  }

  @Override
  public void removeConstraint(String tableName, int number)
      throws AccumuloException, AccumuloSecurityException {
    this.removeProperty(tableName, Property.TABLE_CONSTRAINT_PREFIX.toString() + number);
  }

  @Override
  public Map<String,Integer> listConstraints(String tableName)
      throws AccumuloException, TableNotFoundException {
    EXISTING_TABLE_NAME.validate(tableName);

    Map<String,Integer> constraints = new TreeMap<>();
    for (Entry<String,String> property : this.getProperties(tableName)) {
      if (property.getKey().startsWith(Property.TABLE_CONSTRAINT_PREFIX.toString())) {
        if (constraints.containsKey(property.getValue())) {
          throw new AccumuloException("Same constraint configured twice: " + property.getKey() + "="
              + Property.TABLE_CONSTRAINT_PREFIX + constraints.get(property.getValue()) + "="
              + property.getKey());
        }
        try {
          constraints.put(property.getValue(), Integer.parseInt(
              property.getKey().substring(Property.TABLE_CONSTRAINT_PREFIX.toString().length())));
        } catch (NumberFormatException e) {
          throw new AccumuloException("Bad key for existing constraint: " + property);
        }
      }
    }
    return constraints;
  }
}
