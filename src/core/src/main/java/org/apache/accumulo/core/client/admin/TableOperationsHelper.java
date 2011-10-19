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
package org.apache.accumulo.core.client.admin;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;

public abstract class TableOperationsHelper implements TableOperations {
    
    @Override
    public void attachIterator(String tableName, IteratorSetting setting) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        removeIterator(tableName, setting.getName());
        for (Entry<IteratorScope,Map<String,String>> entry : setting.getOptionsByScope().entrySet()) {
            String root = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, entry.getKey().name().toLowerCase(), setting.getName());
            this.setProperty(tableName, root, setting.getPriority() + "," + setting.getIteratorClass());
            for (Entry<String,String> prop : entry.getValue().entrySet()) {
                this.setProperty(tableName, root + ".opt." + prop.getKey(), prop.getValue());
            }
        }
    }
    
    @Override
    public void removeIterator(String tableName, String name) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        Map<String,String> copy = new HashMap<String,String>();
        for (Entry<String,String> property : this.getProperties(tableName)) {
            copy.put(property.getKey(), property.getValue());
        }
        for (Entry<String,String> property : copy.entrySet()) {
            for (IteratorScope scope : IteratorScope.values()) {
                String root = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scope.name().toLowerCase(), name);
                if (property.getKey().equals(root) || property.getKey().startsWith(root + ".opt.")) this.removeProperty(tableName, property.getKey());
            }
        }
    }
    
    @Override
    public IteratorSetting getIterator(String tableName, String name) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        int priority = -1;
        String classname = null;
        EnumMap<IteratorScope,Map<String,String>> settings = new EnumMap<IteratorScope,Map<String,String>>(IteratorScope.class);
        
        for (Entry<String,String> property : this.getProperties(tableName)) {
            for (IteratorScope scope : IteratorScope.values()) {
                String root = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scope.name().toLowerCase(), name);
                String opt = root + ".opt.";
                if (property.getKey().equals(root)) {
                    String parts[] = property.getValue().split(",");
                    if (parts.length != 2) {
                        throw new AccumuloException("Bad value for iterator setting: " + property.getValue());
                    }
                    priority = Integer.parseInt(parts[0]);
                    classname = parts[1];
                    if (!settings.containsKey(scope)) settings.put(scope, new HashMap<String,String>());
                } else if (property.getKey().startsWith(opt)) {
                    if (!settings.containsKey(scope)) settings.put(scope, new HashMap<String,String>());
                    settings.get(scope).put(property.getKey().substring(opt.length()), property.getValue());
                }
            }
        }
        if (priority < 0 || classname == null) {
            return null;
        }
        IteratorSetting result = new IteratorSetting(priority, name, classname);
        for (Entry<IteratorScope,Map<String,String>> entry : settings.entrySet()) {
            result.addOptions(entry.getKey(), entry.getValue());
        }
        return result;
    }
    
    @Override
    public Set<String> getIterators(String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        Set<String> result = new HashSet<String>();
        Set<String> lifecycles = new HashSet<String>();
        for (IteratorScope scope : IteratorScope.values())
            lifecycles.add(scope.name().toLowerCase());
        for (Entry<String,String> property : this.getProperties(tableName)) {
            String name = property.getKey();
            String[] parts = name.split("\\.");
            if (parts.length == 4) {
                if (parts[0].equals("table") && parts[1].equals("iterator") && lifecycles.contains(parts[2])) result.add(parts[3]);
            }
        }
        return result;
    }
}
