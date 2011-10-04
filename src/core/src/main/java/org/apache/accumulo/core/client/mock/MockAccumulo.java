package org.apache.accumulo.core.client.mock;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.iterators.conf.PerColumnIteratorConfig;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;



@SuppressWarnings("deprecation")
public class MockAccumulo {
    final Map<String, MockTable> tables = new HashMap<String, MockTable>();
    final Map<String, String> systemProperties = new HashMap<String, String>();
    Map<String, MockUser> users = new HashMap<String, MockUser>();
    
    {
        MockUser root = new MockUser("root", new byte[]{}, Constants.NO_AUTHS);
        root.permissions.add(SystemPermission.SYSTEM);
        users.put(root.name, root);
        createTable("root",Constants.METADATA_TABLE_NAME, true, TimeType.LOGICAL);
    }
    
    void setProperty(String key, String value) {
        systemProperties.put(key, value);
    }
    
    void createTable(String user, String table) {
        createTable(user, table, true, TimeType.MILLIS);
    }
    
    public void addMutation(String table, Mutation m) {
        MockTable t = tables.get(table);
        t.addMutation(m);
    }

    public BatchScanner createBatchScanner(String tableName, Authorizations authorizations) {
        return new MockBatchScanner(tables.get(tableName), authorizations);
    }

    public void addAggregators(String tableName, List<? extends PerColumnIteratorConfig> aggregators)
    {
    	tables.get(tableName).addAggregators(aggregators);
    }
    
	public void createTable(String username, String tableName, boolean useVersions, TimeType timeType) {
		MockTable t = new MockTable(useVersions, timeType);
        t.userPermissions.put(username, EnumSet.allOf(TablePermission.class));
        tables.put(tableName, t);
	}
}
