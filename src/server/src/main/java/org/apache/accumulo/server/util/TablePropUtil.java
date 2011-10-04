package org.apache.accumulo.server.util;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.zookeeper.KeeperException;


public class TablePropUtil
{
	public static boolean setTableProperty(String tableId, String property, String value)
	throws KeeperException, InterruptedException
	{
		if(!isPropertyValid(property, value))
			return false;
		
		// create the zk node for per-table properties for this table if it doesn't already exist
		String zkTablePath = getTablePath(tableId);
		ZooReaderWriter.getInstance().putPersistentData(zkTablePath, new byte[0], NodeExistsPolicy.SKIP);

		// create the zk node for this property and set it's data to the specified value
		String zPath = zkTablePath+"/"+property;
		ZooReaderWriter.getInstance().putPersistentData(zPath, value.getBytes(), NodeExistsPolicy.OVERWRITE);
        
		return true;
	}

	public static boolean isPropertyValid(String property, String value) {
		Property p = Property.getPropertyByKey(property);
		if ((p != null && !p.getType().isValidFormat(value)) || !Property.isValidTablePropertyKey(property))
			return false;
		
		return true;
	}
	
	public static void removeTableProperty(String tableId, String property)
	throws InterruptedException, KeeperException
	{
		String zPath = getTablePath(tableId)+"/"+property;
		ZooReaderWriter.getInstance().recursiveDelete(zPath, NodeMissingPolicy.SKIP);
	}
	
	private static String getTablePath(String tablename)
	{
		return ZooUtil.getRoot(HdfsZooInstance.getInstance())+Constants.ZTABLES+"/"+tablename+Constants.ZTABLE_CONF;
	}
}
