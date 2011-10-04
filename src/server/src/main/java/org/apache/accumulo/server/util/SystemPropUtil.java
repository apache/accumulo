package org.apache.accumulo.server.util;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.impl.HdfsZooInstance;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.zookeeper.KeeperException;


public class SystemPropUtil
{
	public static boolean setSystemProperty(String property, String value)
	throws KeeperException, InterruptedException
	{
		Property p = Property.getPropertyByKey(property);
		if ((p != null && !p.getType().isValidFormat(value)) || !Property.isValidZooPropertyKey(property))
			return false;
		
		// create the zk node for this property and set it's data to the specified value
		String zPath = ZooUtil.getRoot(HdfsZooInstance.getInstance())+Constants.ZCONFIG+"/"+property;
        ZooUtil.putPersistentData(zPath, value.getBytes(), NodeExistsPolicy.OVERWRITE);
        
		return true;
	}
	
	public static void removeSystemProperty(String property)
	throws InterruptedException, KeeperException
	{
		String zPath = ZooUtil.getRoot(HdfsZooInstance.getInstance())+Constants.ZCONFIG+"/"+property;
		ZooSession.getSession().delete(zPath, -1);
	}
}
