package org.apache.accumulo.server.tabletserver;

import java.util.Random;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.test.FastFormat;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;



/**
 * Allocates unique names for a accumulo instance.  The names are unique for the lifetime of the instance. 
 * 
 * This is useful for filenames because it makes caching easy.
 * 
 */

public class UniqueNameAllocator {
	private long next = 0;
	private long maxAllocated = 0;
	private String nextNamePath;
	private Random rand;

	private UniqueNameAllocator(){
		nextNamePath = Constants.ZROOT + "/" + HdfsZooInstance.getInstance().getInstanceID() + Constants.ZNEXT_FILE;
		rand = new Random();
	}
	
	public synchronized String getNextName() {

		while (next >= maxAllocated) {
			final int allocate = 100 + rand.nextInt(100);

			try {
				byte[] max = ZooReaderWriter.getInstance().mutate(nextNamePath, null, ZooUtil.PRIVATE,
						new ZooReaderWriter.Mutator() {
							public byte[] mutate(byte[] currentValue)
									throws Exception {
								long l = Long.parseLong(
										new String(currentValue),
										Character.MAX_RADIX);
								l += allocate;
								return Long.toString(l, Character.MAX_RADIX)
										.getBytes();
							}
						});

				maxAllocated = Long.parseLong(new String(max),
						Character.MAX_RADIX);
				next = maxAllocated - allocate;

			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		
		return new String(FastFormat.toZeroPaddedString(next++, 7, Character.MAX_RADIX, new byte[0]));
	}
	
	private static UniqueNameAllocator instance = null;
	
	public static synchronized UniqueNameAllocator getInstance(){
		if(instance == null)
			instance = new UniqueNameAllocator();
		
		return instance;
	}

}
