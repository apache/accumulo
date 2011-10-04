/**
 * 
 */
package org.apache.accumulo.server.master;

import org.apache.accumulo.server.zookeeper.ZooLock.AsyncLockWatcher;
import org.apache.accumulo.server.zookeeper.ZooLock.LockLossReason;

class TServerLockWatcher implements AsyncLockWatcher {

	volatile boolean gotLock = false;
	volatile Exception failureException = null;

	@Override
	public void acquiredLock() {
		gotLock = true;
	}

	@Override
	public void failedToAcquireLock(Exception e) {
		failureException = e;
	}

	@Override
	public void lostLock(LockLossReason reason) {
	}

}