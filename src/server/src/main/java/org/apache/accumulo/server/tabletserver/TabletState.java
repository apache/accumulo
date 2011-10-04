package org.apache.accumulo.server.tabletserver;

import org.apache.accumulo.core.data.KeyExtent;

public interface TabletState {
	KeyExtent getExtent();
	long getLastCommitTime();
	long getMemTableSize();
	long getMinorCompactingMemTableSize();
}
