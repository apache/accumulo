package org.apache.accumulo.server.tabletserver.log;

import org.apache.accumulo.core.data.Mutation;

public interface MutationReceiver {
	void receive(Mutation m);
}
