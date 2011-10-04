package org.apache.accumulo.server.logger;

import java.io.DataOutputStream;
import java.io.IOException;

public enum LogEvents {
    OPEN,
    DEFINE_TABLET,
    MUTATION,
    MANY_MUTATIONS,
    COMPACTION_START,
    COMPACTION_FINISH;
    
    public void write(DataOutputStream out) throws IOException {
        out.write((byte)this.ordinal());
    }
    
}
