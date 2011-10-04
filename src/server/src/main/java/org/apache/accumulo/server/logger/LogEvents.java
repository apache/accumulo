package org.apache.accumulo.server.logger;


public enum LogEvents {
	//DO NOT CHANGE ORDER OF ENUMS, ORDER IS USED IN SERIALIZATION
    OPEN,
    DEFINE_TABLET,
    MUTATION,
    MANY_MUTATIONS,
    COMPACTION_START,
    COMPACTION_FINISH;
}
