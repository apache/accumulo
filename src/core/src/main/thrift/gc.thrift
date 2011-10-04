namespace java org.apache.accumulo.core.gc.thrift

include "security.thrift"
include "cloudtrace.thrift"

struct GcCycleStats {
   1:i64 started;
   2:i64 finished;
   3:i64 candidates;
   4:i64 inUse;
   5:i64 deleted;
   6:i64 errors;
}

struct GCStatus {
   1:GcCycleStats last;
   2:GcCycleStats lastLog;
   3:GcCycleStats current;
   4:GcCycleStats currentLog;
}


service GCMonitorService {
   GCStatus getStatus(2:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials) throws (1:security.ThriftSecurityException sec);
}
