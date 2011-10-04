namespace java cloudtrace.thrift

struct RemoteSpan {
   1:string sender,
   2:string svc, 
   3:i64 traceId, 
   4:i64 spanId, 
   5:i64 parentId, 
   6:i64 start, 
   7:i64 stop, 
   8:string description, 
   9:map<string, string> data
}

struct TInfo {
   1:i64 traceId,
   2:i64 parentId,
}


service SpanReceiver {
   oneway void span(1:RemoteSpan span);
}

// used for testing trace
service TestService {
   bool checkTrace(1:TInfo tinfo, 2:string message),
}
