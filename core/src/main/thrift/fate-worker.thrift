namespace java org.apache.accumulo.core.fate.thrift
namespace cpp org.apache.accumulo.core.fate.thrift

struct FatePartition {
  1:string start
  2:string end
}

service FateWorker {

  list<FatePartition> getPartitions(
    1:client.TInfo tinfo,
    2:security.TCredentials credentials
  ) throws (
    1:client.ThriftSecurityException sec
  )

  bool setPartitions(
    1:client.TInfo tinfo,
    2:security.TCredentials credentials,
    3:list<FatePartition> current,
    4:list<FatePartition> desired
   ) throws (
     1:client.ThriftSecurityException sec
   )
}