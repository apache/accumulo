namespace java org.apache.accumulo.core.data.thrift

typedef i64 ScanID
typedef i64 UpdateID

struct TKey {
	1:binary row;
	2:binary colFamily;
	3:binary colQualifier;
	4:binary colVisibility;
	5:i64 timestamp
}

struct TColumn {
	1:binary columnFamily,
	2:binary columnQualifier,
	3:binary columnVisibility
}

struct TMutation {
	1:binary row,
	2:binary data,
	3:list<binary> values
	4:i32 entries
}

struct TKeyExtent {
	1:binary table,
	2:binary endRow,
	3:binary prevEndRow
}

struct TKeyValue {
	1:TKey key,
	2:binary value
}

struct ScanResult {
	1:list<TKeyValue> results,
	2:bool more
}

struct TRange {
	1:TKey start,
	2:TKey stop,
	3:bool startKeyInclusive,
	4:bool stopKeyInclusive,
	5:bool infiniteStartKey,
	6:bool infiniteStopKey
}

typedef map<TKeyExtent, list<TRange>> ScanBatch
	
struct MultiScanResult {
	1:list<TKeyValue> results,
	2:ScanBatch failures,
	3:list<TKeyExtent> fullScans,
	4:TKeyExtent partScan,
	5:TKey partNextKey,
	6:bool partNextKeyInclusive,
	7:bool more
}

struct InitialScan {
	1:ScanID scanID,
	2:ScanResult result
}

struct InitialMultiScan {
	1:ScanID scanID,
	2:MultiScanResult result
}

struct IterInfo {
	1:i32 priority,
	2:string className,
	3:string iterName
}

struct TConstraintViolationSummary {
	1:string constrainClass,
	2:i16 violationCode,
	3:string violationDescription,
	4:i64 numberOfViolatingMutations
}

struct UpdateErrors {
	1:map<TKeyExtent, i64> failedExtents,
	2:list<TConstraintViolationSummary> violationSummaries,
	3:list<TKeyExtent> authorizationFailures
}

struct MapFileInfo {
	1:i64 estimatedSize
}

typedef map<TKeyExtent,list<TMutation>> UpdateBatch

typedef map<TKeyExtent, map<string, MapFileInfo>> TabletFiles
