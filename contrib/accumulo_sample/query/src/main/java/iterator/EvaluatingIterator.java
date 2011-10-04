package iterator;

import parser.EventFields;
import parser.EventFields.FieldValue;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.ColumnVisibility;

public class EvaluatingIterator extends AbstractEvaluatingIterator {

	public static final String NULL_BYTE_STRING = "\u0000";

	public EvaluatingIterator() {
		super();
	}

	public EvaluatingIterator(AbstractEvaluatingIterator other, IteratorEnvironment env) {
		super(other, env);
	}

	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		return new EvaluatingIterator(this, env);
	}

	@Override
	public PartialKey getKeyComparator() {
		return PartialKey.ROW_COLFAM;
	}

	@Override
	public Key getReturnKey(Key k) {
		//If we were using column visibility, then we would get the merged visibility here and use it in the key.
		//Remove the COLQ from the key and use the combined visibility
		Key r = new Key(k.getRowData().getBackingArray(), k.getColumnFamilyData().getBackingArray(),
				NULL_BYTE, k.getColumnVisibility().getBytes(), k.getTimestamp(), k.isDeleted(), false);
		return r;
	}

	@Override
	public void fillMap(EventFields event, Key key, Value value) {
		//If we were using column visibility, we would have to merge them here.

		//Pull the datatype from the colf in case we need to do anything datatype specific.
		//String colf = key.getColumnFamily().toString();
		//String datatype = colf.substring(0, colf.indexOf(NULL_BYTE_STRING));

		//For the partitioned table, the field name and field value are stored in the column qualifier
		//separated by a \0.
		String colq = key.getColumnQualifier().toString();//.toLowerCase();
		int idx = colq.indexOf(NULL_BYTE_STRING);
		String fieldName = colq.substring(0,idx);
		String fieldValue = colq.substring(idx+1);

		event.put(fieldName, new FieldValue(new ColumnVisibility(key.getColumnVisibility().getBytes()), fieldValue.getBytes()));
	}


	/**
	 * Don't accept this key if the colf starts with 'fi'
	 */
	@Override
	public boolean isKeyAccepted(Key key) {
		if (key.getColumnFamily().toString().startsWith("fi")) {
			return false;
		}
		return true;
	}

}
