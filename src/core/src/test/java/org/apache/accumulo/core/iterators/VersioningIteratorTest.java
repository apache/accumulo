package org.apache.accumulo.core.iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeMap;
import java.util.Map.Entry;

import junit.framework.TestCase;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.VersioningIterator;
import org.apache.accumulo.core.iterators.aggregation.LongSummation;
import org.apache.hadoop.io.Text;


public class VersioningIteratorTest extends TestCase {
	// add test for seek function
	private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();
	
	void createTestData(TreeMap<Key, Value> tm, Text colf, Text colq){
		for (int i = 0; i < 2; i++) {
			for (long j = 0; j < 20; j++) {
				Key k = new Key(new Text(String.format("%03d", i)), colf , colq, j);
				tm.put(k, new Value(LongSummation.longToBytes(j)));
			}
		}
		
		assertTrue("Initial size was "+tm.size(),tm.size()==40);
	}
	
	TreeMap<Key, Value> iteratorOverTestData(VersioningIterator it) throws IOException{
		TreeMap<Key, Value> tmOut = new TreeMap<Key, Value>();
		while (it.hasTop()) {
			tmOut.put(it.getTopKey(), it.getTopValue());
			it.next();
		}
		
		return tmOut;
	}
	
	public void test1() {
		Text colf = new Text("a");
		Text colq = new Text("b");
		
		TreeMap<Key, Value> tm = new TreeMap<Key, Value>();
		
		createTestData(tm, colf, colq);
		
		try {
			VersioningIterator it = new VersioningIterator(new SortedMapIterator(tm), 3);
			TreeMap<Key, Value> tmOut = iteratorOverTestData(it);
			
			for (Entry<Key, Value> e : tmOut.entrySet()) {
				assertTrue(e.getValue().get().length==8);
				assertTrue(16<LongSummation.bytesToLong(e.getValue().get()));
			}
			assertTrue("size after keeping 3 versions was "+tmOut.size(),tmOut.size()==6);
		}
		catch (IOException e) {
			assertFalse(true);
		} catch (Exception e) {
			e.printStackTrace();
			assertFalse(true);
		}
	}
	
	public void test2(){
		Text colf = new Text("a");
		Text colq = new Text("b");
		
		TreeMap<Key, Value> tm = new TreeMap<Key, Value>();
		
		createTestData(tm, colf, colq);
		
		try {
			VersioningIterator it = new VersioningIterator(new SortedMapIterator(tm), 3);
			
			//after doing this seek, should only get two keys for row 1
			//since we are seeking to the middle of the most recent
			//three keys
			Key seekKey = new Key(new Text(String.format("%03d", 1)),colf, colq, 18);
			it.seek(new Range(seekKey, null), EMPTY_COL_FAMS, false);

			TreeMap<Key, Value> tmOut = iteratorOverTestData(it);
			
			for (Entry<Key, Value> e : tmOut.entrySet()) {
				assertTrue(e.getValue().get().length==8);
				assertTrue(16<LongSummation.bytesToLong(e.getValue().get()));
			}
			assertTrue("size after keeping 2 versions was "+tmOut.size(),tmOut.size()==2);
		}
		catch (IOException e) {
			assertFalse(true);
		} catch (Exception e) {
			e.printStackTrace();
			assertFalse(true);
		}
	}
	
	public void test3(){
		Text colf = new Text("a");
		Text colq = new Text("b");
		
		TreeMap<Key, Value> tm = new TreeMap<Key, Value>();
		
		createTestData(tm, colf, colq);
		
		try {
			VersioningIterator it = new VersioningIterator(new SortedMapIterator(tm), 3);

			//after doing this seek, should get zero keys for row 1
			Key seekKey = new Key(new Text(String.format("%03d", 1)),colf, colq, 15);
			it.seek(new Range(seekKey, null), EMPTY_COL_FAMS, false);

			TreeMap<Key, Value> tmOut = iteratorOverTestData(it);
			
			for (Entry<Key, Value> e : tmOut.entrySet()) {
				assertTrue(e.getValue().get().length==8);
				assertTrue(16<LongSummation.bytesToLong(e.getValue().get()));
			}

			assertTrue("size after seeking past versions was "+tmOut.size(),tmOut.size()==0);
			
			
			//after doing this seek, should get zero keys for row 0 and 3 keys for row 1
			seekKey = new Key(new Text(String.format("%03d", 0)),colf, colq, 15);
			it.seek(new Range(seekKey, null), EMPTY_COL_FAMS, false);

			tmOut = iteratorOverTestData(it);
			
			for (Entry<Key, Value> e : tmOut.entrySet()) {
				assertTrue(e.getValue().get().length==8);
				assertTrue(16<LongSummation.bytesToLong(e.getValue().get()));
			}

			assertTrue("size after seeking past versions was "+tmOut.size(),tmOut.size()==3);
			
		}
		catch (IOException e) {
			assertFalse(true);
		} catch (Exception e) {
			e.printStackTrace();
			assertFalse(true);
		}
	}
	
	public void test4() {
		Text colf = new Text("a");
		Text colq = new Text("b");
		
		TreeMap<Key, Value> tm = new TreeMap<Key, Value>();
		
		createTestData(tm, colf, colq);
		
		for(int i = 1; i <=30; i++){
			try {
				VersioningIterator it = new VersioningIterator(new SortedMapIterator(tm), i);
				TreeMap<Key, Value> tmOut = iteratorOverTestData(it);

				assertTrue("size after keeping "+i+" versions was "+tmOut.size(),tmOut.size()==Math.min(40, 2*i));
			}
			catch (IOException e) {
				assertFalse(true);
			} catch (Exception e) {
				e.printStackTrace();
				assertFalse(true);
			}
		}
	}
	
	
	public void test5() throws IOException {
		Text colf = new Text("a");
		Text colq = new Text("b");
		
		TreeMap<Key, Value> tm = new TreeMap<Key, Value>();
		
		createTestData(tm, colf, colq);
		
		VersioningIterator it = new VersioningIterator(new SortedMapIterator(tm), 3);
		
		Key seekKey = new Key(new Text(String.format("%03d", 1)),colf, colq, 19);
		it.seek(new Range(seekKey, false, null, true), EMPTY_COL_FAMS, false);
		
		assertTrue(it.hasTop());
		assertTrue(it.getTopKey().getTimestamp() == 18);
		
	}

}
