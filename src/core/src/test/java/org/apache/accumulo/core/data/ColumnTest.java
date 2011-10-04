package org.apache.accumulo.core.data;

import org.apache.accumulo.core.data.Column;

import junit.framework.TestCase;

public class ColumnTest extends TestCase{
	public void testEquals() {
		Column[] col = createColumns();
		for (int i = 0; i < col.length; i++) {
			for (int j = 0; j < col.length; j++) {
				if (i==j || (i==0 && j==1) || (i==1 && j==0))
					assertTrue(col[i].equals(col[j]));
				else
					assertFalse(col[i].equals(col[j]));
			}
		}
	}
	
	public void testCompare() {
		Column[] col = createColumns();
		for (int i = 0; i < col.length; i++) {
			for (int j = 0; j < col.length; j++) {
				if (i==j || (i==0 && j==1) || (i==1 && j==0))
					assertTrue(col[i].compareTo(col[j])==0);
				else
					assertFalse(col[i].compareTo(col[j])==0);
			}
		}
	}
	
	public void testEqualsCompare() {
		Column[] col = createColumns();
		for (int i = 0; i < col.length; i++)
			for (int j = 0; j < col.length; j++)
				assertTrue((col[i].compareTo(col[j])==0)==col[i].equals(col[j]));
	}
	
	public Column[] createColumns() {
		Column col[] = new Column[4];
		col[0] = new Column("colfam".getBytes(), "colq".getBytes(), "colv".getBytes());
		col[1] = new Column("colfam".getBytes(), "colq".getBytes(), "colv".getBytes());
		col[2] = new Column(new byte[0], new byte[0], new byte[0]);
		col[3] = new Column(null, null, null);
		return col;
	}
}
