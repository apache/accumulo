package org.apache.accumulo.core.iterators.conf;

import org.apache.accumulo.core.iterators.Combiner;
import org.apache.hadoop.io.Text;


/**
 * @deprecated since 1.4
 * @see Combiner#addColumn(Text, Text, accumulo.core.client.IteratorSetting)
 */
public class PerColumnIteratorConfig {

	private String parameter;
	private Text colq;
	private Text colf;

	public PerColumnIteratorConfig(Text columnFamily, String parameter){
		this.colf = columnFamily;
		this.colq = null;
		this.parameter = parameter;
	}
	
	public PerColumnIteratorConfig(Text columnFamily, Text columnQualifier, String parameter){
		this.colf = columnFamily;
		this.colq = columnQualifier;
		this.parameter = parameter;
	}
	
	public Text getColumnFamily() {
		return colf;
	}
	
	public Text getColumnQualifier() {
		return colq;
	}
	
	public String encodeColumns() {
		return encodeColumns(this);
	}

	public String getClassName() {
		return parameter;
	}
	
	private static String encodeColumns(PerColumnIteratorConfig pcic){
		return encodeColumns(pcic.colf, pcic.colq);
	}
	
	public static String encodeColumns(Text columnFamily, Text columnQualifier) {
		StringBuilder sb = new StringBuilder();
		
		encode(sb, columnFamily);
		if(columnQualifier != null){
			sb.append(':');
			encode(sb, columnQualifier);
		}
		
		return sb.toString();
	}
	
	private static void encode(StringBuilder sb, Text t) {
		for(int i = 0; i < t.getLength(); i++){
			int b = (0xff & t.getBytes()[i]);
			
			//very inefficient code
			if((b >= 'a' && b <= 'z') || (b >='A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_' || b=='-'){
				sb.append((char)b);
			}else{
				sb.append('%');
				sb.append(String.format("%02x", b));
			}
		}
	}
	
	public static PerColumnIteratorConfig decodeColumns(String columns, String className){
		String[] cols = columns.split(":");
		
		if(cols.length == 1){
			return new PerColumnIteratorConfig(decode(cols[0]), className);
		}else if(cols.length == 2){
			return new PerColumnIteratorConfig(decode(cols[0]), decode(cols[1]), className);
		}else{
			throw new IllegalArgumentException(columns);
		}
	}

	private static Text decode(String s) {
		Text t = new Text();
		
		byte[] sb = s.getBytes();
		
		//very inefficient code
		for(int i = 0; i < sb.length; i++){
			if(sb[i] != '%'){
				t.append(new byte[]{sb[i]}, 0, 1);
			}else{
				byte hex[] = new byte[]{sb[++i],sb[++i]};
				String hs = new String(hex);
				int b = Integer.parseInt(hs, 16);
				t.append(new byte[]{(byte)b}, 0, 1);
			}
		}
		
		return t;
	}
}
