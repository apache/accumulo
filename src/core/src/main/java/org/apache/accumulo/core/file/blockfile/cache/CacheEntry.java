package org.apache.accumulo.core.file.blockfile.cache;

public class CacheEntry {
	private String fName;
	private Long hash;
	

	public CacheEntry(String name, Long time){
		this.hash = time;
		this.fName = name;
	}
	
	@Override
	public boolean equals(Object other)
	{
		return
		
		((CacheEntry)other).getName().equals(fName) 
		&& ((CacheEntry)other).getHashInfo().equals(hash)
		&& ((CacheEntry)other).getName().equals(fName) 
		&& ((CacheEntry)other).getHashInfo().equals(hash);
		
	}
	
	
	@Override
	public int hashCode()
	{
		return fName.hashCode() + hash.hashCode();
	}
	
	public String getName(){
		return fName;
	}
	
	public Long getHashInfo(){
		
		return this.hash;
	}
	public long length(){
		return fName.length() + Long.SIZE;
	}
	
}
