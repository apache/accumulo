package org.apache.accumulo.core.security;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public enum SystemPermission
{
	// One can add new permissions, with new numbers, but please don't change or use numbers previously assigned
	GRANT((byte)0),
	CREATE_TABLE((byte)1),
	DROP_TABLE((byte)2),
	ALTER_TABLE((byte)3),
	CREATE_USER((byte)4),
	DROP_USER((byte)5),
	ALTER_USER((byte)6),
	SYSTEM((byte)7);
	
	private byte permID;

	private static HashMap<Byte, SystemPermission> mapping;
	static {
		mapping = new HashMap<Byte, SystemPermission>(SystemPermission.values().length);
		for (SystemPermission perm : SystemPermission.values())
			mapping.put(perm.permID, perm);
	}

	private SystemPermission(byte id)
	{ this.permID = id; }
	
	public byte getId()
	{ return this.permID; }
	
	public static List<String> printableValues() {
		SystemPermission[] a = SystemPermission.values();

		List<String> list = new ArrayList<String>(a.length);

		for (SystemPermission p : a)
    		list.add("System." + p);
    		
		return list;
	}
	
	public static SystemPermission getPermissionById(byte id)
	{
		if (mapping.containsKey(id))
			return mapping.get(id);
		throw new IndexOutOfBoundsException("No such permission");
	}
}
