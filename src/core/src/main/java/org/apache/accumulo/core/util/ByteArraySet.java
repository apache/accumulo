package org.apache.accumulo.core.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

public class ByteArraySet extends TreeSet<byte[]> {
  
  private static final long serialVersionUID = 1L;
  
  public ByteArraySet() {
    super(new ByteArrayComparator());
  }
  
  public ByteArraySet(Collection<? extends byte[]> c) {
    this();
    addAll(c);
  }
  
  public static ByteArraySet fromStrings(Collection<String> c) {
    List<byte[]> lst = new ArrayList<byte[]>();
    for (String s : c)
      lst.add(s.getBytes());
    return new ByteArraySet(lst);
  }
  
  public static ByteArraySet fromStrings(String... c) {
    return ByteArraySet.fromStrings(Arrays.asList(c));
  }
  
  public List<byte[]> toList() {
    return new ArrayList<byte[]>(this);
  }
  
}
