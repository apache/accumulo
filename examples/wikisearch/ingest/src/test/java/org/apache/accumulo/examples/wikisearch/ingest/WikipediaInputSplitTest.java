package org.apache.accumulo.examples.wikisearch.ingest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import junit.framework.Assert;

import org.apache.accumulo.examples.wikisearch.ingest.WikipediaInputFormat.WikipediaInputSplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Test;

public class WikipediaInputSplitTest {
  @Test
  public void testSerialization() throws IOException
  {
    Path testPath = new Path("/foo/bar");
    String [] hosts = new String [2];
    hosts[0] = "abcd";
    hosts[1] = "efgh";
    FileSplit fSplit = new FileSplit(testPath,1,2,hosts);
    WikipediaInputSplit split = new WikipediaInputSplit(fSplit,7);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(baos);
    split.write(out);
    out.close();    
    baos.close();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInput in = new ObjectInputStream(bais);
    
    WikipediaInputSplit split2 = new WikipediaInputSplit();
    split2.readFields(in);
    Assert.assertTrue(bais.available() == 0);
    bais.close();
    
    Assert.assertTrue(split.getPartition() == split2.getPartition());
    
    FileSplit fSplit2 = split2.getFileSplit();
    Assert.assertTrue(fSplit.getPath().equals(fSplit2.getPath()));
    Assert.assertTrue(fSplit.getStart() == fSplit2.getStart());
    Assert.assertTrue(fSplit.getLength() == fSplit2.getLength());

    String [] hosts2 = fSplit2.getLocations();
    Assert.assertEquals(hosts.length, hosts2.length);
    for(int i = 0; i < hosts.length; i++)
    {
      Assert.assertEquals(hosts[i],hosts2[i]);
    }
  }
}
