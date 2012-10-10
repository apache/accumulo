package org.apache.accumulo.server.tabletserver;

import java.util.List;

import org.apache.accumulo.core.data.Mutation;

public class TabletMutations {
  private final int tid; 
  private final int seq; 
  private final List<Mutation> mutations;

  public TabletMutations(int tid, int seq, List<Mutation> mutations) {
    this.tid = tid;
    this.seq = seq;
    this.mutations = mutations;
  }

  public List<Mutation> getMutations() {
    return mutations;
  }

  public int getTid() {
    return tid;
  }
  public int getSeq() {
    return seq;
  }
  
  
  
}
