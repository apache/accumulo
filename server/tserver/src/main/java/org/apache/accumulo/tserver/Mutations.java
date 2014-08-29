package org.apache.accumulo.tserver;

import java.util.List;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.client.Durability;

public class Mutations {
  private final Durability durability;
  private final List<Mutation> mutations;

  Mutations(Durability durability, List<Mutation> mutations) {
    this.durability = durability;
    this.mutations = mutations;
  }
  public Durability getDurability() {
    return durability;
  }
  public List<Mutation> getMutations() {
    return mutations;
  }
}