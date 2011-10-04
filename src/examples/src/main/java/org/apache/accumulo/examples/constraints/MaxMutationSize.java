package org.apache.accumulo.examples.constraints;

import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.constraints.Constraint;
import org.apache.accumulo.core.data.Mutation;


/**
 * Ensure that mutations are a reasonable size: we must be able to fit several in memory at a time. 
 *
 */
public class MaxMutationSize implements Constraint {
    static final long MAX_SIZE = Runtime.getRuntime().maxMemory() >> 8;
    static final List<Short> empty = Collections.emptyList();
    static final List<Short> violations = Collections.singletonList(new Short((short)0));
    
    @Override
    public String getViolationDescription(short violationCode) {
        return String.format("mutation exceeded maximum size of %d", MAX_SIZE);
    }

    @Override
    public List<Short> check(Environment env, Mutation mutation) {
        if (mutation.estimatedMemoryUsed() < MAX_SIZE)
            return empty;
        return violations;
    }
}
