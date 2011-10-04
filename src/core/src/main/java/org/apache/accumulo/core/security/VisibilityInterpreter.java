package org.apache.accumulo.core.security;

import java.io.Serializable;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;


public interface VisibilityInterpreter extends Serializable
{
    public abstract String getAbbreviatedValue ();
    
    public abstract String getFullValue ();
    
    public abstract void merge(ColumnVisibility other, Authorizations authorizations);

    public abstract void merge(VisibilityInterpreter other);
    
    // Factory type method that can be used from an instance
    public abstract VisibilityInterpreter create ();
    
    public abstract VisibilityInterpreter create (ColumnVisibility visibility, Authorizations authorizations);
}
