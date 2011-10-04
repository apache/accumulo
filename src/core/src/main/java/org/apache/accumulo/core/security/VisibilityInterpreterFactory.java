package org.apache.accumulo.core.security;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;

public class VisibilityInterpreterFactory
{
    private static VisibilityInterpreter interpreter = null;
    
    public static VisibilityInterpreter create ()
    {
        if (interpreter == null)
        {
            throw new IllegalStateException ("ColumnVisibilityInterpreterFactory is not configured:  Interpreter is null");
        }
        return interpreter.create();
    }
    
    public static VisibilityInterpreter create (ColumnVisibility cv, Authorizations authorizations)
    {
        if (interpreter == null)
        {
            throw new IllegalStateException ("ColumnVisibilityInterpreterFactory is not configured:  Interpreter is null");
        }
        return interpreter.create(cv, authorizations);
    }
    
    public static void setInterpreter (VisibilityInterpreter interpreter)
    {
        VisibilityInterpreterFactory.interpreter = interpreter;
    }
}
