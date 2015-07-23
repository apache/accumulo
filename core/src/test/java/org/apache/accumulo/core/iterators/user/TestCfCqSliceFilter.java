package org.apache.accumulo.core.iterators.user;

public class TestColumnSliceFilter extends TestColumnSlice {
    @Override
    protected Class getFilterClass() {
        return ColumnSliceFilter.class;
    }
}
