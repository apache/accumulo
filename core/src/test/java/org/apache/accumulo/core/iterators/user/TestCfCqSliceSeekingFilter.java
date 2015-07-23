package org.apache.accumulo.core.iterators.user;

public class TestColumnSliceSeekingFilter extends TestColumnSlice {
    @Override
    protected Class getFilterClass() {
        return ColumnSliceSeekingFilter.class;
    }
}
