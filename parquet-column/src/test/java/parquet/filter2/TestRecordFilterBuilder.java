package parquet.filter2;

import java.util.Arrays;

import org.junit.Test;

import parquet.column.ColumnDescriptor;
import parquet.column.ColumnReader;
import parquet.filter2.FilterPredicateOperators.Column;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static parquet.filter2.Filter.eq;
import static parquet.filter2.Filter.intColumn;
import static parquet.filter2.Filter.ltEq;
import static parquet.filter2.Filter.or;

public class TestRecordFilterBuilder {
  @Test
  public void test() {
    Column<Integer> foo = intColumn("foo.bar");
    FilterPredicate p = or(eq(foo, 12), ltEq(foo, 3));

    ColumnReader column = createStrictMock(ColumnReader.class);
    ColumnDescriptor desc = new ColumnDescriptor(new String[] {"foo", "bar"}, PrimitiveTypeName.INT32, 0, 0);
    expect(column.getDescriptor()).andStubReturn(desc);

    expect(column.getInteger()).andReturn(100);
    expect(column.getInteger()).andReturn(100);
    expect(column.getInteger()).andReturn(12);
    expect(column.getInteger()).andReturn(3);
    expect(column.getInteger()).andReturn(3);
    expect(column.getInteger()).andReturn(0);
    expect(column.getInteger()).andReturn(0);

    replay(column);

    RecordPredicate rf = RecordFilterBuilder.build(p, Arrays.asList(column));

    assertFalse(rf.isMatch());
    assertTrue(rf.isMatch());
    assertTrue(rf.isMatch());
    assertTrue(rf.isMatch());

    verify(column);
  }
}
