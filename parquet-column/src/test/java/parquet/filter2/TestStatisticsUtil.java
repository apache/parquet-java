package parquet.filter2;

import org.junit.Test;

import parquet.column.statistics.BinaryStatistics;
import parquet.column.statistics.BooleanStatistics;
import parquet.column.statistics.DoubleStatistics;
import parquet.column.statistics.FloatStatistics;
import parquet.column.statistics.IntStatistics;
import parquet.column.statistics.LongStatistics;
import parquet.column.statistics.Statistics;
import parquet.filter2.StatisticsUtil.MinMaxComparison;
import parquet.filter2.UserDefinedPredicates.BinaryUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.DoubleUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.FloatUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.IntUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.LongUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.StringUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.UserDefinedPredicate;
import parquet.io.api.Binary;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static parquet.filter2.StatisticsUtil.applyUdpMinMax;
import static parquet.filter2.StatisticsUtil.compareMinMax;

public class TestStatisticsUtil {
  private static class InvalidColumnType { }

  private <T> void doTestCompare(Class<T> clazz,
                                 Statistics rawStats,
                                 UserDefinedPredicate<T> udp,
                                 T min,
                                 T max,
                                 T lessThanBoth,
                                 T inBetween,
                                 T moreThanBoth) {

    MinMaxComparison cmp = compareMinMax(clazz, lessThanBoth, rawStats);
    assertTrue(cmp.getMinCmp() < 0);
    assertTrue(cmp.getMaxCmp() < 0);

    cmp = compareMinMax(clazz, min, rawStats);
    assertTrue(cmp.getMinCmp() == 0);
    assertTrue(cmp.getMaxCmp() < 0);

    cmp = compareMinMax(clazz, inBetween, rawStats);
    assertTrue(cmp.getMinCmp() > 0);
    assertTrue(cmp.getMaxCmp() < 0);

    cmp = compareMinMax(clazz, max, rawStats);
    assertTrue(cmp.getMinCmp() > 0);
    assertTrue(cmp.getMaxCmp() == 0);

    cmp = compareMinMax(clazz, moreThanBoth, rawStats);
    assertTrue(cmp.getMinCmp() > 0);
    assertTrue(cmp.getMaxCmp() > 0);

    applyUdpMinMax(clazz, udp, rawStats, false);
    applyUdpMinMax(clazz, udp, rawStats, true);
    verify(udp);
  }

  @Test
  public void testCompareInt() {
    IntStatistics stats = new IntStatistics();
    stats.setMinMax(10, 100);
    IntUserDefinedPredicate udp = createStrictMock(IntUserDefinedPredicate.class);
    expect(udp.canDrop(10, 100)).andReturn(true);
    expect(udp.inverseCanDrop(10, 100)).andReturn(true);
    replay(udp);
    doTestCompare(Integer.class, stats, udp, 10, 100, 9, 11, 101);
  }

  @Test
  public void testCompareLong() {
    LongStatistics stats = new LongStatistics();
    stats.setMinMax(10L, 100L);
    LongUserDefinedPredicate udp = createStrictMock(LongUserDefinedPredicate.class);
    expect(udp.canDrop(10L, 100L)).andReturn(true);
    expect(udp.inverseCanDrop(10L, 100L)).andReturn(true);
    replay(udp);
    doTestCompare(Long.class, stats, udp, 10L, 100L, 9L, 11L, 101L);
  }

  @Test
  public void testCompareFloat() {
    FloatStatistics stats = new FloatStatistics();
    stats.setMinMax(10f, 100f);
    FloatUserDefinedPredicate udp = createStrictMock(FloatUserDefinedPredicate.class);
    expect(udp.canDrop(10f, 100f)).andReturn(true);
    expect(udp.inverseCanDrop(10f, 100f)).andReturn(true);
    replay(udp);
    doTestCompare(Float.class, stats, udp, 10f, 100f, 9.9f, 10.1f, 100.1f);
  }

  @Test
  public void testCompareDouble() {
    DoubleStatistics stats = new DoubleStatistics();
    stats.setMinMax(10D, 100D);
    DoubleUserDefinedPredicate udp = createStrictMock(DoubleUserDefinedPredicate.class);
    expect(udp.canDrop(10D, 100D)).andReturn(true);
    expect(udp.inverseCanDrop(10D, 100D)).andReturn(true);
    replay(udp);
    doTestCompare(Double.class, stats, udp, 10D, 100D, 9.9D, 10.1D, 100.1D);
  }

  @Test
  public void testCompareBinaryAndString() {
    BinaryStatistics stats = new BinaryStatistics();
    Binary min = Binary.fromByteArray(new byte[]{10, 2});
    Binary max = Binary.fromByteArray(new byte[]{100, 7});
    Binary lessThanBoth = Binary.fromByteArray(new byte[]{9, 7});
    Binary inBetween = Binary.fromByteArray(new byte[]{11, 6});
    Binary moreThanBoth = Binary.fromByteArray(new byte[]{101, 7});
    stats.setMinMax(min, max);

    BinaryUserDefinedPredicate binaryUdp = createStrictMock(BinaryUserDefinedPredicate.class);
    expect(binaryUdp.canDrop(min, max)).andReturn(true);
    expect(binaryUdp.inverseCanDrop(min, max)).andReturn(true);
    replay(binaryUdp);
    doTestCompare(Binary.class, stats, binaryUdp, min, max, lessThanBoth, inBetween, moreThanBoth);

    StringUserDefinedPredicate stringUdp = createStrictMock(StringUserDefinedPredicate.class);
    expect(stringUdp.canDrop(min.toStringUsingUTF8(), max.toStringUsingUTF8())).andReturn(true);
    expect(stringUdp.inverseCanDrop(min.toStringUsingUTF8(), max.toStringUsingUTF8())).andReturn(true);
    replay(stringUdp);
    doTestCompare(String.class, stats, stringUdp, min.toStringUsingUTF8(), max.toStringUsingUTF8(),
        lessThanBoth.toStringUsingUTF8(), inBetween.toStringUsingUTF8(), moreThanBoth.toStringUsingUTF8());
  }

  @Test
  public void testCompareBoolean() {
    BooleanStatistics stats = new BooleanStatistics();
    stats.setMinMax(false, true);
    MinMaxComparison cmp = compareMinMax(Boolean.class, false, stats);
    assertTrue(cmp.getMinCmp() == 0);
    assertTrue(cmp.getMaxCmp() < 0);
    cmp = compareMinMax(Boolean.class, true, stats);
    assertTrue(cmp.getMinCmp() > 0);
    assertTrue(cmp.getMaxCmp() == 0);
  }

  @Test
  public void testCompareInvalidType() {
    try {
      compareMinMax(InvalidColumnType.class, new InvalidColumnType(), new IntStatistics());
      fail("this should throw");
    } catch(IllegalArgumentException e) {
      assertEquals("Encountered unknown filter column type: parquet.filter2.TestStatisticsUtil$InvalidColumnType",
          e.getMessage());
    }

  }
}
