package parquet.hadoop.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import parquet.ColumnPath;
import parquet.column.Encoding;
import parquet.column.statistics.BinaryStatistics;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

public class TestColumnChunkMetaData {


  @Test
  public void testConversionBig() {
    long big = (long)Integer.MAX_VALUE + 1;

    ColumnChunkMetaData md = newMD(big);
    assertTrue(md instanceof IntColumnChunkMetaData);
    assertEquals(big, md.getFirstDataPageOffset());
  }

  @Test
  public void testConversionSmall() {
    long small = 1;

    ColumnChunkMetaData md = newMD(small);
    assertTrue(md instanceof IntColumnChunkMetaData);
    assertEquals(small, md.getFirstDataPageOffset());
  }

  @Test
  public void testConversionVeryBig() {
    long veryBig = (long)Integer.MAX_VALUE * 3;

    ColumnChunkMetaData md = newMD(veryBig);
    assertTrue(md instanceof LongColumnChunkMetaData);
    assertEquals(veryBig, md.getFirstDataPageOffset());
  }

  @Test
  public void testConversionNeg() {
    long neg = -1;

    ColumnChunkMetaData md = newMD(neg);
    assertTrue(md instanceof LongColumnChunkMetaData);
    assertEquals(neg, md.getFirstDataPageOffset());
  }

  private ColumnChunkMetaData newMD(long big) {
    Set<Encoding> e = new HashSet<Encoding>();
    PrimitiveTypeName t = BINARY;
    ColumnPath p = ColumnPath.get("foo");
    CompressionCodecName c = CompressionCodecName.GZIP;
    BinaryStatistics s = new BinaryStatistics();
    ColumnChunkMetaData md = ColumnChunkMetaData.get(p, t, c, e, s,
                                                     big, 0, 0, 0, 0);
    return md;
  }
}
