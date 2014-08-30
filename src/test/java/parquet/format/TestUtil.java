package parquet.format;

import static java.util.Arrays.asList;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static parquet.format.Util.readFileMetaData;
import static parquet.format.Util.writeFileMetaData;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.junit.Test;

import parquet.format.Util.DefaultFileMetaDataConsumer;
public class TestUtil {

  @Test
  public void testReadFileMetadata() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    FileMetaData md = new FileMetaData(
        1,
        asList(new SchemaElement("foo")),
        10,
        asList(
            new RowGroup(
                asList(
                    new ColumnChunk(0),
                    new ColumnChunk(1)
                    ),
                10,
                5),
            new RowGroup(
                asList(
                    new ColumnChunk(2),
                    new ColumnChunk(3)
                    ),
                11,
                5)
        )
    );
    writeFileMetaData(md , baos);
    FileMetaData md2 = readFileMetaData(in(baos));
    FileMetaData md3 = new FileMetaData();
    readFileMetaData(in(baos), new DefaultFileMetaDataConsumer(md3));
    FileMetaData md4 = new FileMetaData();
    readFileMetaData(in(baos), new DefaultFileMetaDataConsumer(md4), true);
    FileMetaData md5 = readFileMetaData(in(baos), true);
    FileMetaData md6 = readFileMetaData(in(baos), false);
    assertEquals(md, md2);
    assertEquals(md, md3);
    assertNull(md4.getRow_groups());
    assertNull(md5.getRow_groups());
    assertEquals(md4, md5);
    md4.setRow_groups(md.getRow_groups());
    md5.setRow_groups(md.getRow_groups());
    assertEquals(md, md4);
    assertEquals(md, md5);
    assertEquals(md4, md5);
    assertEquals(md, md6);
  }

  private ByteArrayInputStream in(ByteArrayOutputStream baos) {
    return new ByteArrayInputStream(baos.toByteArray());
  }
}
