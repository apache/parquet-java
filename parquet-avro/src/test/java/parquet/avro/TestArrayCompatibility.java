package parquet.avro;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.node.NullNode;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.api.WriteSupport;
import parquet.io.api.RecordConsumer;
import parquet.schema.MessageType;
import parquet.schema.Types;

import static parquet.schema.OriginalType.*;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.*;

public class TestArrayCompatibility {

  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  @Test
  @Ignore(value="Not yet supported")
  public void testUnannotatedListOfPrimitives() throws Exception {
    Path test = writeDirect(
        Types.buildMessage()
            .repeated(INT32).named("list_of_ints")
            .named("UnannotatedListOfPrimitives"),
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.startField("list_of_ints", 0);

            rc.addInteger(34);
            rc.addInteger(35);
            rc.addInteger(36);

            rc.endField("list_of_ints", 0);
            rc.endMessage();
          }
        });

    Schema expectedSchema = record("OldPrimitiveInList",
        field("list_of_ints", array(primitive(Schema.Type.INT))));

    GenericRecord expectedRecord = instance(expectedSchema,
        "list_of_ints", Arrays.asList(34, 35, 36));

    AvroParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(test);
    GenericRecord actualRecord = reader.read();

    Assert.assertNull("Should only contain one record", reader.read());
    Assert.assertEquals("Should match expected schema",
        expectedSchema, actualRecord.getSchema());
    Assert.assertEquals("Should match the expected record",
        expectedRecord, actualRecord);
  }

  @Test
  @Ignore(value="Not yet supported")
  public void testUnannotatedListOfGroups() throws Exception {
    Path test = writeDirect(
        Types.buildMessage()
            .repeatedGroup()
            .required(FLOAT).named("x")
            .required(FLOAT).named("y")
            .named("list_of_points")
            .named("UnannotatedListOfGroups"),
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.startField("list_of_points", 0);

            rc.startGroup();
            rc.startField("x", 0);
            rc.addFloat(1.0f);
            rc.endField("x", 0);
            rc.startField("y", 1);
            rc.addFloat(1.0f);
            rc.endField("y", 1);
            rc.endGroup();

            rc.startGroup();
            rc.startField("x", 0);
            rc.addFloat(2.0f);
            rc.endField("x", 0);
            rc.startField("y", 1);
            rc.addFloat(2.0f);
            rc.endField("y", 1);
            rc.endGroup();

            rc.endField("list_of_points", 0);
            rc.endMessage();
          }
        });

    Schema point = record("?",
        field("x", primitive(Schema.Type.FLOAT)),
        field("y", primitive(Schema.Type.FLOAT)));
    Schema expectedSchema = record("OldPrimitiveInList",
        field("list_of_points", array(point)));

    GenericRecord expectedRecord = instance(expectedSchema,
        "list_of_points", Arrays.asList(
            instance(point, "x", 1.0f, "y", 1.0f),
            instance(point, "x", 2.0f, "y", 2.0f)));

    AvroParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(test);
    GenericRecord actualRecord = reader.read();

    Assert.assertNull("Should only contain one record", reader.read());
    Assert.assertEquals("Should match expected schema",
        expectedSchema, actualRecord.getSchema());
    Assert.assertEquals("Should match the expected record",
        expectedRecord, actualRecord);
  }

  @Test
  public void testOldPrimitiveInList() throws Exception {
    Path test = writeDirect(
        Types.buildMessage()
            .requiredGroup().as(LIST)
                .repeated(INT32).named("array")
                .named("list_of_ints")
            .named("OldPrimitiveInList"),
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.startField("list_of_ints", 0);

            rc.startGroup();
            rc.startField("array", 0);

            rc.addInteger(34);
            rc.addInteger(35);
            rc.addInteger(36);

            rc.endField("array", 0);
            rc.endGroup();

            rc.endField("list_of_ints", 0);
            rc.endMessage();
          }
        });

    Schema expectedSchema = record("OldPrimitiveInList",
        field("list_of_ints", array(Schema.create(Schema.Type.INT))));

    GenericRecord expectedRecord = instance(expectedSchema,
        "list_of_ints", Arrays.asList(34, 35, 36));

    AvroParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(test);
    GenericRecord actualRecord = reader.read();

    Assert.assertNull("Should only contain one record", reader.read());
    Assert.assertEquals("Should match expected schema",
        expectedSchema, actualRecord.getSchema());
    Assert.assertEquals("Should match the expected record",
        expectedRecord, actualRecord);
  }

  @Test
  public void testOldMultiFieldGroupInList() throws Exception {
    // tests the missing element layer, detected by a multi-field group
    Path test = writeDirect(
        Types.buildMessage()
            .optionalGroup().as(LIST)
                .repeatedGroup()
                    .required(DOUBLE).named("latitude")
                    .required(DOUBLE).named("longitude")
                    .named("element") // should not affect schema conversion
                .named("locations")
            .named("OldMultiFieldGroupInList"),
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.startField("locations", 0);

            rc.startGroup();
            rc.startField("element", 0);

            rc.startGroup();
            rc.startField("latitude", 0);
            rc.addDouble(0.0);
            rc.endField("latitude", 0);
            rc.startField("longitude", 1);
            rc.addDouble(0.0);
            rc.endField("longitude", 1);
            rc.endGroup();

            rc.startGroup();
            rc.startField("latitude", 0);
            rc.addDouble(0.0);
            rc.endField("latitude", 0);
            rc.startField("longitude", 1);
            rc.addDouble(180.0);
            rc.endField("longitude", 1);
            rc.endGroup();

            rc.endField("element", 0);
            rc.endGroup();

            rc.endField("locations", 0);
            rc.endMessage();
          }
        });

    Schema location = record("element",
        field("latitude", primitive(Schema.Type.DOUBLE)),
        field("longitude", primitive(Schema.Type.DOUBLE)));
    Schema expectedSchema = record("OldMultiFieldGroupInList",
        optionalField("locations", array(location)));

    GenericRecord expectedRecord = instance(expectedSchema,
        "locations", Arrays.asList(
            instance(location, "latitude", 0.0, "longitude", 0.0),
            instance(location, "latitude", 0.0, "longitude", 180.0)));

    AvroParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(test);
    GenericRecord actualRecord = reader.read();

    Assert.assertNull("Should only contain one record", reader.read());
    Assert.assertEquals("Should match expected schema",
        expectedSchema, actualRecord.getSchema());
    Assert.assertEquals("Should match the expected record",
        expectedRecord, actualRecord);
  }

  @Test
  public void testOldSingleFieldGroupInList() throws Exception {
    // this tests the case where non-avro older data has an ambiguous list
    Path test = writeDirect(
        Types.buildMessage()
            .optionalGroup().as(LIST)
                .repeatedGroup()
                    .required(INT64).named("count")
                    .named("single_element_group")
                .named("single_element_groups")
            .named("OldSingleFieldGroupInList"),
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.startField("single_element_groups", 0);

            rc.startGroup();
            rc.startField("single_element_group", 0); // start writing array contents

            rc.startGroup();
            rc.startField("count", 0);
            rc.addLong(1234L);
            rc.endField("count", 0);
            rc.endGroup();

            rc.startGroup();
            rc.startField("count", 0);
            rc.addLong(2345L);
            rc.endField("count", 0);
            rc.endGroup();

            rc.endField("single_element_group", 0); // finished writing array contents
            rc.endGroup();

            rc.endField("single_element_groups", 0);
            rc.endMessage();
          }
        });

    // although intended to be a group with a single element, the storage was
    // ambiguous. now, single element groups are assumed to be synthetic

    Schema expectedSchema = record("OldSingleFieldGroupInList",
        optionalField("single_element_groups", array(primitive(Schema.Type.LONG))));

    GenericRecord expectedRecord = instance(expectedSchema,
        "single_element_groups", Arrays.asList(1234L, 2345L));

    AvroParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(test);
    GenericRecord actualRecord = reader.read();

    Assert.assertNull("Should only contain one record", reader.read());
    Assert.assertEquals("Should match expected schema",
        expectedSchema, actualRecord.getSchema());
    Assert.assertEquals("Should match the expected record",
        expectedRecord, actualRecord);
  }

  @Test
  public void testOldSingleFieldGroupInListWithSchema() throws Exception {
    // this tests the case where older data has an ambiguous structure, but the
    // correct interpretation can be determined from the avro schema

    Schema singleElementRecord = record("single_element_group",
        field("count", primitive(Schema.Type.LONG)));

    Schema expectedSchema = record("OldSingleFieldGroupInList",
        optionalField("single_element_groups",
            array(singleElementRecord)));

    Map<String, String> metadata = new HashMap<String, String>();
    metadata.put(AvroWriteSupport.AVRO_SCHEMA, expectedSchema.toString());

    Path test = writeDirect(
        Types.buildMessage()
            .optionalGroup().as(LIST)
                .repeatedGroup()
                    .required(INT64).named("count")
                    .named("single_element_group")
                .named("single_element_groups")
            .named("OldSingleFieldGroupInList"),
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.startField("single_element_groups", 0);

            rc.startGroup();
            rc.startField("single_element_group", 0); // start writing array contents

            rc.startGroup();
            rc.startField("count", 0);
            rc.addLong(1234L);
            rc.endField("count", 0);
            rc.endGroup();

            rc.startGroup();
            rc.startField("count", 0);
            rc.addLong(2345L);
            rc.endField("count", 0);
            rc.endGroup();

            rc.endField("single_element_group", 0); // finished writing array contents
            rc.endGroup();

            rc.endField("single_element_groups", 0);
            rc.endMessage();
          }
        },
        metadata);

    // although intended to be a group with a single element, the storage was
    // ambiguous. now, single element groups are assumed to be synthetic

    GenericRecord expectedRecord = instance(expectedSchema,
        "single_element_groups", Arrays.asList(
            instance(singleElementRecord, "count", 1234L),
            instance(singleElementRecord, "count", 2345L)));

    AvroParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(test);
    GenericRecord actualRecord = reader.read();

    Assert.assertNull("Should only contain one record", reader.read());
    Assert.assertEquals("Should match expected schema",
        expectedSchema, actualRecord.getSchema());
    Assert.assertEquals("Should match the expected record",
        expectedRecord, actualRecord);
  }

  @Test
  public void testNewOptionalGroupInList() throws Exception {
    Path test = writeDirect(
        Types.buildMessage()
            .optionalGroup().as(LIST)
                .repeatedGroup()
                    .optionalGroup()
                        .required(DOUBLE).named("latitude")
                        .required(DOUBLE).named("longitude")
                        .named("element")
                    .named("array")
                .named("locations")
            .named("NewOptionalGroupInList"),
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.startField("locations", 0);

            rc.startGroup();
            rc.startField("array", 0); // start writing array contents

            // write a non-null element
            rc.startGroup(); // array level
            rc.startField("element", 0);

            rc.startGroup();
            rc.startField("latitude", 0);
            rc.addDouble(0.0);
            rc.endField("latitude", 0);
            rc.startField("longitude", 1);
            rc.addDouble(0.0);
            rc.endField("longitude", 1);
            rc.endGroup();

            rc.endField("element", 0);
            rc.endGroup(); // array level

            // write a null element (element field is omitted)
            rc.startGroup(); // array level
            rc.endGroup(); // array level

            // write a second non-null element
            rc.startGroup(); // array level
            rc.startField("element", 0);

            rc.startGroup();
            rc.startField("latitude", 0);
            rc.addDouble(0.0);
            rc.endField("latitude", 0);
            rc.startField("longitude", 1);
            rc.addDouble(180.0);
            rc.endField("longitude", 1);
            rc.endGroup();

            rc.endField("element", 0);
            rc.endGroup(); // array level

            rc.endField("array", 0); // finished writing array contents
            rc.endGroup();

            rc.endField("locations", 0);
            rc.endMessage();
          }
        });

    Schema location = record("element",
        field("latitude", primitive(Schema.Type.DOUBLE)),
        field("longitude", primitive(Schema.Type.DOUBLE)));

    Schema expectedSchema = record("NewOptionalGroupInList",
        optionalField("locations", array(optional(location))));

    GenericRecord expectedRecord = instance(expectedSchema,
        "locations", Arrays.asList(
            instance(location, "latitude", 0.0, "longitude", 0.0),
            null,
            instance(location, "latitude", 0.0, "longitude", 180.0)));

    AvroParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(test);
    GenericRecord actualRecord = reader.read();

    Assert.assertNull("Should only contain one record", reader.read());
    Assert.assertEquals("Should match expected schema",
        expectedSchema, actualRecord.getSchema());
    Assert.assertEquals("Should match the expected record",
        expectedRecord, actualRecord);
  }

  @Test
  public void testNewRequiredGroupInList() throws Exception {
    Path test = writeDirect(
        Types.buildMessage()
            .optionalGroup().as(LIST)
                .repeatedGroup()
                    .requiredGroup()
                        .required(DOUBLE).named("latitude")
                        .required(DOUBLE).named("longitude")
                        .named("element")
                    .named("array")
                .named("locations")
            .named("NewRequiredGroupInList"),
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.startField("locations", 0);

            rc.startGroup();
            rc.startField("array", 0); // start writing array contents

            // write a non-null element
            rc.startGroup(); // array level
            rc.startField("element", 0);

            rc.startGroup();
            rc.startField("latitude", 0);
            rc.addDouble(0.0);
            rc.endField("latitude", 0);
            rc.startField("longitude", 1);
            rc.addDouble(180.0);
            rc.endField("longitude", 1);
            rc.endGroup();

            rc.endField("element", 0);
            rc.endGroup(); // array level

            // write a second non-null element
            rc.startGroup(); // array level
            rc.startField("element", 0);

            rc.startGroup();
            rc.startField("latitude", 0);
            rc.addDouble(0.0);
            rc.endField("latitude", 0);
            rc.startField("longitude", 1);
            rc.addDouble(0.0);
            rc.endField("longitude", 1);
            rc.endGroup();

            rc.endField("element", 0);
            rc.endGroup(); // array level

            rc.endField("array", 0); // finished writing array contents
            rc.endGroup();

            rc.endField("locations", 0);
            rc.endMessage();
          }
        });

    Schema location = record("element",
        field("latitude", primitive(Schema.Type.DOUBLE)),
        field("longitude", primitive(Schema.Type.DOUBLE)));

    Schema expectedSchema = record("NewRequiredGroupInList",
        optionalField("locations", array(location)));

    GenericRecord expectedRecord = instance(expectedSchema,
        "locations", Arrays.asList(
            instance(location, "latitude", 0.0, "longitude", 180.0),
            instance(location, "latitude", 0.0, "longitude", 0.0)));

    AvroParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(test);
    GenericRecord actualRecord = reader.read();

    Assert.assertNull("Should only contain one record", reader.read());
    Assert.assertEquals("Should match expected schema",
        expectedSchema, actualRecord.getSchema());
    Assert.assertEquals("Should match the expected record",
        expectedRecord, actualRecord);
  }

  @Test
  public void testOldRequiredGroupInList() throws Exception {
    Path test = writeDirect(
        Types.buildMessage()
            .optionalGroup().as(LIST)
                .repeatedGroup()
                    .requiredGroup()
                        .required(DOUBLE).named("latitude")
                        .required(DOUBLE).named("longitude")
                        .named("element")
                    .named("array")
                .named("locations")
            .named("OldRequiredGroupInList"),
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.startField("locations", 0);

            rc.startGroup();
            rc.startField("array", 0); // start writing array contents

            // write a non-null element
            rc.startGroup(); // array level
            rc.startField("element", 0);

            rc.startGroup();
            rc.startField("latitude", 0);
            rc.addDouble(0.0);
            rc.endField("latitude", 0);
            rc.startField("longitude", 1);
            rc.addDouble(180.0);
            rc.endField("longitude", 1);
            rc.endGroup();

            rc.endField("element", 0);
            rc.endGroup(); // array level

            // write a second non-null element
            rc.startGroup(); // array level
            rc.startField("element", 0);

            rc.startGroup();
            rc.startField("latitude", 0);
            rc.addDouble(0.0);
            rc.endField("latitude", 0);
            rc.startField("longitude", 1);
            rc.addDouble(0.0);
            rc.endField("longitude", 1);
            rc.endGroup();

            rc.endField("element", 0);
            rc.endGroup(); // array level

            rc.endField("array", 0); // finished writing array contents
            rc.endGroup();

            rc.endField("locations", 0);
            rc.endMessage();
          }
        });

    Schema location = record("element",
        field("latitude", primitive(Schema.Type.DOUBLE)),
        field("longitude", primitive(Schema.Type.DOUBLE)));

    Schema expectedSchema = record("OldRequiredGroupInList",
        optionalField("locations", array(location)));

    GenericRecord expectedRecord = instance(expectedSchema,
        "locations", Arrays.asList(
            instance(location, "latitude", 0.0, "longitude", 180.0),
            instance(location, "latitude", 0.0, "longitude", 0.0)));

    AvroParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(test);
    GenericRecord actualRecord = reader.read();

    Assert.assertNull("Should only contain one record", reader.read());
    Assert.assertEquals("Should match expected schema",
        expectedSchema, actualRecord.getSchema());
    Assert.assertEquals("Should match the expected record",
        expectedRecord, actualRecord);
  }

  private interface DirectWriter {
    public void write(RecordConsumer consumer);
  }

  private static class DirectWriteSupport extends WriteSupport<Void> {
    private RecordConsumer recordConsumer;
    private final MessageType type;
    private final DirectWriter writer;
    private final Map<String, String> metadata;

    private DirectWriteSupport(MessageType type, DirectWriter writer,
                               Map<String, String> metadata) {
      this.type = type;
      this.writer = writer;
      this.metadata = metadata;
    }

    @Override
    public WriteContext init(Configuration configuration) {
      return new WriteContext(type, metadata);
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
      this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(Void record) {
      writer.write(recordConsumer);
    }
  }

  private Path writeDirect(MessageType type, DirectWriter writer) throws IOException {
    return writeDirect(type, writer, new HashMap<String, String>());
  }

  private Path writeDirect(MessageType type, DirectWriter writer,
                           Map<String, String> metadata) throws IOException {
    File temp = tempDir.newFile(UUID.randomUUID().toString());
    temp.deleteOnExit();
    temp.delete();

    Path path = new Path(temp.getPath());

    ParquetWriter<Void> parquetWriter = new ParquetWriter<Void>(
        path, new DirectWriteSupport(type, writer, metadata));
    parquetWriter.write(null);
    parquetWriter.close();

    return path;
  }

  public static Schema record(String name, Schema.Field... fields) {
    Schema record = Schema.createRecord(name, null, null, false);
    record.setFields(Arrays.asList(fields));
    return record;
  }

  public static Schema.Field field(String name, Schema schema) {
    return new Schema.Field(name, schema, null, null);
  }

  public static Schema.Field optionalField(String name, Schema schema) {
    return new Schema.Field(name, optional(schema), null, NullNode.getInstance());
  }

  public static Schema array(Schema element) {
    return Schema.createArray(element);
  }

  public static Schema primitive(Schema.Type type) {
    return Schema.create(type);
  }

  public static Schema optional(Schema original) {
    return Schema.createUnion(Lists.newArrayList(
        Schema.create(Schema.Type.NULL),
        original));
  }

  private static GenericRecord instance(Schema schema, Object... pairs) {
    if ((pairs.length % 2) != 0) {
      throw new RuntimeException("Not enough values");
    }
    GenericRecord record = new GenericData.Record(schema);
    for (int i = 0; i < pairs.length; i += 2) {
      record.put(pairs[i].toString(), pairs[i + 1]);
    }
    return record;
  }

}
