package parquet.avro;

import java.util.*;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import parquet.hadoop.api.InitContext;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static parquet.avro.AvroReadSupport.*;

public class TestAvroReadSupport {

  /* Avro Schemas */

  /* equivalent to message Foo { required binary field_1 (UTF8); } */
  static final String RECORD_WITH_REQUIRED_STRING =
    "{\"type\":\"record\"," +
    " \"name\":\"Foo\"," +
    " \"fields\":[" +
    "    {\"name\":\"field_1\"," +
    "     \"type\":{\"type\":\"string\"}" +
    "    }" +
    "  ]" +
    "}";

  /* equivalent to message Foo { optional binary field_1 (UTF8); } */
  static final String RECORD_WITH_OPTIONAL_STRING =
    "{\"type\":\"record\"," +
    " \"name\":\"Foo\"," +
    " \"fields\":[" +
    "    {\"name\":\"field_1\"," +
    "     \"type\":[\"null\",{\"type\":\"string\"}]," +
    "     \"default\":null" +
    "    }" +
    "  ]" +
    "}";

  /* equivalent to message Foo { required int64 field_1; } */
  static final String RECORD_WITH_REQUIRED_LONG =
    "{\"type\":\"record\"," +
    " \"name\":\"Foo\"," +
    " \"fields\":[" +
    "    {\"name\":\"field_1\"," +
    "     \"type\":{\"type\":\"long\"}" +
    "    }" +
    "  ]" +
    "}";

  /* equivalent to message Foo { optional binary field_1 (UTF8); required binary field_2 (UTF8) = "foo"; } */
  static final String RECORD_WITH_REQUIRED_AND_OPTIONAL_STRINGS = 
    "{\"type\":\"record\"," +
    " \"name\":\"Foo\"," +
    " \"fields\":[" +
    "    {\"name\":\"field_1\"," +
    "     \"type\":[\"null\",{\"type\":\"string\"}]," +
    "     \"default\":null" +
    "    }," +
    "    {\"name\":\"field_2\"," +
    "     \"type\":{\"type\":\"string\"}," +
    "     \"default\":\"foo\"" +
    "    }" +
    "  ]" +
    "}";

  /* Parquet Schemas */

  static final String MESSAGE_WITH_REQUIRED_STRING =
    "message Foo {" +
    "  required binary field_1 (UTF8);" +
    "}";

  static final String MESSAGE_WITH_REQUIRED_AND_OPTIONAL_STRINGS =
    "message Foo {" +
    "  optional binary field_1 (UTF8);" +
    "  required binary field_2 (UTF8);" +
    "}";

  /* Changing a field from required to optional is valid */
  @Test
  public void testValidSchemaCompatibility() throws Exception {
    Schema readerSchema = new Schema.Parser().parse(RECORD_WITH_OPTIONAL_STRING);
    Schema writerSchema = new Schema.Parser().parse(RECORD_WITH_REQUIRED_STRING);
    boolean result = isSchemaCompatible(readerSchema, writerSchema);
    assertTrue(result);
  }

  /* Changing a field from string to long */
  @Test
  public void testInvalidSchemaCompatibility() throws Exception {
    Schema readerSchema = new Schema.Parser().parse(RECORD_WITH_REQUIRED_LONG);
    Schema writerSchema = new Schema.Parser().parse(RECORD_WITH_REQUIRED_STRING);
    boolean result = isSchemaCompatible(readerSchema, writerSchema);
    assertFalse(result);
  }

  /* Changing a field from required to optional and adding an additional field */
  @Test
  public void testValidAggregateSchemaCompatibility() throws Exception {
    String[] writerSchemaStrings = {
      RECORD_WITH_REQUIRED_STRING,
      RECORD_WITH_OPTIONAL_STRING,
      RECORD_WITH_REQUIRED_AND_OPTIONAL_STRINGS
    };
    Set<Schema> writerSchemas = new HashSet<Schema>();
    for (String s : writerSchemaStrings) {
      Schema writerSchema = new Schema.Parser().parse(s);
      writerSchemas.add(writerSchema);
    }
    Schema readerSchema = new Schema.Parser().parse(RECORD_WITH_REQUIRED_AND_OPTIONAL_STRINGS);
    boolean result = areSchemasCompatible(readerSchema, writerSchemas);
    assertTrue(result);
  }

  /* Changing a field from required to optional, changing a field type from string to long, and adding an additional field */
  @Test
  public void testInvalidAggregateSchemaCompatibility() throws Exception {
    String[] writerSchemaStrings = {
      RECORD_WITH_REQUIRED_STRING,
      RECORD_WITH_OPTIONAL_STRING,
      RECORD_WITH_REQUIRED_LONG,
      RECORD_WITH_REQUIRED_AND_OPTIONAL_STRINGS
    };
    Set<Schema> writerSchemas = new HashSet<Schema>();
    for (String s : writerSchemaStrings) {
      Schema writerSchema = new Schema.Parser().parse(s);
      writerSchemas.add(writerSchema);
    }
    Schema readerSchema = new Schema.Parser().parse(RECORD_WITH_REQUIRED_AND_OPTIONAL_STRINGS);
    boolean result = areSchemasCompatible(readerSchema, writerSchemas);
    assertFalse(result);
  }

  @Rule
  public ExpectedException exception = ExpectedException.none();

  /* Reader schema supplied */
  @Test
  public void doesNotThrowRuntimeExceptionIfAvroReadSchemaSet() {
    InitContext context = initContext(RECORD_WITH_REQUIRED_STRING, null, MESSAGE_WITH_REQUIRED_STRING);
    AvroReadSupport readSupport = new AvroReadSupport();
    readSupport.init(context);
  }

  /* No reader schema supplied */
  @Test
  public void doesNotThrowsRuntimeExceptionIfAvroReadSchemaUnset() {
    InitContext context = initContext(null, null, MESSAGE_WITH_REQUIRED_STRING);
    AvroReadSupport readSupport = new AvroReadSupport();
    readSupport.init(context);
  }

  /* No reader schema supplied, but one writer schema found */
  @Test
  public void doesNotThrowRuntimeExceptionIfAvroReadSchemaUnsetAndWriterSchemaPresent() {
    Set<String> writerSchemaStrings = new HashSet<String>();
    writerSchemaStrings.add(RECORD_WITH_REQUIRED_STRING);
    InitContext context = initContext(null, writerSchemaStrings, MESSAGE_WITH_REQUIRED_STRING);
    AvroReadSupport readSupport = new AvroReadSupport();
    readSupport.init(context);
  }

  /* No reader schema supplied, but multiple writer schemas found */
  @Test
  public void throwsRuntimeExceptionIfAvroReadSchemaUnsetAndWriterSchemasPresent() {
    Set<String> writerSchemaStrings = new HashSet<String>();
    writerSchemaStrings.add(RECORD_WITH_REQUIRED_STRING);
    writerSchemaStrings.add(RECORD_WITH_OPTIONAL_STRING);
    InitContext context = initContext(null, writerSchemaStrings, MESSAGE_WITH_REQUIRED_STRING);
    AvroReadSupport readSupport = new AvroReadSupport();
    exception.expect(RuntimeException.class);
    exception.expectMessage("could not merge metadata: key " + AVRO_SCHEMA_METADATA_KEY + " has conflicting values");
    readSupport.init(context);
  }

  /* Changing a field from required to optional, changing a field type from string to long, and adding an additional field */
  @Test
  public void throwsRuntimeExceptionIfSchemasAreNotCompatible() {
    String readerSchemaString = RECORD_WITH_REQUIRED_AND_OPTIONAL_STRINGS;
    Set<String> writerSchemaStrings = new HashSet<String>();
    writerSchemaStrings.add(RECORD_WITH_REQUIRED_STRING);
    writerSchemaStrings.add(RECORD_WITH_OPTIONAL_STRING);
    writerSchemaStrings.add(RECORD_WITH_REQUIRED_LONG);
    writerSchemaStrings.add(RECORD_WITH_REQUIRED_AND_OPTIONAL_STRINGS);
    String parquetSchemaString = MESSAGE_WITH_REQUIRED_AND_OPTIONAL_STRINGS;
    InitContext context = initContext(readerSchemaString, writerSchemaStrings, parquetSchemaString);
    AvroReadSupport readSupport = new AvroReadSupport();
    exception.expect(RuntimeException.class);
    exception.expectMessage("could not merge metadata: key " + AVRO_SCHEMA_METADATA_KEY + " contains incompatible schemas");
    readSupport.init(context);
  }

  /* Changing a field from required to optional and adding an additional field */
  @Test
  public void doesNotThrowRuntimeExceptionIfSchemasAreCompatible() {
    String readerSchemaString = RECORD_WITH_REQUIRED_AND_OPTIONAL_STRINGS;
    Set<String> writerSchemaStrings = new HashSet<String>();
    writerSchemaStrings.add(RECORD_WITH_REQUIRED_STRING);
    writerSchemaStrings.add(RECORD_WITH_OPTIONAL_STRING);
    writerSchemaStrings.add(RECORD_WITH_REQUIRED_AND_OPTIONAL_STRINGS);
    String parquetSchemaString = MESSAGE_WITH_REQUIRED_AND_OPTIONAL_STRINGS;
    InitContext context = initContext(readerSchemaString, writerSchemaStrings, parquetSchemaString);
    AvroReadSupport readSupport = new AvroReadSupport();
    readSupport.init(context);
  }

  /* Although only compatible schemas provided, do not check for compatibility if flag unset */
  @Test
  public void throwRuntimeExceptionIfSchemasAreCompatibleButCheckDisabled() {
    String readerSchemaString = RECORD_WITH_REQUIRED_AND_OPTIONAL_STRINGS;
    Set<String> writerSchemaStrings = new HashSet<String>();
    writerSchemaStrings.add(RECORD_WITH_REQUIRED_STRING);
    writerSchemaStrings.add(RECORD_WITH_OPTIONAL_STRING);
    writerSchemaStrings.add(RECORD_WITH_REQUIRED_AND_OPTIONAL_STRINGS);
    String parquetSchemaString = MESSAGE_WITH_REQUIRED_AND_OPTIONAL_STRINGS;
    InitContext context = initContext(readerSchemaString, writerSchemaStrings, parquetSchemaString);
    AvroReadSupport.disableAvroSchemaCompatibilityCheck(context.getConfiguration());
    AvroReadSupport readSupport = new AvroReadSupport();
    exception.expect(RuntimeException.class);
    exception.expectMessage("could not merge metadata: key " + AVRO_SCHEMA_METADATA_KEY + " has conflicting values");
    readSupport.init(context);
  }

  /* Helper function to build an instance of InitContext given Avro and Parquet schemas
   * NOTE: Invoking this method will enable Avro schema compatibility checking!
   */
  static InitContext initContext(String readerSchemaString, Set<String> writerSchemaStrings, String parquetSchemaString) {
    Configuration configuration = new Configuration(false);
    configuration.setBoolean(AVRO_SCHEMA_COMPATIBILITY_CHECK, true);
    if (readerSchemaString != null)
      configuration.set(AVRO_READ_SCHEMA, readerSchemaString);
    Map<String, Set<String>> metadata = new HashMap<String, Set<String>>();
    if (writerSchemaStrings != null)
      metadata.put(AVRO_SCHEMA_METADATA_KEY, writerSchemaStrings);
    MessageType messageType = MessageTypeParser.parseMessageType(parquetSchemaString);
    return new InitContext(configuration, metadata, messageType);
  }
}
