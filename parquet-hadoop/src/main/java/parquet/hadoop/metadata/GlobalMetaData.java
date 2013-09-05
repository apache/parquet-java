package parquet.hadoop.metadata;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import parquet.schema.MessageType;

public class GlobalMetaData implements Serializable {
  private static final long serialVersionUID = 1L;

  private final MessageType schema;

  private final Map<String, Set<String>> keyValueMetaData;

  private final Set<String> createdBy;

  /**
   * @param schema the schema for the file
   * @param keyValueMetaData the app specific metadata
   * @param createdBy the description of the library that created the file
   */
  public GlobalMetaData(MessageType schema, Map<String, Set<String>> keyValueMetaData, Set<String> createdBy) {
    super();
    if (schema == null) {
      throw new NullPointerException("schema");
    }
    if (keyValueMetaData == null) {
      throw new NullPointerException("keyValueMetaData");
    }
    this.schema = schema;
    this.keyValueMetaData = Collections.unmodifiableMap(keyValueMetaData);
    this.createdBy = createdBy;
  }

  /**
   * @return the schema for the file
   */
  public MessageType getSchema() {
    return schema;
  }

  @Override
  public String toString() {
    return "GlobalMetaData{schema: "+schema+ ", metadata: " + keyValueMetaData + "}";
  }

  /**
   * @return meta data for extensions
   */
  public Map<String, Set<String>> getKeyValueMetaData() {
    return keyValueMetaData;
  }

  /**
   * @return the description of the library that created the file
   */
  public Set<String> getCreatedBy() {
    return createdBy;
  }

  public FileMetaData merge() {
    String createdByString = createdBy.size() == 1 ?
      createdBy.iterator().next() :
      createdBy.toString();
    Map<String, String> mergedKeyValues = new HashMap<String, String>();
    for (Entry<String, Set<String>> entry : keyValueMetaData.entrySet()) {
      if (entry.getValue().size() > 1) {
        throw new RuntimeException("could not merge metadata: key " + entry.getKey() + " has conflicting values: " + entry.getValue());
      }
      mergedKeyValues.put(entry.getKey(), entry.getValue().iterator().next());
    }
    return new FileMetaData(schema, mergedKeyValues, createdByString);
  }

}