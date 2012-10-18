package redelm.pig.summary;

import java.util.Map;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * Summary data for a Map
 *
 * @author julien
 *
 */
public class MapSummaryData extends SummaryData {

  private ValueStat size = new ValueStat();

  private FieldSummaryData key;
  private FieldSummaryData value;

  /**
   * add a map to the summary
   * @param m the map
   */
  public void add(Schema schema, Map<?, ?> m) {
    super.add(m);
    size.add(m.size());
    FieldSchema field = getField(schema, 0);
    if (m.size() > 0 && key == null) {
      key = new FieldSummaryData();
      key.setName(getName(field));
      value = new FieldSummaryData();
      value.setName(getName(field));
    }
    for (Map.Entry<?, ?> entry : m.entrySet()) {
      key.add(null, entry.getKey());
      value.add(getSchema(field), entry.getValue());
    }
  }

  @Override
  public void merge(SummaryData other) {
    super.merge(other);
    MapSummaryData otherMapSummaryData = (MapSummaryData) other;
    size.merge(otherMapSummaryData.size);
    key = merge(key, otherMapSummaryData.key);
    value = merge(value, otherMapSummaryData.value);
  }

  public FieldSummaryData getKey() {
    return key;
  }

  public void setKey(FieldSummaryData key) {
    this.key = key;
  }

  public FieldSummaryData getValue() {
    return value;
  }

  public void setValue(FieldSummaryData value) {
    this.value = value;
  }

  public ValueStat getSize() {
    return size;
  }

  public void setSize(ValueStat size) {
    this.size = size;
  }

}
