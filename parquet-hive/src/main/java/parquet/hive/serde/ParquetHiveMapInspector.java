package parquet.hive.serde;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class ParquetHiveMapInspector implements MapObjectInspector {
    private final ObjectInspector keyInspector;
    private final ObjectInspector valueInspector;

    public ParquetHiveMapInspector(final ObjectInspector keyInspector, final ObjectInspector valueInspector) {
        this.keyInspector = keyInspector;
        this.valueInspector = valueInspector;
    }

    @Override
    public String getTypeName() {
        return "ParquetHiveMap<" + keyInspector.getTypeName() + "," + valueInspector.getTypeName() + ">";
    }

    @Override
    public Category getCategory() {
        return Category.MAP;
    }

    @Override
    public ObjectInspector getMapKeyObjectInspector() {
        return keyInspector;
    }

    @Override
    public ObjectInspector getMapValueObjectInspector() {
        return valueInspector;
    }

    @Override
    public Object getMapValueElement(final Object data, final Object key) {

        if (data == null) {
            return null;
        }

        final Writable[] mapArray = ((ArrayWritable) data).get();

        for (final Writable obj : mapArray) {
            final MapWritable mapObj = (MapWritable) obj;
            if (mapObj.get(ParquetHiveSerDe.MAP_KEY) == key) {
                return mapObj.get(ParquetHiveSerDe.MAP_VALUE);
            }
        }

        return null;
    }

    @Override
    public Map<?, ?> getMap(final Object data) {

        if (data == null) {
            return null;
        }

        final Writable[] mapArray = ((ArrayWritable) data).get();
        final Map<Writable, Writable> map = new HashMap<Writable, Writable>();

        for (final Writable obj : mapArray) {
            final MapWritable mapObj = (MapWritable) obj;
            map.put(mapObj.get(ParquetHiveSerDe.MAP_KEY), mapObj.get(ParquetHiveSerDe.MAP_VALUE));
        }

        return map;
    }

    @Override
    public int getMapSize(final Object data) {
        return ((ArrayWritable) data).get().length;
    }

}
