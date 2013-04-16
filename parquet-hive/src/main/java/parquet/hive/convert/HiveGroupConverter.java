package parquet.hive.convert;

import org.apache.hadoop.io.Writable;

import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.schema.Type;
import parquet.schema.Type.Repetition;

public abstract class HiveGroupConverter extends GroupConverter {
    //    private static final Log LOG = Log.getLog(HiveGroupConverter.class);

    static protected Converter getConverterFromDescription(final Type type, final int index, final HiveGroupConverter parent) {
        if (type == null) {
            return null;
        }

        if (type.isPrimitive()) {
            return ETypeConverter.getNewConverter(type.asPrimitiveType().getPrimitiveTypeName().javaType, index, parent);
        } else {
            if (type.asGroupType().getRepetition() == Repetition.REPEATED) {
                //                LOG.info("getting array converter " + type);
                return new ArrayWritableGroupConverter(type.asGroupType(), parent, index);
            } else {
                //                LOG.info("getting map converter " + type);
                return new MapWritableGroupConverter(type.asGroupType(), parent, index);
            }
        }
    }

    abstract protected void set(int index, Writable value);

}
