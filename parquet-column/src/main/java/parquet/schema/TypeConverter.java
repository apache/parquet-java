package parquet.schema;

import java.util.List;

/**
 * to convert a MessageType tree
 * @see Type#convert(TypeConverter)
 *
 * @author Julien Le Dem
 *
 * @param <T> the resulting Type
 */
public interface TypeConverter<T> {

  /**
   * @param path the path to that node
   * @param primitiveType the type to convert
   * @return the result of conversion
   */
  T convertPrimitiveType(List<GroupType> path, PrimitiveType primitiveType);

  /**
   * @param path the path to that node
   * @param groupType the type to convert
   * @param children its children already converted
   * @return the result of conversion
   */
  T convertGroupType(List<GroupType> path, GroupType groupType, List<T> children);

  /**
   * @param messageType the type to convert
   * @param children its children already converted
   * @return the result of conversion
   */
  T convertMessageType(MessageType messageType, List<T> children);

}
