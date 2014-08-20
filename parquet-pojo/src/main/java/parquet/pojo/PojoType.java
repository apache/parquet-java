package parquet.pojo;

import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Map;

public enum PojoType {
  Input,
  Output;

  void addToConfiguration(Class clazz, Configuration configuration) {
    switch (this) {
      case Input: {
        configuration.set(ParquetPojoConstants.PARQUET_POJO_INPUT_CLASS_KEY, clazz.getName());
        break;
      }
      case Output: {
        configuration.set(ParquetPojoConstants.PARQUET_POJO_OUTPUT_CLASS_KEY, clazz.getName());
      }
    }
  }

  <T> void addListClassToConfiguration(Class<? extends List> listClass, Class<T> valueClass, Configuration configuration) {
    switch (this) {
      case Input: {
        configuration.set(ParquetPojoConstants.PARQUET_POJO_INPUT_CLASS_KEY, listClass.getName());
        configuration.set(ParquetPojoConstants.PARQUET_POJO_INPUT_LIST_VALUE_CLASS, valueClass.getName());
        break;
      }
      case Output: {
        configuration.set(ParquetPojoConstants.PARQUET_POJO_OUTPUT_CLASS_KEY, listClass.getName());
        configuration.set(ParquetPojoConstants.PARQUET_POJO_OUTPUT_LIST_VALUE_CLASS, valueClass.getName());
      }
    }
  }
  <K, V> void addMapClassToConfiguration(Class<? extends Map> mapClass, Class<K> keyClass,
                                                Class<V> valueClass, Configuration configuration) {
    switch (this) {
      case Input: {
        configuration.set(ParquetPojoConstants.PARQUET_POJO_INPUT_CLASS_KEY, mapClass.getName());
        configuration.set(ParquetPojoConstants.PARQUET_POJO_INPUT_MAP_KEY_CLASS, keyClass.getName());
        configuration.set(ParquetPojoConstants.PARQUET_POJO_INPUT_MAP_VALUE_CLASS, valueClass.getName());
        break;
      }
      case Output: {
        configuration.set(ParquetPojoConstants.PARQUET_POJO_OUTPUT_CLASS_KEY, mapClass.getName());
        configuration.set(ParquetPojoConstants.PARQUET_POJO_OUTPUT_MAP_KEY_CLASS, keyClass.getName());
        configuration.set(ParquetPojoConstants.PARQUET_POJO_OUTPUT_MAP_VALUE_CLASS, valueClass.getName());
      }
    }
  }
}
