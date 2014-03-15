/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.pojo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import parquet.Preconditions;
import parquet.hadoop.util.ContextUtil;
import parquet.pojo.field.FieldUtils;

import java.util.List;
import java.util.Map;

public class SchemaUtils {
  public enum Type {
    Input,
    Output
  }

  /**
   * Encodes the class to be used in writing into the job conf, this is then written into the file header
   *
   * @param job   configuration for the job
   * @param clazz the class who's name will be serialized
   */
  public static void setSchemaClass(Type type, Job job, Class clazz) {
    Preconditions.checkArgument(!FieldUtils.isMap(clazz), "Use setMapSchemaClass for map classes.");
    Preconditions.checkArgument(!FieldUtils.isList(clazz), "Use setListSchemaClass for list classes.");

    Configuration configuration = ContextUtil.getConfiguration(job);

    switch (type) {
      case Input: {
        configuration.set(ParquetPojoConstants.PARQUET_POJO_INPUT_CLASS_KEY, clazz.getName());
      }
      case Output: {
        configuration.set(ParquetPojoConstants.PARQUET_POJO_OUTPUT_CLASS_KEY, clazz.getName());
      }
    }
  }

  public static void setMapSchemaClass(
    Type type, Job job, Class<? extends Map> clazz, Class keyClass, Class valueClass
  ) {
    Preconditions.checkArgument(
      FieldUtils.isMap(clazz),
      "Method only valid on classes that implement java.util.Map. You must use a concrete implementation."
    );
    Preconditions.checkArgument(clazz != Map.class, "Cannot use java.util.Map on top level fields");

    Configuration configuration = ContextUtil.getConfiguration(job);

    switch (type) {
      case Input: {
        configuration.set(ParquetPojoConstants.PARQUET_POJO_INPUT_CLASS_KEY, clazz.getName());
        configuration.set(ParquetPojoConstants.PARQUET_POJO_INPUT_MAP_KEY_CLASS, keyClass.getName());
        configuration.set(ParquetPojoConstants.PARQUET_POJO_INPUT_MAP_VALUE_CLASS, valueClass.getName());
      }
      case Output: {
        configuration.set(ParquetPojoConstants.PARQUET_POJO_OUTPUT_CLASS_KEY, clazz.getName());
        configuration.set(ParquetPojoConstants.PARQUET_POJO_OUTPUT_MAP_KEY_CLASS, keyClass.getName());
        configuration.set(ParquetPojoConstants.PARQUET_POJO_OUTPUT_MAP_VALUE_CLASS, valueClass.getName());
      }
    }
  }

  public static void setListSchemaClass(Type type, Job job, Class<? extends List> clazz, Class valueClass) {
    Preconditions.checkArgument(
      FieldUtils.isList(clazz), "Method only valid on classes that implement java.util.List"
    );
    Preconditions.checkArgument(
      clazz != List.class,
      "Cannot use java.util.List on top level fields. You must use a concrete implementation."
    );

    Configuration configuration = ContextUtil.getConfiguration(job);

    switch (type) {
      case Input: {
        configuration.set(ParquetPojoConstants.PARQUET_POJO_INPUT_CLASS_KEY, clazz.getName());
        configuration.set(ParquetPojoConstants.PARQUET_POJO_INPUT_LIST_VALUE_CLASS, valueClass.getName());
      }
      case Output: {
        configuration.set(ParquetPojoConstants.PARQUET_POJO_OUTPUT_CLASS_KEY, clazz.getName());
        configuration.set(ParquetPojoConstants.PARQUET_POJO_OUTPUT_LIST_VALUE_CLASS, valueClass.getName());
      }
    }
  }
}