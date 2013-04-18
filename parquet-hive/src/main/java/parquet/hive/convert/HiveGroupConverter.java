/**
 * Copyright 2013 Criteo.
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
