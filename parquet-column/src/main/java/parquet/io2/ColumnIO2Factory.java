/**
 * Copyright 2014 GoDaddy, Inc.
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
package parquet.io2;

import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;
import parquet.schema.TypeVisitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Factory that supports type coercions in two directions:
 *
 * a) Lifting
 * b) Extracted
 *
 * Lifting conversions allow a requested schema to have additional
 * nodes compared to the file schema assuming that the leaf nodes
 * maintain invariants around repetition and definition counts.
 * This mode is primarily meant to allow Pig and Hive to load
 * files that were written using parquet-protobuf or some other
 * writer that does not have a first class notion of nullable values
 * but it needs to be read into a schema that does. These conversions
 * are safe at runtime as they do not lose or discard any information.
 *
 * Extracted conversions go the other direction. Such conversions
 * are not always safe at runtime and are disabled by default.
 */
public class ColumnIO2Factory {
  Option<GroupColumnIO2<GroupType>> groupType(
      final GroupType fileGroupType,
      final GroupType requestedGroupType,
      final LeafInfo leafInfo) {

    // find suitable matching nodes in the disk schema for
    // for each child node in the requested schema
    final ArrayList<ColumnIO2<?>> children = new ArrayList<ColumnIO2<?>>(requestedGroupType.getFieldCount());
    final List<Type> requestedFields = requestedGroupType.getFields();
    for (int i = 0; i < requestedGroupType.getFieldCount(); ++i) {
      final int ii = i;
      final Type requestedField = requestedFields.get(ii);

      // attempt various matching strategies until the first match is returned using a breadth first
      // search. this doesn't guarantee that the best match is found, just a suitable match.
      Option
          .<ColumnIO2<?>>empty()
          .or(new Function<Void, Option<ColumnIO2<?>>>() {
            @Override
            public Option<ColumnIO2<?>> call(Void unit) {
              // a field name matches, check to see if the schemas are compatible
              if (!fileGroupType.containsField(requestedField.getName())) {
                return Option.empty();
              }

              final int fileFieldIndex = fileGroupType.getFieldIndex(requestedField.getName());
              final Type fileField = fileGroupType.getType(fileFieldIndex);

              final ColumnPath logicalPath = new ColumnPath(requestedField, ii);
              final ColumnPath physicalPath = new ColumnPath(fileField, fileFieldIndex);
              final LeafInfo columnLeaf = leafInfo.combine(new LeafInfo(physicalPath, logicalPath));

              return create(fileField, requestedField, columnLeaf);
            }
          })
          .or(new Function<Void, Option<ColumnIO2<?>>>() {
            @Override
            public Option<ColumnIO2<?>> call(Void unit) {
              // lifted match test
              // since there is not an exact or subtype match we will check for compatible types that require lifting
              if (requestedField.isPrimitive()) {
                // carry on the search, look for extraction matches next
                return Option.empty();
              }

              final GroupType parent = requestedField.asGroupType();
              if (parent.getFieldCount() != 1) {
                // lifting candidates are groups with a single field
                return Option.empty();
              }

              final Type requestedField = parent.getFields().get(0);
              final GroupType liftedType = new GroupType(parent.getRepetition(), parent.getName(), requestedField);
              final int repCorrections = repetitionCorrection(parent, fileGroupType);
              final ColumnPath logicalPath = new ColumnPath(parent, ii, repCorrections, 0);
              // leave the physical path empty
              final LeafInfo columnLeaf = leafInfo.combine(new LeafInfo(ColumnPath.empty, logicalPath));

              return groupType(
                  fileGroupType,
                  liftedType,
                  columnLeaf)
                  .map(new Function<GroupColumnIO2<GroupType>, ColumnIO2<?>>() {
                    @Override
                    public ColumnIO2<?> call(final GroupColumnIO2<GroupType> value) {
                      return value;
                    }
                  });
            }
          })
          .or(new Function<Void, Option<ColumnIO2<?>>>() {
            @Override
            public Option<ColumnIO2<?>> call(Void unit) {
              // TODO: extracted match
              return Option.empty();
            }
          })
          .or(new Function<Void, Option<ColumnIO2<?>>>() {
            @Override
            public Option<ColumnIO2<?>> call(Void unit) {
              if (requestedField.isPrimitive()) {
                return Option.empty();
              }

              final GroupType renamed = requestedField.asGroupType();
              final int repCorrections = repetitionCorrection(renamed, fileGroupType);
              final ColumnPath logicalPath = new ColumnPath(renamed, ii, repCorrections, 0);
              // leave the physical path empty
              final LeafInfo columnLeaf = leafInfo.combine(new LeafInfo(ColumnPath.empty, logicalPath));

              return groupType(
                  fileGroupType,
                  renamed,
                  columnLeaf)
                  .map(new Function<GroupColumnIO2<GroupType>, ColumnIO2<?>>() {
                    @Override
                    public ColumnIO2<?> call(final GroupColumnIO2<GroupType> value) {
                      return value;
                    }
                  });
            }
          })
          .map(new Function<ColumnIO2<?>, Void>() {
            @Override
            public Void call(final ColumnIO2<?> child) {
              children.add(child);
              return null;
            }
          });
    }

    if (children.isEmpty()) {
      // continue searching other paths
      return Option.empty();
    }

    final ArrayList<Type> matchedTypes = new ArrayList<Type>(children.size());
    for (final ColumnIO2<?> child : children) {
      matchedTypes.add(child.getType());
    }

    final GroupType actualType = new GroupType(
        requestedGroupType.getRepetition(),
        requestedGroupType.getName(),
        matchedTypes);

    return Option.pure(
        new GroupColumnIO2<GroupType>(
            actualType,
            actualType.getName(),
            leafInfo,
            Collections.unmodifiableList(children)));
  }

  Option<PrimitiveColumnIO2> primitiveType(
      final PrimitiveType fileSchema,
      final PrimitiveType requestedSchema,
      final LeafInfo leafInfo) {
    if (!fileSchema.getName().equals(requestedSchema.getName())) {
      return Option.empty();
    }

    if (!fileSchema.getPrimitiveTypeName().equals(requestedSchema.getPrimitiveTypeName())) {
      // this would be a convenient place to check for legal widening conversions
      return Option.empty();
    }

    if (leafInfo.getPhysicalPath().getRepetitionLevel() != leafInfo.getLogicalPath().getRepetitionLevel()) {
      // TODO: figure out how to deal with required vs optional vs repeated?
      return Option.empty();
    }

    if (leafInfo.getPhysicalPath().getDefinitionLevel() != leafInfo.getLogicalPath().getDefinitionLevel()) {
      // TODO: figure out how to deal with required vs optional vs repeated?
      return Option.empty();
    }

    return Option.pure(
        new PrimitiveColumnIO2(
            fileSchema,
            fileSchema.getName(),
            leafInfo));
  }

  public Option<MessageColumnIO2> messageType(
      final MessageType fileSchema,
      final MessageType requestedSchema) {
    return create(fileSchema, requestedSchema, LeafInfo.empty)
        .map(new Function<ColumnIO2<?>, MessageColumnIO2>() {
          @Override
          public MessageColumnIO2 call(ColumnIO2<?> value) {
            // yuck
            value.setParent(null);
            return (MessageColumnIO2) value;
          }
        });
  }

  private static int repetitionCorrection(final Type fileType, final Type requestedType) {
    if (requestedType.getRepetition() == fileType.getRepetition() &&
        fileType.getRepetition() == Type.Repetition.REPEATED) {
      return 1;
    } else {
      return 0;
    }
  }

  Option<ColumnIO2<?>> create(
      final Type fileSchema,
      final Type requestedSchema,
      final LeafInfo leafInfo) {
    return requestedSchema.accept(new TypeVisitor<Option<ColumnIO2<?>>>() {
      @Override
      public Option<ColumnIO2<?>> visit(final GroupType requestedGroupType) {
        return fileSchema.accept(new TypeVisitor<Option<ColumnIO2<?>>>() {
          @Override
          public Option<ColumnIO2<?>> visit(final GroupType fileGroupType) {
            return groupType(fileGroupType, requestedGroupType, leafInfo)
                .map(new Function<GroupColumnIO2<GroupType>, ColumnIO2<?>>() {
                  @Override
                  public ColumnIO2<?> call(GroupColumnIO2<GroupType> value) {
                    return value;
                  }
                });
          }

          @Override
          public Option<ColumnIO2<?>> visit(final MessageType fileMessageType) {
            // TODO: check to see if it needs to be wrapped in a MessageColumnIO2?
            throw new UnsupportedOperationException();
          }

          @Override
          public Option<ColumnIO2<?>> visit(final PrimitiveType filePrimitiveType) {
            // see if there is a compatible group representation
            return visit(new GroupType(
                filePrimitiveType.getRepetition(),
                filePrimitiveType.getName(),
                new PrimitiveType(
                    Type.Repetition.REQUIRED,
                    filePrimitiveType.getPrimitiveTypeName(),
                    filePrimitiveType.getTypeLength(),
                    filePrimitiveType.getName(),
                    filePrimitiveType.getOriginalType())));
          }
        });
      }

      @Override
      public Option<ColumnIO2<?>> visit(final MessageType requestedMessageType) {
        return fileSchema.accept(new TypeVisitor<Option<ColumnIO2<?>>>() {
          @Override
          public Option<ColumnIO2<?>> visit(final GroupType fileGroupType) {
            return visit(new MessageType(
                fileGroupType.getName(),
                fileGroupType.getFields()));
          }

          @Override
          public Option<ColumnIO2<?>> visit(final MessageType fileMessageType) {
            return groupType(fileMessageType, requestedMessageType, leafInfo)
                .map(new Function<GroupColumnIO2<GroupType>, ColumnIO2<?>>() {
                  @Override
                  public ColumnIO2<?> call(final GroupColumnIO2<GroupType> groupColumnIO) {
                    return new MessageColumnIO2(
                        requestedMessageType,
                        groupColumnIO.getName(),
                        leafInfo,
                        groupColumnIO.getChildren());
                  }
                });
          }

          @Override
          public Option<ColumnIO2<?>> visit(final PrimitiveType filePrimitiveType) {
            return Option.empty();
          }
        });
      }

      @Override
      public Option<ColumnIO2<?>> visit(final PrimitiveType requestedPrimitiveType) {
        return fileSchema.accept(new TypeVisitor<Option<ColumnIO2<?>>>() {
          @Override
          public Option<ColumnIO2<?>> visit(final GroupType fileGroupType) {
            // requesting a primitive but file schema is a group
            return Option.empty();
          }

          @Override
          public Option<ColumnIO2<?>> visit(final MessageType fileMessageType) {
            // requesting a primitive but file schema is a message
            return Option.empty();
          }

          @Override
          public Option<ColumnIO2<?>> visit(final PrimitiveType filePrimitiveType) {
            return primitiveType(filePrimitiveType, requestedPrimitiveType, leafInfo)
                .map(new Function<PrimitiveColumnIO2, ColumnIO2<?>>() {
                  @Override
                  public ColumnIO2<?> call(PrimitiveColumnIO2 value) {
                    return value;
                  }
                });
          }
        });
      }
    });
  }
}
