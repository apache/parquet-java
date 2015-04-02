/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package parquet.filter2.recordlevel;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import parquet.column.Dictionary;
import parquet.filter2.predicate.Operators;
import parquet.io.api.Binary;
import parquet.io.api.PrimitiveConverter;
import parquet.schema.PrimitiveType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.mockito.Mockito.mock;
import static parquet.filter2.predicate.FilterApi.eq;
import static parquet.filter2.predicate.FilterApi.*;

@RunWith(Parameterized.class)
public class TestFilteringPrimitiveConverter {
    private PrimitiveConverter primitiveConverter = mock(PrimitiveConverter.class);
    private Dictionary dictionary = mock(Dictionary.class);

    private PrimitiveType.PrimitiveTypeName primitiveTypeName;
    private Object value;
    private IncrementallyUpdatedFilterPredicate.ValueInspector eqInspector;
    private IncrementallyUpdatedFilterPredicate.ValueInspector notEqInspector;
    private IncrementallyUpdatedFilterPredicate.ValueInspector gtInspector;
    private IncrementallyUpdatedFilterPredicate.ValueInspector ltInspector;
    private IncrementallyUpdatedFilterPredicate.ValueInspector[] inspectors;
    private final int DICT_INDEX = 0;

    @Parameterized.Parameters(name = "{0}")
    public static Collection typeSpecificFields() {
        Long longVal = 2L;
        Integer intVal = 3;
        Boolean boolVal = true;
        Float floatVal = 2.0F;
        Double doubleVal = 2.0;
        Binary binVal = Binary.fromString("bin");
        Operators.LongColumn longColumn = longColumn("a");
        Operators.IntColumn intColumn = intColumn("a");
        Operators.BooleanColumn booleanColumn = booleanColumn("a");
        Operators.BinaryColumn binaryColumn = binaryColumn("a");
        Operators.FloatColumn floatColumn = floatColumn("a");
        Operators.DoubleColumn doubleColumn = doubleColumn("a");

        return Arrays.asList(new Object[][]{
                {PrimitiveType.PrimitiveTypeName.INT64, longVal,
                        eq(longColumn, longVal), notEq(longColumn, longVal), gt(longColumn, longVal - 1), lt(longColumn, longVal + 1)},
                {PrimitiveType.PrimitiveTypeName.INT32, intVal,
                        eq(intColumn, intVal), notEq(intColumn, intVal), gt(intColumn, intVal - 1), lt(intColumn, intVal + 1)},
                {PrimitiveType.PrimitiveTypeName.BOOLEAN, boolVal,
                        eq(booleanColumn, boolVal), notEq(booleanColumn, boolVal), null, null} ,
                {PrimitiveType.PrimitiveTypeName.BINARY, binVal,
                        eq(binaryColumn, binVal), notEq(binaryColumn, binVal), null, null},
                {PrimitiveType.PrimitiveTypeName.FLOAT, floatVal,
                        eq(floatColumn, floatVal), notEq(floatColumn, floatVal), gt(floatColumn, floatVal - 1), lt(floatColumn, floatVal + 1)},
                {PrimitiveType.PrimitiveTypeName.DOUBLE, doubleVal,
                        eq(doubleColumn, doubleVal), notEq(doubleColumn, doubleVal), gt(doubleColumn, doubleVal - 1), lt(doubleColumn, doubleVal + 1)},
                {PrimitiveType.PrimitiveTypeName.INT96, binVal,
                        eq(binaryColumn, binVal), notEq(binaryColumn, binVal), null, null},
                {PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, binVal,
                        eq(binaryColumn, binVal), notEq(binaryColumn, binVal), null, null}
        });
    }

    public TestFilteringPrimitiveConverter(PrimitiveType.PrimitiveTypeName primitiveTypeName, Object value,
                                           Operators.Eq eq, Operators.NotEq notEq, Operators.Gt gt, Operators.Lt lt) {
        this.primitiveTypeName = primitiveTypeName;
        this.value = value;
        List<IncrementallyUpdatedFilterPredicate.ValueInspector> inspectorList = new ArrayList<IncrementallyUpdatedFilterPredicate.ValueInspector>();
        IncrementallyUpdatedFilterPredicateBuilder builder = new IncrementallyUpdatedFilterPredicateBuilder();

        eqInspector = (IncrementallyUpdatedFilterPredicate.ValueInspector)builder.visit(eq);
        inspectorList.add(eqInspector);
        notEqInspector = (IncrementallyUpdatedFilterPredicate.ValueInspector)builder.visit(notEq);
        inspectorList.add(notEqInspector);

        if (gt != null) {
            gtInspector = (IncrementallyUpdatedFilterPredicate.ValueInspector)builder.visit(gt);
            inspectorList.add(gtInspector);
        }
        if (lt != null) {
            ltInspector = (IncrementallyUpdatedFilterPredicate.ValueInspector)builder.visit(lt);
            inspectorList.add(ltInspector);
        }

        inspectors = inspectorList.toArray(new IncrementallyUpdatedFilterPredicate.ValueInspector[0]);
        mockUpDictionaryDecodeToType(value);
    }


    @Test public void equalityAndInequalityEvaluatedCorrectly() {

        when(primitiveConverter.hasDictionarySupport()).thenReturn(true);
        when(dictionary.getMaxId()).thenReturn(DICT_INDEX);

        FilteringPrimitiveConverter filteringPrimitiveConverter =
                new FilteringPrimitiveConverter(primitiveConverter,inspectors);

        filteringPrimitiveConverter.setDictionary(dictionary);
        filteringPrimitiveConverter.addValueFromDictionary(DICT_INDEX);

        assertTrue(eqInspector.getResult());
        assertFalse(notEqInspector.getResult());
        if (gtInspector != null) assertTrue(gtInspector.getResult());
        if (ltInspector != null) assertTrue(ltInspector.getResult());
    }

    @Test public void passesDictionaryMethodsThroughToDelegate() {
        when(primitiveConverter.hasDictionarySupport()).thenReturn(true);

        FilteringPrimitiveConverter filteringPrimitiveConverter =
                new FilteringPrimitiveConverter(primitiveConverter, inspectors);
        filteringPrimitiveConverter.setDictionary(dictionary);
        filteringPrimitiveConverter.addValueFromDictionary(DICT_INDEX);

        verify(primitiveConverter).setDictionary(dictionary);
        verify(primitiveConverter).addValueFromDictionary(DICT_INDEX);
    }

    @Test public void callsTypeSpecificAddWhenDelegateDoesNotSupportDictionary() {
        when(primitiveConverter.hasDictionarySupport()).thenReturn(false);

        when(dictionary.getPrimitiveTypeName()).thenReturn(primitiveTypeName);

        FilteringPrimitiveConverter filteringPrimitiveConverter =
                new FilteringPrimitiveConverter(primitiveConverter, inspectors);
        filteringPrimitiveConverter.setDictionary(dictionary);
        filteringPrimitiveConverter.addValueFromDictionary(DICT_INDEX);


        if (value instanceof Long) {
            verify(primitiveConverter).addLong((Long) value);
        } else if (value instanceof Integer) {
            verify(primitiveConverter).addInt((Integer) value);
        } else if (value instanceof Boolean) {
            verify(primitiveConverter).addBoolean((Boolean) value);
        } else if (value instanceof Binary) {
            verify(primitiveConverter).addBinary((Binary) value);
        } else if (value instanceof Float) {
            verify(primitiveConverter).addFloat((Float) value);
        } else if (value instanceof Double) {
            verify(primitiveConverter).addDouble((Double) value);
        }
    }


    private void mockUpDictionaryDecodeToType(Object value) {
        if (value instanceof Long) {
            when(dictionary.decodeToLong(DICT_INDEX)).thenReturn((Long) value);
        } else if (value instanceof Integer) {
            when(dictionary.decodeToInt(DICT_INDEX)).thenReturn((Integer) value);
        } else if (value instanceof Boolean) {
            when(dictionary.decodeToBoolean(DICT_INDEX)).thenReturn((Boolean) value);
        } else if (value instanceof Binary) {
            when(dictionary.decodeToBinary(DICT_INDEX)).thenReturn((Binary) value);
        } else if (value instanceof Float) {
            when(dictionary.decodeToFloat(DICT_INDEX)).thenReturn((Float) value);
        } else if (value instanceof Double) {
            when(dictionary.decodeToDouble(DICT_INDEX)).thenReturn((Double) value);
        }
    }
}
