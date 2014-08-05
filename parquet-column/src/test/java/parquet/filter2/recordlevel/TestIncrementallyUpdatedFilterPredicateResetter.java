package parquet.filter2.recordlevel;


import org.junit.Test;

import parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.And;
import parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.Or;
import parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.ValueInspector;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static parquet.filter2.recordlevel.TestIncrementallyUpdatedFilterPredicateEvaluator.doubleMoreThan10;
import static parquet.filter2.recordlevel.TestIncrementallyUpdatedFilterPredicateEvaluator.intIsEven;
import static parquet.filter2.recordlevel.TestIncrementallyUpdatedFilterPredicateEvaluator.intIsNull;

public class TestIncrementallyUpdatedFilterPredicateResetter {
  @Test
  public void testReset() {

    ValueInspector intIsNull = intIsNull();
    ValueInspector intIsEven = intIsEven();
    ValueInspector doubleMoreThan10 = doubleMoreThan10();

    IncrementallyUpdatedFilterPredicate pred = new Or(intIsNull, new And(intIsEven, doubleMoreThan10));

    intIsNull.updateNull();
    intIsEven.update(11);
    doubleMoreThan10.update(20.0D);

    assertTrue(intIsNull.isKnown());
    assertTrue(intIsEven.isKnown());
    assertTrue(doubleMoreThan10.isKnown());

    IncrementallyUpdatedFilterPredicateResetter.reset(pred);

    assertFalse(intIsNull.isKnown());
    assertFalse(intIsEven.isKnown());
    assertFalse(doubleMoreThan10.isKnown());

    intIsNull.updateNull();
    assertTrue(intIsNull.isKnown());
    assertFalse(intIsEven.isKnown());
    assertFalse(doubleMoreThan10.isKnown());

    IncrementallyUpdatedFilterPredicateResetter.reset(pred);
    assertFalse(intIsNull.isKnown());
    assertFalse(intIsEven.isKnown());
    assertFalse(doubleMoreThan10.isKnown());

  }
}
