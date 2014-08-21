package parquet.filter2.recordlevel;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.ValueInspector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static parquet.filter2.recordlevel.TestIncrementallyUpdatedFilterPredicateEvaluator.intIsEven;

public class TestValueInspector {

  @Test
  public void testLifeCycle() {
    ValueInspector v = intIsEven();

    // begins in unknown state
    assertFalse(v.isKnown());
    // calling getResult in unknown state throws
    try {
      v.getResult();
      fail("this should throw");
    } catch (IllegalStateException e) {
      assertEquals("getResult() called on a ValueInspector whose result is not yet known!", e.getMessage());
    }

    // update state to known
    v.update(10);

    // v was updated with value 10, so result is known and should be true
    assertTrue(v.isKnown());
    assertTrue(v.getResult());

    // calling update w/o resetting should throw
    try {
      v.update(11);
      fail("this should throw");
    } catch (IllegalStateException e) {
      assertEquals("setResult() called on a ValueInspector whose result is already known!"
          + " Did you forget to call reset()?", e.getMessage());
    }

    // back to unknown state
    v.reset();

    assertFalse(v.isKnown());
    // calling getResult in unknown state throws
    try {
      v.getResult();
      fail("this should throw");
    } catch (IllegalStateException e) {
      assertEquals("getResult() called on a ValueInspector whose result is not yet known!", e.getMessage());
    }

    // v was updated with value 11, so result is known and should be false
    v.update(11);
    assertTrue(v.isKnown());
    assertFalse(v.getResult());

  }

  @Test
  public void testReusable() {
    List<Integer> values = Arrays.asList(2, 4, 7, 3, 8, 8, 11, 200);
    ValueInspector v = intIsEven();

    for (Integer x : values) {
      v.update(x);
      assertEquals(x % 2 == 0, v.getResult());
      v.reset();
    }

  }
}
