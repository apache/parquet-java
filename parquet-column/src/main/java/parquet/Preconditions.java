package parquet;

/**
 * Utility for parameter validation
 *
 * @author Julien Le Dem
 *
 */
public class Preconditions {

  /**
   * @param o the param to check
   * @param name the name of the param for the error message
   * @return the validated o
   * @throws NullPointerException if o is null
   */
  public static <T> T checkNotNull(T o, String name) {
    if (o == null) {
      throw new NullPointerException(name + " should not be null");
    }
    return o;
  }

}
