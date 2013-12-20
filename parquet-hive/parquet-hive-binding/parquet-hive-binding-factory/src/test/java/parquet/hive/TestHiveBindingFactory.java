package parquet.hive;

import org.junit.Assert;

import org.junit.Before;
import org.junit.Test;

import parquet.hive.internal.Hive010Binding;
import parquet.hive.internal.Hive012Binding;
import parquet.hive.HiveBindingFactory.UnexpectedHiveVersionProviderError;

public class TestHiveBindingFactory {
  private HiveBindingFactory hiveBindingFactory;

  @Before
  public void setup() {
    hiveBindingFactory = new HiveBindingFactory();
  }
  @Test
  public void testMissingHiveVersionInfoClass() {
    Assert.assertEquals(Hive010Binding.class, hiveBindingFactory.
        create(new NoopClassLoader()));
  }
  @Test(expected=UnexpectedHiveVersionProviderError.class)
  public void testNoHiveVersion() {
    hiveBindingFactory.createInternal(NoHiveVersion.class);
  }
  @Test
  public void testBlankHiveVersion() {
    hiveBindingFactory.createInternal(BlankHiveVersion.class);
    Assert.assertEquals(Hive012Binding.class, hiveBindingFactory.
        createInternal(BlankHiveVersion.class));
  }
  @Test
  public void testUnknownHiveVersion() {
    hiveBindingFactory.createInternal(BlankHiveVersion.class);
    // returns 0.12 because we don't have hive in classpath
    Assert.assertEquals(Hive012Binding.class, hiveBindingFactory.
        createInternal(BlankHiveVersion.class));
  }
  @Test
  public void testNullHiveVersion() {
    hiveBindingFactory.createInternal(NullHiveVersion.class);
    Assert.assertEquals(Hive012Binding.class, hiveBindingFactory.
        createInternal(NullHiveVersion.class));
  }
  @Test
  public void testHive010() {
    Assert.assertEquals(Hive010Binding.class, hiveBindingFactory.
        createInternal(Hive010Version.class));
  }
  @Test
  public void testHive010WithSpaces() {
    Assert.assertEquals(Hive010Binding.class, hiveBindingFactory.
        createInternal(Hive010VersionWithSpaces.class));
  }
  @Test
  public void testHive011() {
    Assert.assertEquals(Hive010Binding.class, hiveBindingFactory.
        createInternal(Hive011Version.class));
  }
  @Test
  public void testHive012() {
    Assert.assertEquals(Hive012Binding.class, hiveBindingFactory.
        createInternal(Hive012Version.class));
  }
  @Test
  public void testHive013() {
    Assert.assertEquals(Hive012Binding.class, hiveBindingFactory.
        createInternal(Hive013Version.class));
  }

  static class NoopClassLoader extends ClassLoader {
    public Class<?> loadClass(String name) throws ClassNotFoundException {
      throw new ClassNotFoundException(name);
    }    
  }
  static class NoHiveVersion {
    
  }
  static class BlankHiveVersion {
    public static String getVersion() {
      return "";
    } 
  }
  static class UnknownHiveVersion {
    public static String getVersion() {
      return HiveBindingFactory.HIVE_VERSION_UNKNOWN;
    } 
  }
  static class NullHiveVersion {
    public static String getVersion() {
      return null;
    } 
  }
  static class Hive010Version {
    public static String getVersion() {
      return HiveBindingFactory.HIVE_VERSION_010;
    } 
  }
  static class Hive010VersionWithSpaces {
    public static String getVersion() {
      return " " + HiveBindingFactory.HIVE_VERSION_010 + " ";
    } 
  }
  static class Hive011Version {
    public static String getVersion() {
      return HiveBindingFactory.HIVE_VERSION_011;
    } 
  }
  static class Hive012Version {
    public static String getVersion() {
      return HiveBindingFactory.HIVE_VERSION_012;
    } 
  }
  static class Hive013Version {
    public static String getVersion() {
      return HiveBindingFactory.HIVE_VERSION_013;
    } 
  }
}
