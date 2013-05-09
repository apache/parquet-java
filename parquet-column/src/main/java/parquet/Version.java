package parquet;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.Properties;
import java.util.jar.Manifest;

/**
 * The version of the library
 *
 * parquet-mr version 1.0.0-SNAPSHOT (build 6cf94d29b2b7115df4de2c06e2ab4326d721eb55)
 *
 * @author Julien Le Dem
 *
 */
public class Version {
  private static final Log LOG = Log.getLog(Version.class);

  public static final String FULL_VERSION = readVersion();

  private static String getJarPath() {
    final URL versionClassBaseUrl = Version.class.getResource("");
    if (versionClassBaseUrl.getProtocol().equals("jar")) {
      String path = versionClassBaseUrl.getPath();
      int jarEnd = path.indexOf("!");
      if (jarEnd != -1) {
        String jarPath = path.substring(0, jarEnd);
        return jarPath;
      }
    }
    return null;
  }

  private static URL getResourceFromJar(String jarPath, String path) throws IOException {
    Enumeration<URL> resources = Version.class.getClassLoader().getResources(path);
    while (resources.hasMoreElements()) {
      URL url = resources.nextElement();
      if (url.getProtocol().equals("jar") && url.getPath().startsWith(jarPath)) {
        return url;
      }
    }
    return null;
  }

  private static String readVersion() {
    String version = null;
    String sha = null;
    try {
      String jarPath = getJarPath();
      if (jarPath != null) {
        URL pomPropertiesUrl = getResourceFromJar(jarPath, "META-INF/maven/com.twitter/parquet-column/pom.properties");
        if (pomPropertiesUrl != null) {
          Properties properties = new Properties();
          properties.load(pomPropertiesUrl.openStream());
          version = properties.getProperty("version");
        }
        URL manifestUrl = getResourceFromJar(jarPath, "META-INF/MANIFEST.MF");
        if (manifestUrl != null) {
          Manifest manifest = new Manifest(manifestUrl.openStream());
          sha = manifest.getMainAttributes().getValue("git-SHA-1");
        }
      }
    } catch (Exception e) {
      LOG.warn("can't read from META-INF", e);
    }
    return "parquet-mr" + (version != null ? " version " + version : "") + (sha != null ? " (build " + sha + ")" : "");
  }

  public static void main(String[] args) {
    System.out.println(FULL_VERSION);
  }
}
