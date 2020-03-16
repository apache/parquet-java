package org.apache.parquet.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext;
import org.apache.parquet.hadoop.util.ConfigurationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.parquet.Preconditions.checkNotNull;

public abstract class CryptoPropertiesFactory {

  private static final Logger LOG = LoggerFactory.getLogger(CryptoPropertiesFactory.class);
  public static final String CRYPTO_FACTORY_CLASS_PROPERTY_NAME = "parquet.encryption.factory.class";

  public static CryptoPropertiesFactory loadFactory(Configuration conf) throws IOException {
    if (null == conf) {
      LOG.debug("CryptoPropertiesFactory is not configured - null hadoop config");
      return null;
    }

    final Class<?> cryptoPropertiesFactoryClass = ConfigurationUtil.getClassFromConfig(conf,
      CRYPTO_FACTORY_CLASS_PROPERTY_NAME, CryptoPropertiesFactory.class);

    if (null == cryptoPropertiesFactoryClass) {
      LOG.debug("CryptoPropertiesFactory is not configured - name not found in hadoop config");
      return null;
    }

    try {
      CryptoPropertiesFactory cryptoFactory = (CryptoPropertiesFactory)cryptoPropertiesFactoryClass.newInstance();
      cryptoFactory.initialize(conf);
      return cryptoFactory;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new BadConfigurationException("could not instantiate CryptoPropertiesFactory class: "
        + cryptoPropertiesFactoryClass, e);
    }
  }

  public abstract void initialize(Configuration hadoopConfig);

  public abstract FileEncryptionProperties getFileEncryptionProperties(
    Configuration fileHadoopConfig, Path tempFilePath,
    WriteContext fileWriteContext)  throws IOException;

  public abstract FileDecryptionProperties getFileDecryptionProperties(
    Configuration hadoopConfig, Path filePath)  throws IOException;
}
