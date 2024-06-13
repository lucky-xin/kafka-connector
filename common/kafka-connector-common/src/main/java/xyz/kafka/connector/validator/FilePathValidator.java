package xyz.kafka.connector.validator;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.io.File;

/**
 * 文件路径校验器
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2022-12-29
 */
public class FilePathValidator implements ConfigDef.Validator {

  private final String extension;

  public FilePathValidator(String extension) {
    this.extension = extension;
  }

  @Override
  public void ensureValid(String name, Object value) {
    if (value == null) {
      return;
    }

    if (extension != null && !extension.isEmpty() && !((String) value).endsWith(extension)) {
      throw new ConfigException(name, value, "The specified file must end with ." + extension);
    }

    File file = new File((String) value);
    if (!file.exists()) {
      throw new ConfigException(name, value, "The specified file does not exist.");
    }
  }
}
