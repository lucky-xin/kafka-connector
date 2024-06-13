package xyz.kafka.connector.validator;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * Url校验器
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2022-12-29
 */
public class UrlListValidator implements ConfigDef.Validator {

  @SuppressWarnings("unchecked")
  @Override
  public void ensureValid(String name, Object value) {
    if (value == null) {
      throw new ConfigException(name, null, "The config must be provided and non-null.");
    }

    List<String> urls = (List<String>) value;
    for (String url : urls) {
      try {
        new URI(url);
      } catch (URISyntaxException e) {
        throw new ConfigException(
            name, value, "The provided url '" + url + "' is not a valid url.");
      }
    }
  }

  @Override
  public String toString() {
    return "List of valid URIs.";
  }
}
