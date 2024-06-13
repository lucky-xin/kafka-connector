package xyz.kafka.connector.validator;

import com.google.common.collect.ImmutableList;
import org.apache.kafka.common.config.ConfigException;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * UriValidator
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class UriValidator extends Validators.SingleOrListValidator {
    protected List<String> validSchemes;

    public UriValidator(String... validSchemes) {
        this.validSchemes = ImmutableList.copyOf(validSchemes);
    }

    @Override
    protected void validate(String config, Object value) {
        if (value == null) {
            throw new ConfigException(config, value, String.format("URI scheme must be one of the following. '%s'", String.join("', '", this.validSchemes)));
        }
        try {
            URI uri = new URI(value.toString());
            if (!this.validSchemes.isEmpty() && !this.validSchemes.contains(uri.getScheme())) {
                throw new ConfigException(config, value, String.format("URI scheme must be one of the following. '%s'", String.join("', '", this.validSchemes)));
            }
        } catch (URISyntaxException e) {
            ConfigException configException = new ConfigException(config, value, "Could not parse to URI.");
            configException.initCause(e);
            throw configException;
        }
    }

    @Override
    public String toString() {
        return "URI with one of these schemes: '" + String.join("', '", this.validSchemes) + "'";
    }
}
