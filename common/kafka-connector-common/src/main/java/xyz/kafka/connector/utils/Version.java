package xyz.kafka.connector.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;

/**
 * Version
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-07-27
 */
public class Version {
    private static final Logger log = LoggerFactory.getLogger(Version.class);

    private Version() {
    }

    public static String forClass(Class<?> cls) {
        Objects.requireNonNull(cls, "The class reference may not be null");
        return Version.readVersion(cls);
    }

    protected static String readVersion(Class<?> cls) {
        try {
            String version = cls.getPackage().getImplementationVersion();
            if (version != null && !version.trim().isEmpty()) {
                log.trace("Found version '{}' for package '{}'", version, cls.getPackage().getName());
                return version;
            } else {
                String var5;
                try (InputStream stream = cls.getResourceAsStream("version.properties")) {
                    if (stream == null) {
                        log.warn("Failed to find version for {} and using '{}' " +
                                "for version; expecting to find file '{}/{}' using classloader for {}", cls.getName(), "unknown", modifiedPackageName(cls), "version.properties", cls.getName());
                        return "unknown";
                    }
                    Properties props = new Properties();
                    props.load(stream);
                    version = props.getProperty("version", "unknown").trim();
                    log.trace("Found version '{}' in '{}/{}' using classloader for {}",
                            version, modifiedPackageName(cls), "version.properties", cls.getName());
                    var5 = version;
                }
                return var5;
            }
        } catch (Exception var18) {
            log.warn("Failed to find version for {} and using '{}' for version; error reading find file '{}/{}' using classloader for {}",
                    cls.getName(), "unknown", modifiedPackageName(cls), "version.properties", cls.getName(), var18);
            return "unknown";
        }
    }

    protected static String modifiedPackageName(Class<?> clazz) {
        return clazz.getPackage().getName().replaceAll("[.]", "/");
    }
}
