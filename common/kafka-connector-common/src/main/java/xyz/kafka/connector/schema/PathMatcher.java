package xyz.kafka.connector.schema;

import com.google.common.collect.ImmutableSet;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * LogicalTypeMatcher
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
@FunctionalInterface
public interface PathMatcher {
    boolean matches(FieldPath var1);

    default PathMatcher or(PathMatcher matcher) {
        return matcher == null ? this : path -> this.matches(path) || matcher.matches(path);
    }

    default PathMatcher and(PathMatcher matcher) {
        return matcher == null ? this : path -> this.matches(path) && matcher.matches(path);
    }

    static PathMatcher matchAll() {
        return p -> true;
    }

    static PathMatcher matchNonNullPaths() {
        return Objects::nonNull;
    }

    static PathMatcher matchNone() {
        return p -> false;
    }

    static PathMatcher matchAnyOf(FieldPath... paths) {
        if (paths != null && paths.length != 0) {
            Set<?> allowedPaths = ImmutableSet.copyOf(paths).stream()
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());
            return allowedPaths::contains;
        } else {
            throw new IllegalArgumentException("At least one path must be specified");
        }
    }

    static PathMatcher matchAnyOf(String... paths) {
        if (paths != null && paths.length != 0) {
            Set<FieldPath> allowedPaths = ImmutableSet.copyOf(paths).stream()
                    .filter(Objects::nonNull)
                    .map(FieldPath::parse)
                    .collect(Collectors.toSet());
            return allowedPaths::contains;
        } else {
            throw new IllegalArgumentException("At least one path must be specified");
        }
    }

    static PathMatcher match(FieldPath path) {
        if (path == null) {
            throw new IllegalArgumentException("The specified path to match may not be null");
        } else {
            return path::equals;
        }
    }

    static PathMatcher matchDescendantOf(String path) {
        if (path == null) {
            throw new IllegalArgumentException("The specified path to match may not be null");
        } else {
            return matchDescendantOf(FieldPath.parse(path));
        }
    }

    static PathMatcher matchDescendantOf(FieldPath path) {
        if (path == null) {
            throw new IllegalArgumentException("The specified path to match may not be null");
        } else {
            int depth = path.depth();
            return p -> p.depth() > depth && path.equals(p.ancestorAtDepth(depth));
        }
    }

    static PathMatcher matchPathOrDescendantOf(String path) {
        if (path == null) {
            throw new IllegalArgumentException("The specified path to match may not be null");
        } else {
            return matchPathOrDescendantOf(FieldPath.parse(path));
        }
    }

    static PathMatcher matchPathOrDescendantOf(FieldPath path) {
        if (path == null) {
            throw new IllegalArgumentException("The specified path to match may not be null");
        } else {
            int depth = path.depth();
            return p -> p.depth() >= depth && p.equals(path) || path.equals(p.ancestorAtDepth(depth));
        }
    }

    static PathMatcher matchChildOf(String path) {
        if (path == null) {
            throw new IllegalArgumentException("The specified path to match may not be null");
        } else {
            return matchChildOf(FieldPath.parse(path));
        }
    }

    static PathMatcher matchChildOf(FieldPath path) {
        if (path == null) {
            throw new IllegalArgumentException("The specified path to match may not be null");
        } else {
            int childDepth = path.depth() + 1;
            return p -> p.depth() == childDepth && path.equals(p.parent());
        }
    }

    static PathMatcher matchName(String name) {
        if (name == null) {
            throw new IllegalArgumentException("The specified name may not be null");
        } else {
            return p -> p.name().equals(name);
        }
    }

    static PathMatcher matchAnyNames(String... names) {
        if (names != null && names.length != 0) {
            Set<?> allowedNames = ImmutableSet.of(names).stream().filter(Objects::nonNull).collect(Collectors.toSet());
            return p -> allowedNames.contains(p.name());
        } else {
            throw new IllegalArgumentException("At least one child name must be specified");
        }
    }

    static PathMatcher matchChildWithName(String path, String... childNames) {
        return matchChildWithName(FieldPath.parse(path), childNames);
    }

    static PathMatcher matchChildWithName(FieldPath path, String... childNames) {
        return matchChildOf(path).and(matchAnyNames(childNames));
    }
}
