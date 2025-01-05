package xyz.kafka.connector.schema;

import com.google.common.collect.ImmutableList;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import xyz.kafka.connector.utils.Strings;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * FieldPath
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class FieldPath implements Iterable<FieldPath> {
    public static final FieldPath EMPTY = new FieldPath("", null, null);
    protected static final char DELIMITER = '.';
    protected static final String DELIMITER_STR = ".";
    protected static final char OPEN_BRACKET = '[';
    protected static final char CLOSE_BRACKET = ']';
    protected static final String QUOTE_STR = "'";
    protected static final String OPEN_BRACKET_STR = "[";
    protected static final String CLOSE_BRACKET_STR = "]";
    private final FieldPath parent;
    private final String name;
    private final Integer index;
    private final int hc;
    private final int depth;

    public static FieldPath parse(String path) {
        if (path == null) {
            throw new IllegalArgumentException("Unable to parse a null path");
        }
        String path2 = path.replaceFirst("^[$][.]?", "");
        if (path2.isEmpty()) {
            throw new InvalidFieldPathException("Unable to parse an empty path");
        } else {
            FieldPath pathParent = null;
            int startIndex = 0;
            while (startIndex < path2.length()) {
                if (path2.charAt(startIndex) == OPEN_BRACKET) {
                    int fromIndex = startIndex + 1;
                    int endQuote = findCloseBrace(path2, fromIndex);
                    if (isQuoted(path2, fromIndex, endQuote)) {
                        pathParent = new FieldPath(removeQuotesFrom(path2, fromIndex, endQuote), pathParent);
                    } else {
                        String indexStr = nameFrom(path2, fromIndex, endQuote);
                        try {
                            pathParent = new FieldPath(Integer.parseInt(indexStr), pathParent);
                        } catch (NumberFormatException e) {
                            throw new InvalidFieldPathException(String.format("Expected integer but found \"%s\" at position %d: %s", indexStr, fromIndex, path2));
                        }
                    }
                    startIndex = endQuote + 1;
                } else {
                    int delimIndex = Strings.firstIndexOf(path2, '.', '[', startIndex);
                    pathParent = new FieldPath(nameFrom(path2, startIndex, delimIndex), pathParent);
                    startIndex = delimIndex < 0 ? path2.length() : delimIndex;
                }
                if (path2.length() <= startIndex) {
                    return pathParent;
                }
                if (path2.charAt(startIndex) == DELIMITER) {
                    startIndex++;
                }
            }
            return pathParent;
        }
    }

    protected static boolean isQuoted(String path, int startIndex, int endIndex) {
        String name = endIndex < 0 ? path.substring(startIndex) : path.substring(startIndex, endIndex);
        return name.startsWith(QUOTE_STR) && name.endsWith(QUOTE_STR) && name.length() >= 2;
    }

    protected static String removeQuotesFrom(String path, int startIndex, int endIndex) {
        String name = endIndex < 0 ? path.substring(startIndex) : path.substring(startIndex, endIndex);
        if (!name.startsWith(QUOTE_STR) || !name.endsWith(QUOTE_STR) || name.length() < 2) {
            throw new InvalidFieldPathException(String.format("Path segments expected to be quoted at position %d: %s", startIndex, path));
        }
        String name2 = name.substring(1, name.length() - 1);
        if (!name2.isEmpty()) {
            return name2;
        }
        throw new InvalidFieldPathException(String.format("Path segments may not be empty at position %d: %s", startIndex, path));
    }

    protected static String nameFrom(String path, int startIndex, int endIndex) {
        String name = endIndex < 0 ? path.substring(startIndex) : path.substring(startIndex, endIndex);
        if (name.isEmpty()) {
            throw new InvalidFieldPathException(String.format("Path segments may not be empty at position %d: %s", startIndex, path));
        } else if (!name.startsWith(QUOTE_STR) && !name.endsWith(QUOTE_STR)) {
            return name;
        } else {
            throw new InvalidFieldPathException(String.format("Path segments may not be unquoted at position %d: %s", startIndex, path));
        }
    }

    protected static int findCloseBrace(String path, int fromIndex) {
        int endQuote = path.indexOf(CLOSE_BRACKET, fromIndex);
        if (endQuote >= 0) {
            return endQuote;
        }
        throw new InvalidFieldPathException(String.format("Unable to find close backet matching open bracket at position %d: %s", fromIndex, path));
    }

    public FieldPath(String name) {
        this(name, null);
    }

    public FieldPath(String name, FieldPath parent) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("The name may not be null or blank");
        }
        this.name = name;
        this.parent = parent;
        this.index = null;
        this.hc = Objects.hash(parent, name, this.index);
        this.depth = parent == null ? 1 : parent.depth + 1;
    }

    public FieldPath(int index, FieldPath parent) {
        this(Integer.toString(index), index, parent);
    }

    protected FieldPath(String name, Integer index, FieldPath parent) {
        this.name = name;
        this.parent = parent;
        this.index = index;
        this.hc = Objects.hash(parent, name, index);
        this.depth = parent == null ? 1 : parent.depth + 1;
    }

    public String name() {
        return this.name;
    }

    public Integer index() {
        return this.index;
    }

    public boolean isArrayIndex() {
        return this.index != null;
    }

    public boolean isRoot() {
        return this.parent == null;
    }

    public int depth() {
        return this.depth;
    }

    public FieldPath root() {
        return isRoot() ? this : this.parent.root();
    }

    public FieldPath parent() {
        if (isRoot()) {
            return null;
        }
        return this.parent;
    }

    public FieldPath ancestorAtDepth(int depth) {
        FieldPath result = this;
        while (result != null && result.depth != depth) {
            result = result.parent();
        }
        return result;
    }

    public void onPath(Consumer<FieldPath> function) {
        if (this.parent != null) {
            this.parent.onPath(function);
        }
        function.accept(this);
    }

    public FieldPath child(String field) {
        return new FieldPath(field, this);
    }

    public Object resolve(Object value) {
        return new Resolver(this, value).resolve();
    }

    public FieldPath append(FieldPath path) {
        FieldPath parent = this;
        if (path != null) {
            for (FieldPath fieldPath : path) {
                parent = new FieldPath(fieldPath.name(), parent);
            }
        }
        return parent;
    }

    @Override
    public int hashCode() {
        return this.hc;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof FieldPath that)) {
            return false;
        }
        return depth() == that.depth() && hashCode() == that.hashCode()
                && Objects.equals(this.name, that.name) && Objects.equals(this.parent, that.parent);
    }

    @Override
    public Iterator<FieldPath> iterator() {
        List<FieldPath> nodePath = new LinkedList<>();
        onPath(nodePath::add);
        return ImmutableList.copyOf(nodePath).iterator();
    }

    @Override
    public String toString() {
        return asString();
    }

    public String asString() {
        return asString(DELIMITER_STR, FieldPath::name);
    }

    protected String asString(Function<FieldPath, String> getName) {
        return asString(DELIMITER_STR, getName);
    }

    protected String asString(String delimiter) {
        return asString(delimiter, FieldPath::name);
    }

    protected String asString(String delimiter, Function<FieldPath, String> getName) {
        StringBuilder sb = new StringBuilder();
        sb.append("$");
        onPath(node -> {
            sb.append(OPEN_BRACKET_STR);
            if (node.isArrayIndex()) {
                sb.append(node.index());
            } else {
                sb.append(QUOTE_STR).append(node.name()).append(QUOTE_STR);
            }
            sb.append(CLOSE_BRACKET_STR);
        });
        return sb.toString();
    }

    protected static class Resolver {
        private final FieldPath originalPath;
        private final Object originalValue;
        private Object value;

        public Resolver(FieldPath path, Object value) {
            this.originalPath = path;
            this.originalValue = value;
            this.value = value;
        }

        public Object resolve() {
            this.originalPath.onPath(this::resolveNext);
            return this.value;
        }

        protected void resolveNext(FieldPath nextSegment) {
            String msg;
            if (this.value != null) {
                if (this.value instanceof Struct struct) {
                    Field field = struct.schema().field(nextSegment.name());
                    if (field != null) {
                        this.value = struct.get(field);
                        return;
                    }
                }
                if (this.value instanceof Map<?, ?> m) {
                    this.value = m.get(nextSegment.name());
                    return;
                }
                if (this.value instanceof List<?> list) {
                    Integer index = nextSegment.index();
                    if (index != null) {
                        if (index < list.size()) {
                            this.value = list.get(nextSegment.index());
                        } else {
                            this.value = null;
                        }
                        return;
                    }
                }
                if (nextSegment.isRoot()) {
                    msg = String.format("Unable to resolve path %s for value", this.originalPath);
                } else {
                    msg = String.format("Unable to resolve path %s beyond %s for value", this.originalPath, nextSegment.parent());
                }
                throw new InvalidFieldPathException(msg);
            }
        }
    }
}
