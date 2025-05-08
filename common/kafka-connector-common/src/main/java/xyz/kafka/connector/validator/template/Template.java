package xyz.kafka.connector.validator.template;

import com.google.common.collect.ImmutableSet;
import xyz.kafka.connector.utils.Strings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Template
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public abstract class Template {
    protected static final Template EMPTY_TEMPLATE = new LiteralTemplate();

    public abstract int size();

    public abstract Set<String> variableNames();

    public abstract Template replace(VariableResolver<String> variableResolver);

    public abstract String replaceVariables(VariableResolver<String> variableResolver);

    public abstract <T> Resolution<T> resolveFirstVariableValue(VariableResolver<T> variableResolver);

    public interface Replacement {
        boolean isLiteral();

        String variableName();

        String variable();

        <T> Resolution<T> getReplacement(VariableResolver<T> variableResolver);

        default Object getReplacementOrVariable(VariableResolver<?> resolver) {
            Resolution<?> resolution = getReplacement(resolver);
            if (resolution.isResolved()) {
                return resolution.get();
            }
            return String.format("${%s}", variable());
        }
    }

    public static class Literal implements Replacement {
        private final Resolution<String> literal;

        protected Literal(String literal) {
            this.literal = Resolution.with(Objects.requireNonNull(literal));
        }

        @Override
        public boolean isLiteral() {
            return true;
        }

        @Override
        public String variableName() {
            return null;
        }

        @Override
        public String variable() {
            return null;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> Resolution<T> getReplacement(VariableResolver<T> resolver) {
            return (Resolution<T>) this.literal;
        }

        @Override
        public String toString() {
            return this.literal.get();
        }
    }

    public static class VariableReplacement implements Replacement {
        private final String variableName;
        private final String variable;
        private final Object parsed;

        protected VariableReplacement(String variableName, String variable, Object parsed) {
            this.variableName = Objects.requireNonNull(variableName);
            this.variable = Objects.requireNonNull(variable);
            this.parsed = Objects.requireNonNull(parsed);
        }

        @Override
        public boolean isLiteral() {
            return false;
        }

        @Override
        public String variableName() {
            return this.variableName;
        }

        @Override
        public String variable() {
            return this.variable;
        }

        @Override
        public <T> xyz.kafka.connector.validator.template.Resolution<T> getReplacement(VariableResolver<T> resolver) {
            return resolver.resolveVariable(this.variableName, this.variable, this.parsed);
        }

        @Override
        public String toString() {
            if (!Objects.equals(this.parsed, this.variable)) {
                return String.format("${%s} as %s", this.variable, this.parsed);
            }
            return String.format("${%s}", this.variable);
        }
    }

    public static class Builder {
        private final List<Replacement> replacements = new ArrayList<>();
        static final boolean assertionsDisabled;

        static {
            assertionsDisabled = !Template.class.desiredAssertionStatus();
        }

        public Builder addVariable(String variableName) {
            return addVariable(variableName, variableName, variableName);
        }

        public Builder addVariable(String variableName, String variable) {
            return addReplacement(new VariableReplacement(variableName, variable, variable));
        }

        public Builder addVariable(String variableName, String variable, Object parsed) {
            return addReplacement(new VariableReplacement(variableName, variable, parsed));
        }

        public Builder addLiteral(String substring) {
            return addReplacement(new Literal(substring));
        }

        protected Builder addReplacement(Replacement replacement) {
            if (assertionsDisabled || replacement != null) {
                if (replacement.isLiteral() && !this.replacements.isEmpty()) {
                    int lastIndex = this.replacements.size() - 1;
                    Replacement last = this.replacements.get(lastIndex);
                    if (last.isLiteral()) {
                        this.replacements.set(lastIndex, new Literal(last + replacement.toString()));
                        return this;
                    }
                }
                this.replacements.add(replacement);
                return this;
            }
            throw new AssertionError();
        }

        public Builder trim() {
            if (!this.replacements.isEmpty() && this.replacements.get(0).isLiteral()) {
                String result = Strings.removeLeadingWhitespace(this.replacements.get(0).toString());
                if (result.isEmpty()) {
                    this.replacements.remove(0);
                } else {
                    this.replacements.set(0, new Literal(result));
                }
            }
            int lastIndex = this.replacements.size() - 1;
            if (!this.replacements.isEmpty() && this.replacements.get(lastIndex).isLiteral()) {
                String result2 = Strings.removeTrailingWhitespace(this.replacements.get(lastIndex).toString());
                if (result2.isEmpty()) {
                    this.replacements.remove(lastIndex);
                } else {
                    this.replacements.set(lastIndex, new Literal(result2));
                }
            }
            return this;
        }

        public Template build() {
            if (this.replacements.isEmpty()) {
                return Template.EMPTY_TEMPLATE;
            }
            if (this.replacements.size() != 1) {
                return new MixedTemplate(this.replacements);
            }
            Replacement replacement = this.replacements.get(0);
            if (replacement.isLiteral()) {
                return new LiteralTemplate((Literal) replacement);
            }
            return new SingleVariableTemplate((VariableReplacement) replacement);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public int variableCount() {
        return variableNames().size();
    }

    public boolean hasVariables() {
        return variableCount() != 0;
    }

    public boolean isOneVariableOnly() {
        return variableCount() == 1 && size() == 1;
    }

    public static class LiteralTemplate extends Template {
        private final Literal replacement;
        private final String strValue;

        protected LiteralTemplate() {
            this.replacement = null;
            this.strValue = "";
        }

        protected LiteralTemplate(Literal replacement) {
            this.replacement = Objects.requireNonNull(replacement);
            this.strValue = replacement.toString();
        }

        @Override
        public int size() {
            return this.replacement != null ? 1 : 0;
        }

        @Override
        public Set<String> variableNames() {
            return Collections.emptySet();
        }

        @Override
        public Template replace(VariableResolver<String> variableResolver) {
            return this;
        }

        @Override
        public String replaceVariables(VariableResolver<String> variableResolver) {
            return this.strValue;
        }

        @Override
        public <T> Resolution<T> resolveFirstVariableValue(VariableResolver<T> variableResolver) {
            return Resolution.unresolved();
        }

        @Override
        public String toString() {
            return this.strValue;
        }
    }

    public static class SingleVariableTemplate extends Template {
        private final VariableReplacement variableReplacement;
        private final Set<String> variableNames;

        protected SingleVariableTemplate(VariableReplacement replacement) {
            this.variableReplacement = Objects.requireNonNull(replacement);
            this.variableNames = ImmutableSet.of(this.variableReplacement.variableName());
        }

        @Override
        public int size() {
            return 1;
        }

        @Override
        public Set<String> variableNames() {
            return this.variableNames;
        }

        @Override
        public Template replace(VariableResolver<String> variableResolver) {
            Resolution<?> resolution = this.variableReplacement.getReplacement(variableResolver);
            if (resolution.isResolved()) {
                return new LiteralTemplate(new Literal(resolution.map(Object::toString).orElse("")));
            }
            return this;
        }

        @Override
        public String replaceVariables(VariableResolver<String> variableResolver) {
            Object result = this.variableReplacement.getReplacementOrVariable(variableResolver);
            if (result == null) {
                return null;
            }
            return result.toString();
        }

        @Override
        public <T> Resolution<T> resolveFirstVariableValue(VariableResolver<T> variableResolver) {
            return this.variableReplacement.getReplacement(variableResolver);
        }

        @Override
        public String toString() {
            return this.variableReplacement.toString();
        }
    }

    public static class MixedTemplate extends Template {
        private final List<Replacement> replacements;
        private final List<Replacement> variableReplacements;
        private final Set<String> variableNames;

        protected MixedTemplate(List<Replacement> replacements) {
            this.replacements = Objects.requireNonNull(replacements);
            this.variableReplacements = replacements.stream()
                    .filter(replacement -> !replacement.isLiteral())
                    .toList();
            this.variableNames = ImmutableSet.copyOf(this.variableReplacements.stream()
                    .map(Replacement::variableName).collect(Collectors.toSet()));
        }

        @Override
        public int size() {
            return this.replacements.size();
        }

        @Override
        public int variableCount() {
            return this.variableReplacements.size();
        }

        @Override
        public Set<String> variableNames() {
            return this.variableNames;
        }

        @Override
        public Template replace(VariableResolver<String> variableResolver) {
            Builder builder = builder();
            this.replacements.forEach(replacement -> {
                Resolution<?> resolution = replacement.getReplacement(variableResolver);
                if (resolution.isResolved()) {
                    builder.addLiteral(resolution.map(Object::toString).orElse(""));
                } else {
                    builder.addReplacement(replacement);
                }
            });
            return builder.build();
        }

        @Override
        public String replaceVariables(VariableResolver<String> variableResolver) {
            StringBuilder sb = new StringBuilder();
            this.replacements.forEach(replacement -> {
                Object str = replacement.getReplacementOrVariable(variableResolver);
                if (str != null) {
                    sb.append(str);
                }
            });
            return sb.toString();
        }

        @Override
        public <T> Resolution<T> resolveFirstVariableValue(VariableResolver<T> variableResolver) {
            return this.variableReplacements.get(0).getReplacement(variableResolver);
        }

        @Override
        public String toString() {
            return this.replacements.stream().map(Object::toString).collect(Collectors.joining(""));
        }
    }
}
