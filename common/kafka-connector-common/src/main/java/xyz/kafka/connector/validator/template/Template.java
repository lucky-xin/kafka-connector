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
    // 定义一个空模板，用于在需要模板但实际内容为空的情况下使用
    protected static final Template EMPTY_TEMPLATE = new LiteralTemplate();

    /**
     * 获取模板的大小，以便了解模板中包含的变量或内容的数量
     *
     * @return 模板的大小
     */
    public abstract int size();

    /**
     * 获取模板中所有变量的名称，以便知道模板中使用了哪些变量
     *
     * @return 包含所有变量名称的集合
     */
    public abstract Set<String> variableNames();

    /**
     * 使用变量解析器替换模板中的变量，返回一个新的模板对象
     * 这允许在不同上下文中重用模板，并根据需要替换其中的变量
     *
     * @param variableResolver 用于解析变量的解析器
     * @return 替换变量后的新模板对象
     */
    public abstract Template replace(VariableResolver<String> variableResolver);

    /**
     * 使用变量解析器替换模板中的变量并返回结果字符串
     * 这提供了一种快速获取替换变量后模板内容的方法
     *
     * @param variableResolver 用于解析变量的解析器
     * @return 替换变量后的模板内容字符串
     */
    public abstract String replaceVariables(VariableResolver<String> variableResolver);

    /**
     * 解析并获取模板中第一个变量的值
     * 此方法用于快速访问模板中第一个变量的解析值，适用于只需要获取第一个变量值的场景
     *
     * @param <T>              变量值的类型
     * @param variableResolver 用于解析变量的解析器
     * @return 变量值的解析结果
     */
    public abstract <T> Resolution<T> resolveFirstVariableValue(VariableResolver<T> variableResolver);


    /**
     * Replacement接口定义了如何获取替换内容的规则
     * 它提供了两种获取替换内容的方式：直接返回预设的字面量或通过变量解析
     */
    public interface Replacement {
        /**
         * 判断当前替换是否为字面量
         *
         * @return 如果当前替换为字面量，则返回true；否则返回false
         */
        boolean isLiteral();

        /**
         * 获取变量名
         *
         * @return 变量名，如果当前替换不涉及变量，则返回null
         */
        String variableName();

        /**
         * 获取变量的字符串表示形式
         *
         * @return 变量的字符串表示形式，如果当前替换不涉及变量，则返回null
         */
        String variable();

        /**
         * 通过变量解析器获取替换内容
         *
         * @param variableResolver 变量解析器，用于解析变量到具体值
         * @param <T> 解析结果的类型
         * @return 解析结果封装在Resolution对象中
         */
        <T> Resolution<T> getReplacement(VariableResolver<T> variableResolver);

        /**
         * 根据解析结果获取替换内容或变量字符串
         * 如果解析成功，则返回解析结果；否则返回变量的字符串表示形式
         *
         * @param resolver 变量解析器
         * @return 解析结果或变量字符串
         */
        default Object getReplacementOrVariable(VariableResolver<?> resolver) {
            Resolution<?> resolution = getReplacement(resolver);
            if (resolution.isResolved()) {
                return resolution.get();
            }
            return String.format("${%s}", variable());
        }
    }

    /**
     * Literal类表示一个字面量替换
     * 它实现了Replacement接口，并且总是返回一个固定的字面量作为替换内容
     */
    public static class Literal implements Replacement {
        private final Resolution<String> lit;

        /**
         * 构造一个Literal实例
         *
         * @param literal 字面量字符串，不能为空
         */
        protected Literal(String literal) {
            this.lit = Resolution.with(Objects.requireNonNull(literal));
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

        /**
         * 返回字面量的解析结果
         *
         * @param resolver 变量解析器，未使用
         * @param <T> 解析结果的类型，由于是字面量，所以这里会强制转换类型
         * @return 字面量的解析结果，类型为T
         */
        @SuppressWarnings("unchecked")
        @Override
        public <T> Resolution<T> getReplacement(VariableResolver<T> resolver) {
            return (Resolution<T>) this.lit;
        }

        /**
         * 返回字面量的字符串表示形式
         *
         * @return 字面量的字符串表示形式
         */
        @Override
        public String toString() {
            return this.lit.get();
        }
    }

    /**
     * VariableReplacement类表示一个变量替换
     * 它实现了Replacement接口，并且通过变量解析器解析变量到具体值
     */
    public static class VariableReplacement implements Replacement {
        private final String variableName;
        private final String variable;
        private final Object parsed;

        /**
         * 构造一个VariableReplacement实例
         *
         * @param variableName 变量名，不能为空
         * @param variable 变量的字符串表示形式，不能为空
         * @param parsed 解析后的变量值，不能为空
         */
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

        /**
         * 通过变量解析器解析变量到具体值
         *
         * @param resolver 变量解析器，用于解析变量到具体值
         * @param <T> 解析结果的类型
         * @return 解析结果封装在Resolution对象中
         */
        @Override
        public <T> xyz.kafka.connector.validator.template.Resolution<T> getReplacement(VariableResolver<T> resolver) {
            return resolver.resolveVariable(this.variableName, this.variable, this.parsed);
        }

        /**
         * 返回变量的字符串表示形式
         * 如果解析后的变量值与变量的字符串表示形式不相同，则同时返回两者
         *
         * @return 变量的字符串表示形式
         */
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
        static final boolean ASSERTIONS_DISABLED;

        static {
            ASSERTIONS_DISABLED = !Template.class.desiredAssertionStatus();
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
            if (ASSERTIONS_DISABLED || replacement != null) {
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
