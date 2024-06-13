package xyz.kafka.connector.schema;

/**
 * LogicalTypeMatcher
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public interface LogicalTypeMatcher {
    boolean isTimestampLiteral(String str);

    boolean isTimeLiteral(String str);

    boolean isDateLiteral(String str);

    default LogicalTypeMatcher and(final LogicalTypeMatcher other) {
        if (other == this || other == null) {
            return this;
        }
        return new LogicalTypeMatcher() {
            @Override
            public boolean isTimestampLiteral(String value) {
                return this.isTimestampLiteral(value) && other.isTimestampLiteral(value);
            }

            @Override
            public boolean isTimeLiteral(String value) {
                return this.isTimeLiteral(value) && other.isTimeLiteral(value);
            }

            @Override
            public boolean isDateLiteral(String value) {
                return this.isDateLiteral(value) && other.isDateLiteral(value);
            }
        };
    }

    default LogicalTypeMatcher or(final LogicalTypeMatcher other) {
        if (other == this || other == null) {
            return this;
        }
        return new LogicalTypeMatcher() {
            @Override
            public boolean isTimestampLiteral(String value) {
                return this.isTimestampLiteral(value) || other.isTimestampLiteral(value);
            }

            @Override
            public boolean isTimeLiteral(String value) {
                return this.isTimeLiteral(value) || other.isTimeLiteral(value);
            }

            @Override
            public boolean isDateLiteral(String value) {
                return this.isDateLiteral(value) || other.isDateLiteral(value);
            }
        };
    }
}
