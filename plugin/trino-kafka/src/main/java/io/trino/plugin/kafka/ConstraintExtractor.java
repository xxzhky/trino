/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.math.LongMath;
import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DateType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.base.expression.ConnectorExpressions.and;
import static io.trino.plugin.base.expression.ConnectorExpressions.extractConjuncts;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Math.toIntExact;
import static java.math.RoundingMode.UNNECESSARY;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
/**
 * This class is used to extract and manipulate constraint related data.
 * It provides static functions to handle different operations,
 * including transforming constraints and expressions, managing conjuncts,
 * converting and intersecting tuple domains, and handling different types of calls.
 * <p>
 * It has several private static helper methods for dealing with various cases
 * of comparisons and conversions, such as timestamp to date casts and date truncations.
 * Specialized handling for processing Connector specific timestamp handling is also included.
 * <p>
 * This class ends with a nested record ExtractionResult,
 * which groups a TupleDomain with a remaining ConnnectorExpression,
 * representing the result of an extraction process.
 * <p>
 * This class is declared as final, and it also has a private constructor,
 * which means it cannot be instantiated or sub-classed.
 * All of its methods are static and can be accessed directly from the class.
 */
public final class ConstraintExtractor
{
    private ConstraintExtractor() {}

    /**
     * Extracts the tuple domain and remaining expressions from a constraint.
     *
     * @param <C> the type of the column handle
     * @param constraint the constraint to extract from
     * @param columnTypeProvider provides the column types for the given column handles
     * @return an ExtractionResult object containing the extracted tuple domain and remaining expressions
     */
    public static <C extends ColumnHandle> ExtractionResult<C> extractTupleDomain(Constraint constraint, ColumnTypeProvider<C> columnTypeProvider)
    {
        TupleDomain<C> result = constraint.getSummary()
                .transformKeys(key -> (C) key);
        ImmutableList.Builder<ConnectorExpression> remainingExpressions = ImmutableList.builder();
        for (ConnectorExpression conjunct : extractConjuncts(constraint.getExpression())) {
            Optional<TupleDomain<C>> converted = toTupleDomain(conjunct, constraint.getAssignments(), columnTypeProvider);
            if (converted.isEmpty()) {
                remainingExpressions.add(conjunct);
            }
            else {
                result = result.intersect(converted.get());
                if (result.isNone()) {
                    return new ExtractionResult(TupleDomain.none(), Constant.TRUE);
                }
            }
        }
        return new ExtractionResult(result, and(remainingExpressions.build()));
    }

    /**
     * Converts a ConnectorExpression to TupleDomain.
     *
     * @param <C> the type of the column handle
     * @param expression the ConnectorExpression to convert
     * @param assignments a map of column assignments
     * @param columnTypeProvider provides the column types for the given column handles
     * @return an Optional containing the converted TupleDomain if the expression is a Call,
     *         otherwise an empty Optional
     */
    private static <C> Optional<TupleDomain<C>> toTupleDomain(ConnectorExpression expression, Map<String, ColumnHandle> assignments, ColumnTypeProvider<C> columnTypeProvider)
    {
        if (expression instanceof Call call) {
            return toTupleDomain(call, assignments, columnTypeProvider);
        }
        return Optional.empty();
    }

    /**
     * Attempts to convert a 'Call' expression into a 'TupleDomain', if applicable. This method handles
     * specific patterns of 'Call' expressions, particularly those involving 'cast', 'date_trunc', and 'year'
     * functions, and attempts to transform them into a 'TupleDomain' representation.
     *
     * @param <C> the generic type parameter representing the column handle type.
     * @param call the 'Call' expression to be converted. It represents a function call in the query.
     * @param assignments a mapping of string identifiers to 'ColumnHandle' objects, used to resolve column references.
     * @param columnTypeProvider a provider for column types based on column handles, used to determine data types for columns.
     * @return an 'Optional' containing the 'TupleDomain' if conversion is applicable and successful, otherwise an empty 'Optional'.
     *
     * The method first checks if the 'Call' expression has exactly two arguments, a pattern common in binary functions
     * like comparisons. It then examines the type and nature of these arguments:
     *
     * - If the first argument is a 'cast' function call and the second is a constant, and both arguments have the same type,
     *   it attempts to unwrap this cast comparison into a 'TupleDomain'.
     * - If the first argument is a 'date_trunc' function call and the second is a constant, with both having the same type,
     *   it tries to unwrap this date truncation comparison.
     * - If the first argument is a 'year' function call on a 'TimestampWithTimeZoneType' and the second is a constant,
     *   with both having the same type, it unwraps this year comparison.
     *
     * If none of these patterns match, or if the arguments' types do not align in a way that permits meaningful comparison,
     * the method returns an empty 'Optional', indicating that no 'TupleDomain' representation is applicable for the given 'Call'.
     */
    private static <C> Optional<TupleDomain<C>> toTupleDomain(Call call, Map<String, ColumnHandle> assignments, ColumnTypeProvider<C> columnTypeProvider)
    {
        if (call.getArguments().size() == 2) {
            ConnectorExpression firstArgument = call.getArguments().get(0);
            ConnectorExpression secondArgument = call.getArguments().get(1);

            // Note: CanonicalizeExpressionRewriter ensures that constants are the second comparison argument.

            if (firstArgument instanceof Call firstAsCall && firstAsCall.getFunctionName().equals(CAST_FUNCTION_NAME) &&
                    secondArgument instanceof Constant constant &&
                    // if type do no match, this cannot be a comparison function
                    firstArgument.getType().equals(secondArgument.getType())) {
                return unwrapCastInComparison(
                        call.getFunctionName(),
                        getOnlyElement(firstAsCall.getArguments()),
                        constant,
                        assignments,
                        columnTypeProvider);
            }

            if (firstArgument instanceof Call firstAsCall &&
                    firstAsCall.getFunctionName().equals(new FunctionName("date_trunc")) && firstAsCall.getArguments().size() == 2 &&
                    firstAsCall.getArguments().get(0) instanceof Constant unit &&
                    secondArgument instanceof Constant constant &&
                    // if type do no match, this cannot be a comparison function
                    firstArgument.getType().equals(secondArgument.getType())) {
                return unwrapDateTruncInComparison(
                        call.getFunctionName(),
                        unit,
                        firstAsCall.getArguments().get(1),
                        constant,
                        assignments,
                        columnTypeProvider);
            }

            if (firstArgument instanceof Call firstAsCall &&
                    firstAsCall.getFunctionName().equals(new FunctionName("year")) &&
                    firstAsCall.getArguments().size() == 1 &&
                    getOnlyElement(firstAsCall.getArguments()).getType() instanceof TimestampWithTimeZoneType &&
                    secondArgument instanceof Constant constant &&
                    // if types do no match, this cannot be a comparison function
                    firstArgument.getType().equals(secondArgument.getType())) {
                return unwrapYearInTimestampTzComparison(
                        call.getFunctionName(),
                        getOnlyElement(firstAsCall.getArguments()),
                        constant,
                        assignments,
                        columnTypeProvider);
            }
        }

        return Optional.empty();
    }

    /**
     * Tries to unwrap a cast operation within a comparison expression and convert it into a TupleDomain.
     * This method specifically handles cases where a cast operation is used in a comparison, such as
     * casting a column to a different type and then comparing it to a constant value.
     *
     * @param <C> the generic type parameter representing the column handle type.
     * @param functionName the name of the function that represents the comparison operation.
     * @param castSource the expression that is being cast, typically a column or a variable in the query.
     * @param constant the constant value being compared against after the cast operation.
     * @param assignments a map of string identifiers to column handles, used for resolving column references in expressions.
     * @param columnTypeProvider a provider for column types based on column handles, used to determine data types for columns.
     * @return an Optional containing the TupleDomain if the cast can be successfully unwrapped into a meaningful comparison, otherwise an empty Optional.
     *
     * The method performs several checks and transformations:
     * - It first verifies that the castSource is a variable (source column), as the method is designed to work primarily with source columns.
     * - It then checks if the constant is non-null, as comparisons with null are expected to be simplified by the query engine.
     * - After resolving the column and its type, the method handles specific cases, like casting a timestamp with time zone to a date,
     *   and attempts to create a corresponding TupleDomain.
     * - If the operation involves a type or pattern not handled by this method (e.g., unsupported cast types or non-source columns),
     *   it returns an empty Optional, indicating that the cast cannot be unwrapped into a domain in the current context.
     */
    private static <C> Optional<TupleDomain<C>> unwrapCastInComparison(
            // upon invocation, we don't know if this really is a comparison
            FunctionName functionName,
            ConnectorExpression castSource,
            Constant constant,
            Map<String, ColumnHandle> assignments,
            ColumnTypeProvider<C> columnTypeProvider)
    {
        if (!(castSource instanceof Variable sourceVariable)) {
            // Engine unwraps casts in comparisons in UnwrapCastInComparison. Within a connector we can do more than
            // engine only for source columns. We cannot draw many conclusions for intermediate expressions without
            // knowing them well.
            return Optional.empty();
        }

        if (constant.getValue() == null) {
            // Comparisons with NULL should be simplified by the engine
            return Optional.empty();
        }

        C column = resolve(sourceVariable, assignments);
        Optional<Type> columnType = columnTypeProvider.getType(column);
        if (columnType.isPresent() && columnType.get() instanceof TimestampWithTimeZoneType sourceType) {
            // Iceberg supports only timestamp(6) with time zone
            checkArgument(sourceType.getPrecision() == 6, "Unexpected type: %s", columnType);

            if (constant.getType() == DateType.DATE) {
                return unwrapTimestampTzToDateCast(column, functionName, (long) constant.getValue(), columnTypeProvider)
                        .map(domain -> TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)));
            }
            // TODO support timestamp constant
        }

        return Optional.empty();
    }

    /**
     * Unwraps a cast operation from timestamp with timezone to date within a comparison expression and converts it into a Domain.
     * This method specifically handles cases where a timestamp with timezone column is cast to a date and then compared against
     * a constant date value.
     *
     * @param <C> the generic type parameter representing the column handle type.
     * @param column the column handle, representing a column in the query that is cast from timestamp with timezone to date.
     * @param functionName the name of the comparison function being used in the expression.
     * @param date the date value (in epoch days) that the timestamp with timezone is compared to.
     * @param columnTypeProvider a provider for column types based on column handles, used to determine the data type of the column.
     * @return an Optional containing the Domain if the cast and comparison can be successfully unwrapped, otherwise an empty Optional.
     *
     * The method performs several operations:
     * - It first validates that the column is of the expected type (timestamp with timezone).
     * - It checks to ensure the provided date value does not cause overflow issues, asserting that it must be within the integer range.
     * - The method then calculates the start of the given date and the start of the next date in terms of timestamp with timezone.
     * - These calculated timestamps are then used to create a Domain that represents the result of the comparison operation.
     * - If the column type does not match or if other validation checks fail, the method returns an empty Optional,
     *   indicating that the Domain cannot be created for the given parameters.
     */
    private static <C> Optional<Domain> unwrapTimestampTzToDateCast(C column, FunctionName functionName, long date, ColumnTypeProvider<C> columnTypeProvider)
    {
        //Type type = column.getType();
        Type type = columnTypeProvider.getType(column).orElseThrow();
        checkArgument(type.equals(TIMESTAMP_TZ_MICROS), "Column of unexpected type %s: %s", type, column);

        // Verify no overflow. Date values must be in integer range.
        verify(date <= Integer.MAX_VALUE, "Date value out of range: %s", date);

        // In the Connector, timestamp with time zone values are all in UTC

        LongTimestampWithTimeZone startOfDate = LongTimestampWithTimeZone.fromEpochMillisAndFraction(date * MILLISECONDS_PER_DAY, 0, UTC_KEY);
        LongTimestampWithTimeZone startOfNextDate = LongTimestampWithTimeZone.fromEpochMillisAndFraction((date + 1) * MILLISECONDS_PER_DAY, 0, UTC_KEY);

        return createDomain(functionName, type, startOfDate, startOfNextDate);
    }

    /**
     * Unwraps the year in a comparison between a timestamp with time zone column
     * and a constant.
     *
     * @param <C> the type of the column handle
     * @param functionName the name of the function being evaluated
     * @param type the type of the timestamp with time zone column
     * @param constant the constant value being compared against
     * @return an Optional containing the unwrapped domain if the constant value is not null
     * and the type of the column is TIMESTAMP_TZ_MICROS, otherwise an empty Optional
     */
    private static <C> Optional<Domain> unwrapYearInTimestampTzComparison(FunctionName functionName, Type type, Constant constant)
    {
        checkArgument(constant.getValue() != null, "Unexpected constant: %s", constant);
        checkArgument(type.equals(TIMESTAMP_TZ_MICROS), "Unexpected type: %s", type);

        int year = toIntExact((Long) constant.getValue());
        ZonedDateTime periodStart = ZonedDateTime.of(year, 1, 1, 0, 0, 0, 0, UTC);
        ZonedDateTime periodEnd = periodStart.plusYears(1);

        LongTimestampWithTimeZone start = LongTimestampWithTimeZone.fromEpochSecondsAndFraction(periodStart.toEpochSecond(), 0, UTC_KEY);
        LongTimestampWithTimeZone end = LongTimestampWithTimeZone.fromEpochSecondsAndFraction(periodEnd.toEpochSecond(), 0, UTC_KEY);

        return createDomain(functionName, type, start, end);
    }

    /**
     * Creates a domain based on the given function name, type, start of date, and start of next date.
     *
     * @param functionName the name of the function used to create the domain
     * @param type the type of the domain
     * @param startOfDate the start of the date range
     * @param startOfNextDate the start of the next date range
     * @return an Optional containing the created Domain, or an empty Optional if the domain cannot be created
     */
    private static Optional<Domain> createDomain(FunctionName functionName, Type type, LongTimestampWithTimeZone startOfDate, LongTimestampWithTimeZone startOfNextDate)
    {
        Map<FunctionName, DomainCreator> domainCreators = Map.of(
                EQUAL_OPERATOR_FUNCTION_NAME, (t, s, e) -> Optional.of(Domain.create(ValueSet.ofRanges(Range.range(t, s, true, e, false)), false)),
                NOT_EQUAL_OPERATOR_FUNCTION_NAME, (t, s, e) -> Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(t, s), Range.greaterThanOrEqual(t, e)), false)),
                LESS_THAN_OPERATOR_FUNCTION_NAME, (t, s, e) -> Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(t, s)), false)),
                LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, (t, s, e) -> Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(t, e)), false)),
                GREATER_THAN_OPERATOR_FUNCTION_NAME, (t, s, e) -> Optional.of(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(t, e)), false)),
                GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, (t, s, e) -> Optional.of(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(t, s)), false)),
                IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME, (t, s, e) -> Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(t, s), Range.greaterThanOrEqual(t, e)), true))
        );

        return Optional.ofNullable(domainCreators.get(functionName))
                .map(creator -> creator.create(type, startOfDate, startOfNextDate))
                .orElse(Optional.empty());
    }

    /**
     * This functional interface represents a domain creator that creates a new {@link Domain} with the specified type, start, and end timestamps.
     */
    @FunctionalInterface
    private interface DomainCreator
    {
        /**
         * Creates a new {@link Domain} with the specified type, start, and end timestamps.
         *
         * @param type The type of the domain.
         * @param start The start timestamp of the domain.
         * @param end The end timestamp of the domain.
         * @return An optional containing the created domain, or an empty optional if the creation fails.
         */
        Optional<Domain> create(Type type, LongTimestampWithTimeZone start, LongTimestampWithTimeZone end);
    }

    private static <C> Optional<TupleDomain<C>> unwrapDateTruncInComparison(
            // upon invocation, we don't know if this really is a comparison
            FunctionName functionName,
            Constant unit,
            ConnectorExpression dateTruncSource,
            Constant constant,
            Map<String, ColumnHandle> assignments,
            ColumnTypeProvider<C> columnTypeProvider)
    {
        if (!(dateTruncSource instanceof Variable sourceVariable)) {
            // Engine unwraps date_trunc in comparisons in UnwrapDateTruncInComparison. Within a connector we can do more than
            // engine only for source columns. We cannot draw many conclusions for intermediate expressions without
            // knowing them well.
            return Optional.empty();
        }

        if (unit.getValue() == null) {
            return Optional.empty();
        }

        if (constant.getValue() == null) {
            // Comparisons with NULL should be simplified by the engine
            return Optional.empty();
        }

        C column = resolve(sourceVariable, assignments);
        Optional<Type> columnType = columnTypeProvider.getType(column);
        if (columnType.isPresent() && columnType.get()  instanceof TimestampWithTimeZoneType type) {
            // Iceberg supports only timestamp(6) with time zone
            checkArgument(type.getPrecision() == 6, "Unexpected type: %s", columnType);
            verify(constant.getType().equals(type), "This method should not be invoked when type mismatch (i.e. surely not a comparison)");

            return unwrapDateTruncInComparison(((Slice) unit.getValue()).toStringUtf8(), functionName, constant)
                    .map(domain -> TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)));
        }

        return Optional.empty();
    }

    private static Optional<Domain> unwrapDateTruncInComparison0(String unit, FunctionName functionName, Constant constant)
    {
        Type type = constant.getType();
        checkArgument(constant.getValue() != null, "Unexpected constant: %s", constant);
        checkArgument(type.equals(TIMESTAMP_TZ_MICROS), "Unexpected type: %s", type);

        // Normalized to UTC because for comparisons the zone is irrelevant
        ZonedDateTime dateTime = Instant.ofEpochMilli(((LongTimestampWithTimeZone) constant.getValue()).getEpochMillis())
                .plusNanos(LongMath.divide(((LongTimestampWithTimeZone) constant.getValue()).getPicosOfMilli(), PICOSECONDS_PER_NANOSECOND, UNNECESSARY))
                .atZone(UTC);

        ZonedDateTime periodStart;
        ZonedDateTime nextPeriodStart;
        switch (unit.toLowerCase(ENGLISH)) {
            case "hour" -> {
                periodStart = ZonedDateTime.of(dateTime.toLocalDate(), LocalTime.of(dateTime.getHour(), 0), UTC);
                nextPeriodStart = periodStart.plusHours(1);
            }
            case "day" -> {
                periodStart = dateTime.toLocalDate().atStartOfDay().atZone(UTC);
                nextPeriodStart = periodStart.plusDays(1);
            }
            case "month" -> {
                periodStart = dateTime.toLocalDate().withDayOfMonth(1).atStartOfDay().atZone(UTC);
                nextPeriodStart = periodStart.plusMonths(1);
            }
            case "year" -> {
                periodStart = dateTime.toLocalDate().withMonth(1).withDayOfMonth(1).atStartOfDay().atZone(UTC);
                nextPeriodStart = periodStart.plusYears(1);
            }
            default -> {
                return Optional.empty();
            }
        }
        boolean constantAtPeriodStart = dateTime.equals(periodStart);

        LongTimestampWithTimeZone start = LongTimestampWithTimeZone.fromEpochSecondsAndFraction(periodStart.toEpochSecond(), 0, UTC_KEY);
        LongTimestampWithTimeZone end = LongTimestampWithTimeZone.fromEpochSecondsAndFraction(nextPeriodStart.toEpochSecond(), 0, UTC_KEY);

        if (functionName.equals(EQUAL_OPERATOR_FUNCTION_NAME)) {
            if (!constantAtPeriodStart) {
                return Optional.of(Domain.none(type));
            }
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.range(type, start, true, end, false)), false));
        }
        if (functionName.equals(NOT_EQUAL_OPERATOR_FUNCTION_NAME)) {
            if (!constantAtPeriodStart) {
                return Optional.of(Domain.notNull(type));
            }
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(type, start), Range.greaterThanOrEqual(type, end)), false));
        }
        if (functionName.equals(IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME)) {
            if (!constantAtPeriodStart) {
                return Optional.of(Domain.all(type));
            }
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(type, start), Range.greaterThanOrEqual(type, end)), true));
        }
        if (functionName.equals(LESS_THAN_OPERATOR_FUNCTION_NAME)) {
            if (constantAtPeriodStart) {
                return Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(type, start)), false));
            }
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(type, end)), false));
        }
        if (functionName.equals(LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME)) {
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(type, end)), false));
        }
        if (functionName.equals(GREATER_THAN_OPERATOR_FUNCTION_NAME)) {
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, end)), false));
        }
        if (functionName.equals(GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME)) {
            if (constantAtPeriodStart) {
                return Optional.of(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, start)), false));
            }
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, end)), false));
        }
        return Optional.empty();
    }

    /**
     * Unwraps a date truncation comparison to create a domain.
     * This method processes a comparison where a date-time value has been truncated to a specific unit
     * (e.g., hour, day, month, year) and then compared to a constant value.
     *
     * @param unit The unit of date truncation (e.g., "hour", "day", "month", "year").
     * @param functionName The name of the function representing the comparison operation.
     * @param constant The constant value used in the comparison.
     * @return An Optional containing the created Domain if the comparison can be unwrapped successfully, otherwise an empty Optional.
     *
     * The method first checks if the constant is valid for this kind of comparison.
     * It then converts the constant to a ZonedDateTime and calculates the start and end of the period based on the truncation unit.
     * If a valid period can be determined, it converts the start and end times to LongTimestampWithTimeZone format.
     * Finally, it calls 'createDomainBasedOnFunction' to create the appropriate domain for the given comparison function,
     * taking into account whether the constant is at the start of the calculated period.
     */
    private static Optional<Domain> unwrapDateTruncInComparison(String unit, FunctionName functionName, Constant constant) {
        if (!isValidConstant(constant)) {
            return Optional.empty();
        }

        ZonedDateTime dateTime = convertConstantToZonedDateTime(constant);
        PeriodInterval periodInterval = calculatePeriodInterval(unit, dateTime);
        if (periodInterval == null) {
            return Optional.empty();
        }

        LongTimestampWithTimeZone startTimestamp = convertToLongTimestampWithTimeZone(periodInterval.start);
        LongTimestampWithTimeZone endTimestamp = convertToLongTimestampWithTimeZone(periodInterval.end);
        boolean isConstantAtPeriodStart = dateTime.equals(periodInterval.start);

        return createDomainBasedOnFunction(functionName, constant.getType(),
                startTimestamp, endTimestamp, isConstantAtPeriodStart);
    }

    private static boolean isValidConstant(Constant constant) {
        return constant.getValue() instanceof LongTimestampWithTimeZone &&
                constant.getType().equals(TIMESTAMP_TZ_MICROS);
    }

    private static ZonedDateTime convertConstantToZonedDateTime(Constant constant) {
        LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) constant.getValue();
        return Instant.ofEpochMilli(timestamp.getEpochMillis())
                .plusNanos(LongMath.divide(timestamp.getPicosOfMilli(), PICOSECONDS_PER_NANOSECOND, UNNECESSARY))
                .atZone(UTC);
    }

    private static PeriodInterval calculatePeriodInterval(String unit, ZonedDateTime dateTime) {
        return switch (unit.toLowerCase(ENGLISH)) {
            case "hour" -> new PeriodInterval(dateTime, dateTime.plusHours(1));
            case "day" -> new PeriodInterval(dateTime.toLocalDate().atStartOfDay(UTC), dateTime.plusDays(1));
            case "month" -> new PeriodInterval(dateTime.toLocalDate().withDayOfMonth(1).atStartOfDay(UTC), dateTime.plusMonths(1));
            case "year" -> new PeriodInterval(dateTime.toLocalDate().withDayOfMonth(1).withMonth(1).atStartOfDay(UTC), dateTime.plusYears(1));
            default -> null;
        };
    }

    private static LongTimestampWithTimeZone convertToLongTimestampWithTimeZone(ZonedDateTime zonedDateTime) {
        return LongTimestampWithTimeZone.fromEpochSecondsAndFraction(zonedDateTime.toEpochSecond(), 0, UTC_KEY);
    }

    /**
     * Creates a domain for a given comparison function based on the type, start, end, and a boolean flag.
     * This method utilizes a map of function names to domain creators, selecting the appropriate domain logic
     * based on the specified comparison function.
     *
     * @param functionName The name of the comparison function to be used.
     * @param type The data type of the domain.
     * @param start The start timestamp of the domain.
     * @param end The end timestamp of the domain.
     * @param isConstantAtPeriodStart A boolean flag indicating if the constant is at the start of the period.
     * @return An Optional containing the created Domain if a matching domain creator is found; otherwise, an empty Optional.
     */
    private static Optional<Domain> createDomainBasedOnFunction(FunctionName functionName, Type type,
                                                                LongTimestampWithTimeZone start, LongTimestampWithTimeZone end,
                                                                boolean isConstantAtPeriodStart) {
        Map<FunctionName, ComparisonDomainCreator> domainCreators = Map.of(
                EQUAL_OPERATOR_FUNCTION_NAME, (t, s, e, atStart) -> atStart
                        ? Optional.of(Domain.create(ValueSet.ofRanges(Range.range(t, s, true, e, false)), false))
                        : Optional.of(Domain.none(t)),
                NOT_EQUAL_OPERATOR_FUNCTION_NAME, (t, s, e, atStart) -> atStart
                        ? Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(t, s), Range.greaterThanOrEqual(t, e)), false))
                        : Optional.of(Domain.notNull(t)),
                IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME, (t, s, e, atStart) -> atStart
                        ? Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(t, s), Range.greaterThanOrEqual(t, e)), true))
                        : Optional.of(Domain.all(t)),
                LESS_THAN_OPERATOR_FUNCTION_NAME, (t, s, e, atStart) -> Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(t, atStart ? s : e)), false)),
                LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, (t, s, e, atStart) -> Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(t, e)), false)),
                GREATER_THAN_OPERATOR_FUNCTION_NAME, (t, s, e, atStart) -> Optional.of(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(t, e)), false)),
                GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, (t, s, e, atStart) -> Optional.of(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(t, atStart ? s : e)), false))
        );

        return Optional.ofNullable(domainCreators.get(functionName))
                .map(creator -> creator.create(type, start, end, isConstantAtPeriodStart))
                .orElse(Optional.empty());
    }

    @FunctionalInterface
    private interface ComparisonDomainCreator {
        Optional<Domain> create(Type type, LongTimestampWithTimeZone start, LongTimestampWithTimeZone end, boolean isConstantAtPeriodStart);
    }

    /**
     * Represents a time interval defined by a start and end {@link ZonedDateTime}.
     */
    private static class PeriodInterval {
        ZonedDateTime start;
        ZonedDateTime end;

        PeriodInterval(ZonedDateTime start, ZonedDateTime end) {
            this.start = start;
            this.end = end;
        }
    }

    /**
     * Unwraps the year in a comparison between a timestamp with time zone column
     * and a constant.
     *
     * @param <C> the type of the column handle
     * @param functionName the name of the function being evaluated
     * @param yearSource the source expression for the year value
     * @param constant the constant value being compared against
     * @param assignments a map of column assignments
     * @param columnTypeProvider provides the column types for the given column handles
     * @return an Optional containing the unwrapped TupleDomain if the yearSource is a variable
     *         representing a timestamp with time zone column, otherwise an empty Optional
     */
    private static <C> Optional<TupleDomain<C>> unwrapYearInTimestampTzComparison(
            // upon invocation, we don't know if this really is a comparison
            FunctionName functionName,
            ConnectorExpression yearSource,
            Constant constant,
            Map<String, ColumnHandle> assignments,
            ColumnTypeProvider<C> columnTypeProvider)
    {
        if (!(yearSource instanceof Variable sourceVariable)) {
            // Engine unwraps year in comparisons in UnwrapYearInComparison. Within a connector we can do more than
            // engine only for source columns. We cannot draw many conclusions for intermediate expressions without
            // knowing them well.
            return Optional.empty();
        }

        if (constant.getValue() == null) {
            // Comparisons with NULL should be simplified by the engine
            return Optional.empty();
        }

        C column = resolve(sourceVariable, assignments);
        Optional<Type> columnType = columnTypeProvider.getType(column);
        if (columnType.isPresent() && columnType.get()  instanceof TimestampWithTimeZoneType type) {
            // Iceberg supports only timestamp(6) with time zone
            checkArgument(type.getPrecision() == 6, "Unexpected type: %s", columnType);

            return unwrapYearInTimestampTzComparison(functionName, type, constant)
                    .map(domain -> TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)));
        }

        return Optional.empty();
    }

    /**
     * Resolves a Variable to its corresponding ColumnHandle assignment.
     *
     * @param <C> the type of the ColumnHandle
     * @param variable the Variable to resolve
     * @param assignments the map of assignments where the Variable must be present
     * @return the corresponding ColumnHandle assignment for the Variable
     * @throws IllegalArgumentException if no assignment is found for the Variable
     */
    private static <C> C resolve(Variable variable, Map<String, ColumnHandle> assignments) {
        ColumnHandle columnHandle = assignments.get(variable.getName());
        checkArgument(columnHandle != null, "No assignment for %s", variable);
        return (C) columnHandle;
    }

    /**
     * This record class, `ExtractionResult`, represents the result of an operation
     * that extracts a tuple domain and the remaining expressions from some particular constraint.
     * It encapsulates both the tuple domain and the remaining connector expression after the extraction.
     *
     * @param <C> A generic type that extends `ColumnHandle`.
     *           It represents the type of the column handle that the `TupleDomain` consists of.
     *
     * @record
     */
    public record ExtractionResult<C extends ColumnHandle>(TupleDomain<C> tupleDomain, ConnectorExpression remainingExpression)
    {

        /**
         * This is a constructor for the `ExtractionResult`. It checks that both `tupleDomain` and
         * `remainingExpression` are not null before initializing.
         *
         * If either `tupleDomain` or `remainingExpression` is null, it throws a `NullPointerException`.
         */
        public ExtractionResult
        {
            requireNonNull(tupleDomain, "tupleDomain is null");
            requireNonNull(remainingExpression, "remainingExpression is null");
        }
    }
}
