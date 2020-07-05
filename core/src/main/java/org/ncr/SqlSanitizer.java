package org.ncr;


import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Optional;
import java.util.function.Function;

public class SqlSanitizer {


    public static String escape(String value) {
        if (value == null) { return ""; }
        return value.trim()
                .replaceAll("", "")
                .replaceAll("'", "''")
                .replaceAll("/\\*.*\\*/", "")
                ;
    }

    public static String escape(Object value) {
        return escape(value, v -> v == null ? "null" : v.toString());
    }

    public static String escape(Object value, Function<Object, String> toString) {
        if (value == null) { return "null"; }

        if (value instanceof Integer
                || value instanceof Float
                || value instanceof Double
                || value instanceof Boolean
                || value instanceof Long
                || value instanceof BigDecimal) {
            return value.toString();
        } else if (value instanceof Date) {
            return SimpleCalendar.MYSQL_DATE.format((Date)value);
        }

        return escape(toString.apply(value));
    }

    public static String escapeAndQuote(String value) {
        return "'"+ escape(value) +"'";
    }

    public static String escapeAndQuote(Object value) {
        if (value == null) { return null; }
        if ((value instanceof Integer)
                || (value instanceof Double)
                || (value instanceof Float)
                || (value instanceof Long)
                || (value instanceof BigInteger)
                || (value instanceof BigDecimal)
                || (value instanceof Number)
                || (value instanceof Boolean)
           ) {
            return value.toString();
        }

        if ((value instanceof Collection)) {
            return ((Collection<Object>)value).stream()
                    .map(v -> Optional.of(escapeAndQuote(v)))
                    .reduce(new Join(","))
                    .orElse(Optional.ofNullable(null))
                    .get()
                    ;
        }
        if (value instanceof Object[] || value instanceof Array) {
            return Arrays.stream(((Object[]) value))
                    .map(v -> Optional.of(escapeAndQuote(v)))
                    .reduce(new Join(","))
                    .orElse(Optional.ofNullable(null))
                    .get()
                    ;
        }

        return "'"+ escape(value) +"'";
    }

    public static String escapeAndQuote(Object value, Function<Object, String> toString) {
        return "'"+ escape(value, toString) +"'";
    }
}
