package sbe.generation.csharp;

import uk.co.real_logic.sbe.PrimitiveType;
import uk.co.real_logic.sbe.SbeTool;
import uk.co.real_logic.sbe.ValidationUtil;
import uk.co.real_logic.sbe.generation.Generators;

import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

/**
 * Utilities for mapping between IR and the Java language.
 */
public class CSharpUtil {
    /**
     * Package in which the generated Java interfaces will be placed.
     */
    public static final String CSHARP_INTERFACE_PACKAGE = "Adaptive.Agrona.SBE";
    private static final Map<PrimitiveType, String> TYPE_NAME_BY_PRIMITIVE_TYPE_MAP =
            new EnumMap<>(PrimitiveType.class);
    /**
     * Indexes known charset aliases to the name of the instance in {@link StandardCharsets}.
     */
    private static final Map<String, String> STD_CHARSETS = new HashMap<>();

    static {
        TYPE_NAME_BY_PRIMITIVE_TYPE_MAP.put(PrimitiveType.CHAR, "byte");
        TYPE_NAME_BY_PRIMITIVE_TYPE_MAP.put(PrimitiveType.INT8, "sbyte");
        TYPE_NAME_BY_PRIMITIVE_TYPE_MAP.put(PrimitiveType.INT16, "short");
        TYPE_NAME_BY_PRIMITIVE_TYPE_MAP.put(PrimitiveType.INT32, "int");
        TYPE_NAME_BY_PRIMITIVE_TYPE_MAP.put(PrimitiveType.INT64, "long");
        TYPE_NAME_BY_PRIMITIVE_TYPE_MAP.put(PrimitiveType.UINT8, "byte");
        TYPE_NAME_BY_PRIMITIVE_TYPE_MAP.put(PrimitiveType.UINT16, "ushort");
        TYPE_NAME_BY_PRIMITIVE_TYPE_MAP.put(PrimitiveType.UINT32, "uint");
        TYPE_NAME_BY_PRIMITIVE_TYPE_MAP.put(PrimitiveType.UINT64, "ulong");
        TYPE_NAME_BY_PRIMITIVE_TYPE_MAP.put(PrimitiveType.FLOAT, "float");
        TYPE_NAME_BY_PRIMITIVE_TYPE_MAP.put(PrimitiveType.DOUBLE, "double");
    }

    static {
        STD_CHARSETS.put("ASCII", "ASCII");
        STD_CHARSETS.put("US-ASCII", "ASCII");
        STD_CHARSETS.put("UTF-8", "UTF8");
        STD_CHARSETS.put("UTF8", "UTF8");
    }

    /**
     * Map the name of a {@link uk.co.real_logic.sbe.PrimitiveType} to a Java primitive type name.
     *
     * @param primitiveType to map.
     * @return the name of the Java primitive that most closely maps.
     */
    public static String csharpTypeName(final PrimitiveType primitiveType) {
        return TYPE_NAME_BY_PRIMITIVE_TYPE_MAP.get(primitiveType);
    }

    /**
     * Format a property name for generated code.
     * <p>
     * If the formatted property name is a keyword then {@link SbeTool#KEYWORD_APPEND_TOKEN} is appended if set.
     *
     * @param value to be formatted.
     * @return the string formatted as a property name.
     * @throws IllegalStateException if a keyword and {@link SbeTool#KEYWORD_APPEND_TOKEN} is not set.
     */
    public static String formatPropertyName(final String value) {
        String formattedValue = Generators.toUpperFirstChar(value);

        if (ValidationUtil.isCSharpKeyword(formattedValue)) {
            final String keywordAppendToken = System.getProperty(SbeTool.KEYWORD_APPEND_TOKEN);
            if (null == keywordAppendToken) {
                throw new IllegalStateException(
                        "Invalid property name='" + formattedValue +
                                "' please correct the schema or consider setting system property: " + SbeTool.KEYWORD_APPEND_TOKEN);
            }

            formattedValue += keywordAppendToken;
        }

        return formattedValue;
    }

    /**
     * Format a class name for the generated code.
     *
     * @param value to be formatted.
     * @return the string formatted as a class name.
     */
    public static String formatClassName(final String value) {
        return Generators.toUpperFirstChar(value);
    }

    /**
     * f
     * Shortcut to append a line of generated code
     *
     * @param builder string builder to which to append the line
     * @param indent  current text indentation
     * @param line    line to be appended
     */
    public static void append(final StringBuilder builder, final String indent, final String line) {
        builder.append(indent).append(line).append('\n');
    }

    /**
     * Code to fetch an instance of {@link java.nio.charset.Charset} corresponding to the given encoding.
     *
     * @param encoding as a string name (eg. UTF-8).
     * @return the code to fetch the associated Charset.
     */
    public static String charset(final String encoding) {
        final String charsetName = STD_CHARSETS.get(encoding);
        if (charsetName != null) {
            return "Encoding." + charsetName;
        } else {
            return "Encoding.GetEncoding(\"" + encoding + "\")";
        }
    }

    public static String getCSharpByteOrder(final ByteOrder byteOrder) {
        if (byteOrder == ByteOrder.BIG_ENDIAN) {
            return "ByteOrder.BigEndian";
        } else {
            return "ByteOrder.LittleEndian";
        }
    }

    /**
     * Generate a literal value to be used in code generation.
     *
     * @param type  of the lateral value.
     * @param value of the lateral.
     * @return a String representation of the Java literal.
     */
    public static String generateLiteral(final PrimitiveType type, final String value) {
        String literal = "";

        final String castType = csharpTypeName(type);
        switch (type) {
            case CHAR:
            case UINT8:
            case INT8:
            case INT16:
                literal = "(" + castType + ")" + value;
                break;

            case UINT16:
            case INT32:
                literal = value;
                break;

            case UINT32:
                literal = value;
                break;

            case FLOAT:
                literal = value.endsWith("NaN") ? "float.NaN" : value + "f";
                break;

            case INT64:
                literal = value + "L";
                break;

            case UINT64:
                literal = "0x" + Long.toHexString(Long.parseLong(value)) + "L";
                break;

            case DOUBLE:
                literal = value.endsWith("NaN") ? "double.NaN" : value + "d";
                break;
        }

        return literal;
    }

    public enum Separators {
        BEGIN_GROUP('['),
        END_GROUP(']'),
        BEGIN_COMPOSITE('('),
        END_COMPOSITE(')'),
        BEGIN_SET('{'),
        END_SET('}'),
        BEGIN_ARRAY('['),
        END_ARRAY(']'),
        FIELD('|'),
        KEY_VALUE('='),
        ENTRY(',');

        public final char symbol;

        Separators(final char symbol) {
            this.symbol = symbol;
        }

        /**
         * Add separator to a generated StringBuilder
         *
         * @param builder          the code generation builder to which information should be added
         * @param indent           the current generated code indentation
         * @param generatedBuilder the name of the generated StringBuilder to which separator should be added
         */
        public void appendToGeneratedBuilder(
                final StringBuilder builder, final String indent, final String generatedBuilder) {
            append(builder, indent, generatedBuilder + ".Append('" + symbol + "');");
        }

        public String toString() {
            return String.valueOf(symbol);
        }
    }
}
