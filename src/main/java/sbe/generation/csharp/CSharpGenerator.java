package sbe.generation.csharp;

import org.agrona.Verify;
import org.agrona.generation.OutputManager;
import uk.co.real_logic.sbe.PrimitiveType;
import uk.co.real_logic.sbe.PrimitiveValue;
import uk.co.real_logic.sbe.generation.CodeGenerator;
import uk.co.real_logic.sbe.generation.Generators;
import uk.co.real_logic.sbe.ir.*;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static sbe.generation.csharp.CSharpGenerator.CodecType.DECODER;
import static sbe.generation.csharp.CSharpGenerator.CodecType.ENCODER;
import static sbe.generation.csharp.CSharpUtil.*;
import static uk.co.real_logic.sbe.generation.Generators.toUpperFirstChar;
import static uk.co.real_logic.sbe.ir.GenerationUtil.*;

public class CSharpGenerator implements CodeGenerator {
    private static final String META_ATTRIBUTE_ENUM = "MetaAttribute";
    private static final String BASE_INDENT = "";
    private static final String INDENT = "    ";
    private static final String GEN_COMPOSITE_DECODER_FLYWEIGHT = "ICompositeDecoderFlyweight";
    private static final String GEN_COMPOSITE_ENCODER_FLYWEIGHT = "ICompositeEncoderFlyweight";
    private static final String GEN_MESSAGE_DECODER_FLYWEIGHT = "IMessageDecoderFlyweight";
    private static final String GEN_MESSAGE_ENCODER_FLYWEIGHT = "IMessageEncoderFlyweight";
    private final Ir ir;
    private final OutputManager outputManager;
    private final String fqMutableBuffer;
    private final String mutableBuffer;
    private final String fqReadOnlyBuffer;
    private final String readOnlyBuffer;
    private final boolean shouldGenerateGroupOrderAnnotation;
    private final boolean shouldGenerateInterfaces;
    private final boolean shouldDecodeUnknownEnumValues;

    public CSharpGenerator(
            final Ir ir,
            final String mutableBuffer,
            final String readOnlyBuffer,
            final boolean shouldGenerateGroupOrderAnnotation,
            final boolean shouldGenerateInterfaces,
            final boolean shouldDecodeUnknownEnumValues,
            final OutputManager outputManager) {
        Verify.notNull(ir, "ir");
        Verify.notNull(outputManager, "outputManager");

        this.ir = ir;
        this.outputManager = outputManager;

        this.mutableBuffer = "IMutableDirectBuffer";
        this.fqMutableBuffer = "Adaptive.Agrona";

        this.readOnlyBuffer = "IDirectBuffer";
        this.fqReadOnlyBuffer = "Adaptive.Agrona";

        this.shouldGenerateGroupOrderAnnotation = shouldGenerateGroupOrderAnnotation;
        this.shouldGenerateInterfaces = shouldGenerateInterfaces;
        this.shouldDecodeUnknownEnumValues = shouldDecodeUnknownEnumValues;
    }

    private static String primitiveTypeName(final Token token) {
        return CSharpUtil.csharpTypeName(token.encoding().primitiveType());
    }

    private static CharSequence generateGroupDecoderProperty(
            final String groupName, final Token token, final String indent) {
        final StringBuilder sb = new StringBuilder();
        final String className = formatClassName(groupName);
        final String propertyName = formatPropertyName(token.name());

        sb.append(String.format(
                "\n" +
                        indent + "    private %s _%s = new %s();\n",
                className,
                propertyName,
                className));

        sb.append(String.format(
                "\n" +
                        indent + "    public static long %sId()\n" +
                        indent + "    {\n" +
                        indent + "        return %d;\n" +
                        indent + "    }\n",
                formatPropertyName(groupName),
                token.id()));

        sb.append(String.format(
                "\n" +
                        indent + "    public static int %sSinceVersion()\n" +
                        indent + "    {\n" +
                        indent + "        return %d;\n" +
                        indent + "    }\n",
                formatPropertyName(groupName),
                token.version()));

        final String actingVersionGuard = token.version() == 0 ?
                "" :
                indent + "        if (_parentMessage._actingVersion < " + token.version() + ")\n" +
                        indent + "        {\n" +
                        indent + "            _" + propertyName + ".WrapEmpty();\n" +
                        indent + "            return _" + propertyName + ";\n" +
                        indent + "        }\n\n";

        sb.append(String.format(
                "\n" +
                        indent + "    public %1$s %2$s()\n" +
                        indent + "    {\n" + "%3$s" +
                        indent + "        _%2$s.Wrap(_parentMessage, _buffer);\n" +
                        indent + "        return _%2$s;\n" +
                        indent + "    }\n",
                className,
                propertyName,
                actingVersionGuard));

        return sb;
    }

    private static CharSequence generateEnumFileHeader(final String packageName) {
        return String.format(
                "/* Generated SBE (Simple Binary Encoding) message codec */\n" +
                        "namespace %s {\n\n",
                packageName);
    }

    private static CharSequence generateDeclaration(final String className, final String implementsString) {
        return String.format(
                "public class %s%s\n" +
                        "{\n",
                className,
                implementsString);
    }

    private static CharSequence generateEnumDeclaration(
            final String name,
            final String primitiveType,
            final boolean addFlagsAttribute) {
        String result = "";
        if (addFlagsAttribute) {
            result += INDENT + "[Flags]\n";
        }

        result +=
                INDENT + "public enum " + name + " : " + primitiveType + "\n" +
                        INDENT + "{\n";

        return result;
    }

    private static CharSequence generateArrayFieldNotPresentCondition(final int sinceVersion, final String indent) {
        if (0 == sinceVersion) {
            return "";
        }

        return String.format(
                indent + "        if (_parentMessage._actingVersion < %d)\n" +
                        indent + "        {\n" +
                        indent + "            return 0;\n" +
                        indent + "        }\n\n",
                sinceVersion);
    }

    private static CharSequence generateStringNotPresentCondition(final int sinceVersion, final String indent) {
        if (0 == sinceVersion) {
            return "";
        }

        return String.format(
                indent + "        if (_parentMessage._actingVersion < %d)\n" +
                        indent + "        {\n" +
                        indent + "            return \"\";\n" +
                        indent + "        }\n\n",
                sinceVersion);
    }

    private static CharSequence generatePropertyNotPresentCondition(
            final boolean inComposite, final CodecType codecType, final int sinceVersion, final String indent) {
        if (inComposite || codecType == ENCODER || 0 == sinceVersion) {
            return "";
        }

        return String.format(
                indent + "        if (_parentMessage._actingVersion < %d)\n" +
                        indent + "        {\n" +
                        indent + "            return null;\n" +
                        indent + "        }\n\n",
                sinceVersion);
    }

    private static void generateArrayLengthMethod(
            final String propertyName, final String indent, final int fieldLength, final StringBuilder sb) {
        sb.append(String.format(
                "\n" +
                        indent + "    public static int %sLength()\n" +
                        indent + "    {\n" +
                        indent + "        return %d;\n" +
                        indent + "    }\n\n",
                propertyName,
                fieldLength));
    }

    private static int sizeOfPrimitive(final Encoding encoding) {
        return encoding.primitiveType().size();
    }

    private static void generateCharacterEncodingMethod(
            final StringBuilder sb, final String propertyName, final String characterEncoding, final String indent) {
        if (null != characterEncoding) {
            sb.append(String.format(
                    "\n" +
                            indent + "    public static string %sCharacterEncoding()\n" +
                            indent + "    {\n" +
                            indent + "        return \"%s\";\n" +
                            indent + "    }\n",
                    formatPropertyName(propertyName),
                    characterEncoding));
        }
    }

    private static CharSequence generateByteLiteralList(final byte[] bytes) {
        final StringBuilder values = new StringBuilder();
        for (final byte b : bytes) {
            values.append(b).append(", ");
        }

        if (values.length() > 0) {
            values.setLength(values.length() - 2);
        }

        return values;
    }

    private CharSequence generateFixedFlyweightCode(
            final String className, final int size, final String bufferImplementation) {

        final HeaderStructure headerStructure = ir.headerStructure();
        final String schemaIdType = csharpTypeName(headerStructure.schemaIdType());
        final String schemaVersionType = csharpTypeName(headerStructure.schemaVersionType());
        return String.format(
                "    public const int ENCODED_LENGTH = %2$d;\n" +
                "    public const %4$s SCHEMA_ID = %5$s;\n" +
                "    public const %6$s SCHEMA_VERSION = %7$s;\n\n" +
                        "    private %3$s _buffer;\n" +
                        "    private int _offset;\n\n" +
                        "    public %1$s Wrap(%3$s buffer, int offset)\n" +
                        "    {\n" +
                        "        this._buffer = buffer;\n" +
                        "        this._offset = offset;\n\n" +
                        "        return this;\n" +
                        "    }\n\n" +
                        "    public %3$s Buffer()\n" +
                        "    {\n" +
                        "        return _buffer;\n" +
                        "    }\n\n" +
                        "    public int Offset()\n" +
                        "    {\n" +
                        "        return _offset;\n" +
                        "    }\n\n" +
                        "    public int EncodedLength()\n" +
                        "    {\n" +
                        "        return ENCODED_LENGTH;\n" +
                        "    }\n",
                className,
                size,
                bufferImplementation,
                schemaIdType,
                generateLiteral(headerStructure.schemaIdType(), Integer.toString(ir.id())),
                schemaVersionType,
                generateLiteral(headerStructure.schemaVersionType(), Integer.toString(ir.version()))
                );
    }

    private CharSequence generateCompositeFlyweightCode(
            final String className, final int size, final String bufferImplementation, final String compositeReturnType) {

        final HeaderStructure headerStructure = ir.headerStructure();
        final String schemaIdType = csharpTypeName(headerStructure.schemaIdType());
        final String schemaVersionType = csharpTypeName(headerStructure.schemaVersionType());
        return String.format(
                "    public const int ENCODED_LENGTH = %2$d;\n" +
                "    public const %5$s SCHEMA_ID = %6$s;\n" +
                "    public const %7$s SCHEMA_VERSION = %8$s;\n\n" +
                        "    private int _offset;\n" +
                        "    private %3$s _buffer;\n\n" +
                        "    public %4$s Wrap(%3$s buffer, int offset)\n" +
                        "    {\n" +
                        "        this._buffer = buffer;\n" +
                        "        this._offset = offset;\n\n" +
                        "        return this;\n" +
                        "    }\n\n" +
                        "    public %3$s Buffer()\n" +
                        "    {\n" +
                        "        return _buffer;\n" +
                        "    }\n\n" +
                        "    public int Offset()\n" +
                        "    {\n" +
                        "        return _offset;\n" +
                        "    }\n\n" +
                        "    public int EncodedLength()\n" +
                        "    {\n" +
                        "        return ENCODED_LENGTH;\n" +
                        "    }\n",
                className,
                size,
                bufferImplementation,
                compositeReturnType,
                schemaIdType,
                generateLiteral(headerStructure.schemaIdType(), Integer.toString(ir.id())),
                schemaVersionType,
                generateLiteral(headerStructure.schemaVersionType(), Integer.toString(ir.version())));
    }

    private static void generateFieldIdMethod(final StringBuilder sb, final Token token, final String indent) {
        sb.append(String.format(
                "\n" +
                        indent + "    public static int %sId()\n" +
                        indent + "    {\n" +
                        indent + "        return %d;\n" +
                        indent + "    }\n",
                formatPropertyName(token.name()),
                token.id()));
    }

    private static void generateEncodingOffsetMethod(
            final StringBuilder sb, final String name, final int offset, final String indent) {
        sb.append(String.format(
                "\n" +
                        indent + "    public static int %sEncodingOffset()\n" +
                        indent + "    {\n" +
                        indent + "        return %d;\n" +
                        indent + "    }\n",
                formatPropertyName(name),
                offset));
    }

    private static void generateEncodingLengthMethod(
            final StringBuilder sb, final String name, final int length, final String indent) {
        sb.append(String.format(
                "\n" +
                        indent + "    public static int %sEncodingLength()\n" +
                        indent + "    {\n" +
                        indent + "        return %d;\n" +
                        indent + "    }\n",
                formatPropertyName(name),
                length));
    }

    private static void generateFieldSinceVersionMethod(final StringBuilder sb, final Token token, final String indent) {
        sb.append(String.format(
                "\n" +
                        indent + "    public static int %sSinceVersion()\n" +
                        indent + "    {\n" +
                        indent + "        return %d;\n" +
                        indent + "    }\n",
                formatPropertyName(token.name()),
                token.version()));
    }

    private static void generateFieldMetaAttributeMethod(final StringBuilder sb, final Token token, final String indent) {
        final Encoding encoding = token.encoding();
        final String epoch = encoding.epoch() == null ? "" : encoding.epoch();
        final String timeUnit = encoding.timeUnit() == null ? "" : encoding.timeUnit();
        final String semanticType = encoding.semanticType() == null ? "" : encoding.semanticType();

        sb.append(String.format(
                "\n" +
                        indent + "    public static string %sMetaAttribute(MetaAttribute metaAttribute)\n" +
                        indent + "    {\n" +
                        indent + "        switch (metaAttribute)\n" +
                        indent + "        {\n" +
                        indent + "            case MetaAttribute.EPOCH: return \"%s\";\n" +
                        indent + "            case MetaAttribute.TIME_UNIT: return \"%s\";\n" +
                        indent + "            case MetaAttribute.SEMANTIC_TYPE: return \"%s\";\n" +
                        indent + "            case MetaAttribute.PRESENCE: return \"%s\";\n" +
                        indent + "        }\n\n" +
                        indent + "        return \"\";\n" +
                        indent + "    }\n",
                formatPropertyName(token.name()),
                epoch,
                timeUnit,
                semanticType,
                encoding.presence().toString().toLowerCase()));
    }

    private String encoderName(final String className) {
        return className + "Encoder";
    }

    private String decoderName(final String className) {
        return className + "Decoder";
    }

    private String implementsInterface(final String interfaceName) {
        if (!shouldGenerateInterfaces) {
            return "";
        } else {
            return " : " + interfaceName;
        }
    }

    private String generateDecoderExplicitInterface() {
        if (!shouldGenerateInterfaces) {
            return "";
        } else {
            return String.format("%1$s %1$s.Wrap(%2$s buffer, int offset)\n" +
                            "    {\n" +
                            "        return Wrap(buffer, offset);\n" +
                            "    }",
                    "IEncoderFlyweight",
                    mutableBuffer


            );


        }
    }

    public void generateMessageHeaderStub() throws IOException {
        generateComposite(ir.headerStructure().tokens());
    }

    public void generateTypeStubs() throws IOException {
        generateMetaAttributeEnum();

        for (final List<Token> tokens : ir.types()) {
            switch (tokens.get(0).signal()) {
                case BEGIN_ENUM:
                    generateEnum(tokens);
                    break;

                case BEGIN_SET:
                    generateBitSet(tokens);
                    break;

                case BEGIN_COMPOSITE:
                    generateComposite(tokens);
                    break;
            }
        }
    }

    public void generate() throws IOException {
        generateTypeStubs();
        generateMessageHeaderStub();

        for (final List<Token> tokens : ir.messages()) {
            final Token msgToken = tokens.get(0);
            final List<Token> messageBody = getMessageBody(tokens);

            int i = 0;
            final List<Token> fields = new ArrayList<>();
            i = collectFields(messageBody, i, fields);

            final List<Token> groups = new ArrayList<>();
            i = collectGroups(messageBody, i, groups);

            final List<Token> varData = new ArrayList<>();
            collectVarData(messageBody, i, varData);

            generateDecoder(BASE_INDENT, fields, groups, varData, msgToken);
            generateEncoder(BASE_INDENT, fields, groups, varData, msgToken);
        }
    }

    private void generateEncoder(
            final String indent,
            final List<Token> fields,
            final List<Token> groups,
            final List<Token> varData,
            final Token msgToken) throws IOException {
        final String className = formatClassName(encoderName(msgToken.name()));
        final String implementsString = implementsInterface(GEN_MESSAGE_ENCODER_FLYWEIGHT);

        try (Writer out = outputManager.createOutput(className)) {
            out.append(generateMainHeader(namespace()));

            generateAnnotations(indent, className, groups, out, 0, this::encoderName);
            out.append(generateDeclaration(className, implementsString));
            out.append(generateDecoderExplicitInterface());
            out.append(generateEncoderFlyweightCode(className, msgToken));
            out.append(generateEncoderFields(className, fields, indent));

            final StringBuilder sb = new StringBuilder();
            generateEncoderGroups(sb, className, groups, indent);
            out.append(sb);

            out.append(generateEncoderVarData(className, varData, indent));

            out.append(generateEncoderDisplay(formatClassName(decoderName(msgToken.name())), indent));

            out.append("}\n");
            out.append("}\n");
        }
    }

    private void generateDecoder(
            final String indent,
            final List<Token> fields,
            final List<Token> groups,
            final List<Token> varData,
            final Token msgToken) throws IOException {
        final String className = formatClassName(decoderName(msgToken.name()));
        final String implementsString = implementsInterface(GEN_MESSAGE_DECODER_FLYWEIGHT);

        try (Writer out = outputManager.createOutput(className)) {
            out.append(generateMainHeader(namespace()));

            generateAnnotations(indent, className, groups, out, 0, this::decoderName);
            out.append(generateDeclaration(className, implementsString));
            out.append(generateDecoderFlyweightCode(className, msgToken));
            out.append(generateDecoderFields(fields, indent));

            final StringBuilder sb = new StringBuilder();
            generateDecoderGroups(sb, className, groups, indent);
            out.append(sb);

            out.append(generateDecoderVarData(varData, indent));

            out.append(generateDecoderDisplay(msgToken.name(), fields, groups, varData, indent));

            out.append("}\n");
            out.append("}\n");
        }
    }

    private void generateDecoderGroups(
            final StringBuilder sb,
            final String outerClassName,
            final List<Token> tokens,
            final String indent) throws IOException {
        for (int i = 0, size = tokens.size(); i < size; i++) {
            final Token groupToken = tokens.get(i);
            if (groupToken.signal() != Signal.BEGIN_GROUP) {
                throw new IllegalStateException("tokens must begin with BEGIN_GROUP: token=" + groupToken);
            }

            final String groupName = decoderName(formatClassName(groupToken.name()));
            sb.append(generateGroupDecoderProperty(groupName, groupToken, indent));

            generateAnnotations(indent + INDENT, groupName, tokens, sb, i + 1, this::decoderName);
            generateGroupDecoderClassHeader(sb, groupName, outerClassName, tokens, i, indent + INDENT);

            ++i;
            final int groupHeaderTokenCount = tokens.get(i).componentTokenCount();
            i += groupHeaderTokenCount;

            final List<Token> fields = new ArrayList<>();
            i = collectFields(tokens, i, fields);
            sb.append(generateDecoderFields(fields, indent + INDENT));

            final List<Token> groups = new ArrayList<>();
            i = collectGroups(tokens, i, groups);
            generateDecoderGroups(sb, outerClassName, groups, indent + INDENT);

            final List<Token> varData = new ArrayList<>();
            i = collectVarData(tokens, i, varData);
            sb.append(generateDecoderVarData(varData, indent + INDENT));

            appendGroupInstanceDecoderDisplay(sb, fields, groups, varData, indent + INDENT);

            sb.append(indent).append("    }\n");
        }
    }

    private void generateEncoderGroups(
            final StringBuilder sb,
            final String outerClassName,
            final List<Token> tokens,
            final String indent) throws IOException {
        for (int i = 0, size = tokens.size(); i < size; i++) {
            final Token groupToken = tokens.get(i);
            if (groupToken.signal() != Signal.BEGIN_GROUP) {
                throw new IllegalStateException("tokens must begin with BEGIN_GROUP: token=" + groupToken);
            }

            final String groupName = groupToken.name();
            final String groupClassName = formatClassName(encoderName(groupName));
            sb.append(generateGroupEncoderProperty(groupName, groupToken, indent));

            generateAnnotations(indent + INDENT, groupClassName, tokens, sb, i + 1, this::encoderName);
            generateGroupEncoderClassHeader(sb, groupName, outerClassName, tokens, i, indent + INDENT);

            ++i;
            final int groupHeaderTokenCount = tokens.get(i).componentTokenCount();
            i += groupHeaderTokenCount;

            final List<Token> fields = new ArrayList<>();
            i = collectFields(tokens, i, fields);
            sb.append(generateEncoderFields(groupClassName, fields, indent + INDENT));

            final List<Token> groups = new ArrayList<>();
            i = collectGroups(tokens, i, groups);
            generateEncoderGroups(sb, outerClassName, groups, indent + INDENT);

            final List<Token> varData = new ArrayList<>();
            i = collectVarData(tokens, i, varData);
            sb.append(generateEncoderVarData(groupClassName, varData, indent + INDENT));

            sb.append(indent).append("    }\n");
        }
    }

    private void generateGroupDecoderClassHeader(
            final StringBuilder sb,
            final String groupName,
            final String parentMessageClassName,
            final List<Token> tokens,
            final int index,
            final String indent) {
        final String dimensionsClassName = formatClassName(tokens.get(index + 1).name());
        final int dimensionHeaderLen = tokens.get(index + 1).encodedLength();

        generateGroupDecoderClassDeclaration(
                sb, groupName, parentMessageClassName, indent, dimensionsClassName, dimensionHeaderLen);

        sb.append(
                indent + "    public void WrapEmpty()\n" +
                        indent + "    {\n" +
                        indent + "        _index = -1;\n" +
                        indent + "        _count = 0;\n" +
                        indent + "    }\n\n"
        );

        sb.append(String.format(
                indent + "    public void Wrap(\n" +
                        indent + "        %s parentMessage, %s buffer)\n" +
                        indent + "    {\n" +
                        indent + "        this._parentMessage = parentMessage;\n" +
                        indent + "        this._buffer = buffer;\n" +
                        indent + "        _dimensions.Wrap(buffer, parentMessage.Limit());\n" +
                        indent + "        _blockLength = _dimensions.BlockLength();\n" +
                        indent + "        _count = _dimensions.NumInGroup();\n" +
                        indent + "        _index = -1;\n" +
                        indent + "        parentMessage.Limit(parentMessage.Limit() + HEADER_SIZE);\n" +
                        indent + "    }\n\n",
                parentMessageClassName,
                readOnlyBuffer));

        final int blockLength = tokens.get(index).encodedLength();

        sb.append(indent).append("    public static int SbeHeaderSize()\n")
                .append(indent).append("    {\n")
                .append(indent).append("        return HEADER_SIZE;\n")
                .append(indent).append("    }\n\n");

        sb.append(String.format(
                indent + "    public static int SbeBlockLength()\n" +
                        indent + "    {\n" +
                        indent + "        return %d;\n" +
                        indent + "    }\n\n",
                blockLength));

        sb.append(String.format(
                indent + "    public int ActingBlockLength()\n" +
                        indent + "    {\n" +
                        indent + "        return _blockLength;\n" +
                        indent + "    }\n\n" +
                        indent + "    public long LongCount()\n" +
                        indent + "    {\n" +
                        indent + "        return _count;\n" +
                        indent + "    }\n\n" +
                        indent + "    public int Count()\n" +
                        indent + "    {\n" +
                        indent + "        if (_count > Int32.MaxValue)\n" +
                        indent + "        {\n" +
                        indent + "            throw new InvalidOperationException(\"count cannot be represented by a 32-bit int\");\n" +
                        indent + "        }\n" +
                        indent + "        return (int) _count;\n" +
                        indent + "    }\n\n" +
                        indent + "    public bool HasNext()\n" +
                        indent + "    {\n" +
                        indent + "        return (_index + 1) < _count;\n" +
                        indent + "    }\n\n",
                formatClassName(groupName)));

        sb.append(String.format(
                indent + "    public %s Next()\n" +
                        indent + "    {\n" +
                        indent + "        if (_index + 1 >= _count)\n" +
                        indent + "        {\n" +
                        indent + "            throw new IndexOutOfRangeException();\n" +
                        indent + "        }\n\n" +
                        indent + "        _offset = _parentMessage.Limit();\n" +
                        indent + "        _parentMessage.Limit(_offset + _blockLength);\n" +
                        indent + "        ++_index;\n\n" +
                        indent + "        return this;\n" +
                        indent + "    }\n",
                formatClassName(groupName)));

        sb.append(String.format(
                indent + "    public bool MoveNext()\n" +
                        indent + "    {\n" +
                        indent + "        if (_index + 1 >= _count)\n" +
                        indent + "        {\n" +
                        indent + "            return false;\n" +
                        indent + "        }\n\n" +
                        indent + "        _offset = _parentMessage.Limit();\n" +
                        indent + "        _parentMessage.Limit(_offset + _blockLength);\n" +
                        indent + "        ++_index;\n\n" +
                        indent + "        return true;\n" +
                        indent + "    }\n\n",
                formatClassName(groupName)));

        sb.append(String.format(
                indent + "    public IEnumerator<%s> GetEnumerator()\n" +
                        indent + "    {\n" +
                        indent + "        return this;\n" +
                        indent + "    }\n\n",
                formatClassName(groupName)));

        sb.append(String.format(
                indent + "    public %s Current => this;\n\n",
                formatClassName(groupName)));

        sb.append(
                indent + "    IEnumerator IEnumerable.GetEnumerator()\n" +
                        indent + "    {\n" +
                        indent + "        return GetEnumerator();\n" +
                        indent + "    }\n\n");

        sb.append(
                indent + "    public void Reset()\n" +
                        indent + "    {\n" +
                        indent + "        throw new NotSupportedException();\n" +
                        indent + "    }\n\n");

        sb.append(
                indent + "    object IEnumerator.Current => Current;\n\n"
        );

        sb.append(
                indent + "    public void Dispose()\n" +
                        indent + "    {\n" +
                        indent + "    }\n\n");
    }

    private void generateGroupEncoderClassHeader(
            final StringBuilder sb,
            final String groupName,
            final String parentMessageClassName,
            final List<Token> tokens,
            final int index,
            final String ind) {
        final String dimensionsClassName = formatClassName(encoderName(tokens.get(index + 1).name()));
        final int dimensionHeaderSize = tokens.get(index + 1).encodedLength();

        generateGroupEncoderClassDeclaration(
                sb, groupName, parentMessageClassName, ind, dimensionsClassName, dimensionHeaderSize);

        final int blockLength = tokens.get(index).encodedLength();
        final String javaTypeForBlockLength = primitiveTypeName(tokens.get(index + 2));
        final Token numInGroupToken = tokens.get(index + 3);
        final String javaTypeForNumInGroup = primitiveTypeName(numInGroupToken);

        sb.append(String.format(
                ind + "    public void Wrap(\n" +
                        ind + "        %1$s parentMessage, %2$s buffer, uint count)\n" +
                        ind + "    {\n" +
                        ind + "        if (count < %3$d || count > %4$d)\n" +
                        ind + "        {\n" +
                        ind + "            throw new ArgumentException(\"count outside allowed range: count=\" + count);\n" +
                        ind + "        }\n\n" +
                        ind + "        this._parentMessage = parentMessage;\n" +
                        ind + "        this._buffer = buffer;\n" +
                        ind + "        _dimensions.Wrap(buffer, parentMessage.Limit());\n" +
                        ind + "        _dimensions.BlockLength((%5$s)%6$d);\n" +
                        ind + "        _dimensions.NumInGroup((%7$s)count);\n" +
                        ind + "        _index = -1;\n" +
                        ind + "        this._count = count;\n" +
                        ind + "        parentMessage.Limit(parentMessage.Limit() + HEADER_SIZE);\n" +
                        ind + "    }\n\n",
                parentMessageClassName,
                mutableBuffer,
                numInGroupToken.encoding().applicableMinValue().longValue(),
                numInGroupToken.encoding().applicableMaxValue().longValue(),
                javaTypeForBlockLength,
                blockLength,
                javaTypeForNumInGroup));

        sb.append(ind).append("    public static int SbeHeaderSize()\n")
                .append(ind).append("    {\n")
                .append(ind).append("        return HEADER_SIZE;\n")
                .append(ind).append("    }\n\n");

        sb.append(String.format(
                ind + "    public static int SbeBlockLength()\n" +
                        ind + "    {\n" +
                        ind + "        return %d;\n" +
                        ind + "    }\n\n",
                blockLength));

        sb.append(String.format(
                ind + "    public %s Next()\n" +
                        ind + "    {\n" +
                        ind + "        if (_index + 1 >= _count)\n" +
                        ind + "        {\n" +
                        ind + "            throw new IndexOutOfRangeException();\n" +
                        ind + "        }\n\n" +
                        ind + "        _offset = _parentMessage.Limit();\n" +
                        ind + "        _parentMessage.Limit(_offset + SbeBlockLength());\n" +
                        ind + "        ++_index;\n\n" +
                        ind + "        return this;\n" +
                        ind + "    }\n",
                formatClassName(encoderName(groupName))));
    }

    private void generateGroupDecoderClassDeclaration(
            final StringBuilder sb,
            final String groupName,
            final String parentMessageClassName,
            final String indent,
            final String dimensionsClassName,
            final int dimensionHeaderSize) {
        sb.append(String.format(
                "\n" +
                        indent + "public class %1$s : IEnumerable<%1$s>, IEnumerator<%1$s>\n" +
                        indent + "{\n" +
                        indent + "    private static int HEADER_SIZE = %2$d;\n" +
                        indent + "    private %3$s _dimensions = new %3$s();\n" +
                        indent + "    private %4$s _parentMessage;\n" +
                        indent + "    private %5$s _buffer;\n" +
                        indent + "    private uint _count;\n" +
                        indent + "    private int _index;\n" +
                        indent + "    private int _offset;\n" +
                        indent + "    private int _blockLength;\n\n",
                formatClassName(groupName),
                dimensionHeaderSize,
                decoderName(dimensionsClassName),
                parentMessageClassName,
                readOnlyBuffer));
    }

    private void generateGroupEncoderClassDeclaration(
            final StringBuilder sb,
            final String groupName,
            final String parentMessageClassName,
            final String indent,
            final String dimensionsClassName,
            final int dimensionHeaderSize) {
        sb.append(String.format(
                "\n" +
                        indent + "public class %1$s\n" +
                        indent + "{\n" +
                        indent + "    private static int HEADER_SIZE = %2$d;\n" +
                        indent + "    private %3$s _dimensions = new %3$s();\n" +
                        indent + "    private %4$s _parentMessage;\n" +
                        indent + "    private %5$s _buffer;\n" +
                        indent + "    private uint _count;\n" +
                        indent + "    private int _index;\n" +
                        indent + "    private int _offset;\n\n",
                formatClassName(encoderName(groupName)),
                dimensionHeaderSize,
                dimensionsClassName,
                parentMessageClassName,
                mutableBuffer));
    }

    private CharSequence generateGroupEncoderProperty(final String groupName, final Token token, final String indent) {
        final StringBuilder sb = new StringBuilder();
        final String className = formatClassName(encoderName(groupName));
        final String propertyName = formatPropertyName(groupName);

        sb.append(String.format(
                "\n" +
                        indent + "    private %s _%s = new %s();\n",
                className,
                propertyName,
                className));

        sb.append(String.format(
                "\n" +
                        indent + "    public static long %sId()\n" +
                        indent + "    {\n" +
                        indent + "        return %d;\n" +
                        indent + "    }\n",
                formatPropertyName(groupName),
                token.id()));

        sb.append(String.format(
                "\n" +
                        indent + "    public %1$s %2$sCount(int count)\n" +
                        indent + "    {\n" +
                        indent + "        if (count < 0)\n" +
                        indent + "        {\n" +
                        indent + "            throw new ArgumentException(\"count must be >= 0\");\n" +
                        indent + "        }\n" +
                        indent + "        _%2$s.Wrap(_parentMessage, _buffer, (uint) count);\n" +
                        indent + "        return _%2$s;\n" +
                        indent + "    }\n",
                className,
                propertyName));

        sb.append(String.format(
                "\n" +
                        indent + "    public %1$s %2$sCount(uint count)\n" +
                        indent + "    {\n" +
                        indent + "        _%2$s.Wrap(_parentMessage, _buffer, count);\n" +
                        indent + "        return _%2$s;\n" +
                        indent + "    }\n",
                className,
                propertyName));

        return sb;
    }

    private CharSequence generateDecoderVarData(final List<Token> tokens, final String indent) {
        final StringBuilder sb = new StringBuilder();

        for (int i = 0, size = tokens.size(); i < size; ) {
            final Token token = tokens.get(i);
            if (token.signal() != Signal.BEGIN_VAR_DATA) {
                throw new IllegalStateException("tokens must begin with BEGIN_VAR_DATA: token=" + token);
            }

            generateFieldIdMethod(sb, token, indent);
            generateFieldSinceVersionMethod(sb, token, indent);

            final String characterEncoding = tokens.get(i + 3).encoding().characterEncoding();
            generateCharacterEncodingMethod(sb, token.name(), characterEncoding, indent);
            generateFieldMetaAttributeMethod(sb, token, indent);

            final String propertyName = toUpperFirstChar(token.name());
            final Token lengthToken = tokens.get(i + 2);
            final int sizeOfLengthField = lengthToken.encodedLength();
            final Encoding lengthEncoding = lengthToken.encoding();
            final PrimitiveType lengthType = lengthEncoding.primitiveType();
            final String byteOrderStr = byteOrderString(lengthEncoding);

            sb.append(String.format(
                    "\n" +
                            indent + "    public static int %sHeaderLength()\n" +
                            indent + "    {\n" +
                            indent + "        return %d;\n" +
                            indent + "    }\n",
                    toUpperFirstChar(propertyName),
                    sizeOfLengthField));

            sb.append(String.format(
                    "\n" +
                            indent + "    public int %sLength()\n" +
                            indent + "    {\n" +
                            "%s" +
                            indent + "        int limit = _parentMessage.Limit();\n" +
                            indent + "        return (int)%s;\n" +
                            indent + "    }\n",
                    toUpperFirstChar(propertyName),
                    generateArrayFieldNotPresentCondition(token.version(), indent),
                    generateGet(lengthType, "limit", byteOrderStr)));

            generateDataDecodeMethods(
                    sb, token, propertyName, sizeOfLengthField, lengthType, byteOrderStr, characterEncoding, indent);

            i += token.componentTokenCount();
        }

        return sb;
    }

    private CharSequence generateEncoderVarData(final String className, final List<Token> tokens, final String indent) {
        final StringBuilder sb = new StringBuilder();

        for (int i = 0, size = tokens.size(); i < size; ) {
            final Token token = tokens.get(i);
            if (token.signal() != Signal.BEGIN_VAR_DATA) {
                throw new IllegalStateException("tokens must begin with BEGIN_VAR_DATA: token=" + token);
            }

            generateFieldIdMethod(sb, token, indent);
            final String characterEncoding = tokens.get(i + 3).encoding().characterEncoding();
            generateCharacterEncodingMethod(sb, token.name(), characterEncoding, indent);
            generateFieldMetaAttributeMethod(sb, token, indent);

            final String propertyName = toUpperFirstChar(token.name());
            final Token lengthToken = tokens.get(i + 2);
            final int sizeOfLengthField = lengthToken.encodedLength();
            final Encoding lengthEncoding = lengthToken.encoding();
            final int maxLengthValue = (int) lengthEncoding.applicableMaxValue().longValue();
            final String byteOrderStr = byteOrderString(lengthEncoding);

            sb.append(String.format(
                    "\n" +
                            indent + "    public static int %sHeaderLength()\n" +
                            indent + "    {\n" +
                            indent + "        return %d;\n" +
                            indent + "    }\n",
                    toUpperFirstChar(propertyName),
                    sizeOfLengthField));

            generateDataEncodeMethods(
                    sb,
                    propertyName,
                    sizeOfLengthField,
                    maxLengthValue,
                    lengthEncoding.primitiveType(),
                    byteOrderStr,
                    characterEncoding,
                    className,
                    indent);

            i += token.componentTokenCount();
        }

        return sb;
    }

    private void generateDataDecodeMethods(
            final StringBuilder sb,
            final Token token,
            final String propertyName,
            final int sizeOfLengthField,
            final PrimitiveType lengthType,
            final String byteOrderStr,
            final String characterEncoding,
            final String indent) {
        generateDataTypedDecoder(
                sb,
                token,
                propertyName,
                sizeOfLengthField,
                mutableBuffer,
                lengthType,
                byteOrderStr,
                indent);

        generateDataTypedDecoder(
                sb,
                token,
                propertyName,
                sizeOfLengthField,
                "byte[]",
                lengthType,
                byteOrderStr,
                indent);

        if (null != characterEncoding) {
            sb.append(String.format(
                    "\n" +
                            indent + "    public string %1$s()\n" +
                            indent + "    {\n" +
                            "%2$s" +
                            indent + "        int headerLength = %3$d;\n" +
                            indent + "        int limit = _parentMessage.Limit();\n" +
                            indent + "        int dataLength = (int)%4$s;\n" +
                            indent + "        _parentMessage.Limit(limit + headerLength + dataLength);\n" +
                            indent + "        byte[] tmp = new byte[dataLength];\n" +
                            indent + "        _buffer.GetBytes(limit + headerLength, tmp, 0, dataLength);\n\n" +
                            indent + "        return %6$s.GetString(tmp);\n" +
                            indent + "    }\n",
                    formatPropertyName(propertyName),
                    generateStringNotPresentCondition(token.version(), indent),
                    sizeOfLengthField,
                    generateGet(lengthType, "limit", byteOrderStr),
                    characterEncoding,
                    charset(characterEncoding)));
        }
    }

    private void generateDataEncodeMethods(
            final StringBuilder sb,
            final String propertyName,
            final int sizeOfLengthField,
            final int maxLengthValue,
            final PrimitiveType lengthType,
            final String byteOrderStr,
            final String characterEncoding,
            final String className,
            final String indent) {
        generateDataTypedEncoder(
                sb,
                className,
                propertyName,
                sizeOfLengthField,
                maxLengthValue,
                readOnlyBuffer,
                lengthType,
                byteOrderStr,
                indent);

        generateDataTypedEncoder(
                sb,
                className,
                propertyName,
                sizeOfLengthField,
                maxLengthValue,
                "byte[]",
                lengthType,
                byteOrderStr,
                indent);

        if (null == characterEncoding) {
            return;
        }

        if (characterEncoding.contains("ASCII")) {
            sb.append(String.format(
                    "\n" +
                            indent + "    public %1$s %2$s(string value)\n" +
                            indent + "    {\n" +
                            indent + "        int length = value.Length;\n" +
                            indent + "        if (length > %3$d)\n" +
                            indent + "        {\n" +
                            indent + "            throw new InvalidOperationException" +
                            "(\"length > maxValue for type: \" + length);\n" +
                            indent + "        }\n\n" +
                            indent + "        int headerLength = %4$d;\n" +
                            indent + "        int limit = _parentMessage.Limit();\n" +
                            indent + "        _parentMessage.Limit(limit + headerLength + length);\n" +
                            indent + "        %5$s;\n" +
                            indent + "        _buffer.PutStringWithoutLengthAscii(limit + headerLength, value);\n\n" +
                            indent + "        return this;\n" +
                            indent + "    }\n",
                    className,
                    formatPropertyName(propertyName),
                    maxLengthValue,
                    sizeOfLengthField,
                    generatePut(lengthType, "limit", "length", byteOrderStr)));
        } else {
            sb.append(String.format(
                    "\n" +
                            indent + "    public %1$s %2$s(string value)\n" +
                            indent + "    {\n" +
                            indent + "        byte[] bytes = %7$s.GetBytes(value);\n" +
                            indent + "        int length = bytes.Length;\n" +
                            indent + "        if (length > %4$d)\n" +
                            indent + "        {\n" +
                            indent + "            throw new InvalidOperationException" +
                            "(\"length > maxValue for type: \" + length);\n" +
                            indent + "        }\n\n" +
                            indent + "        int headerLength = %5$d;\n" +
                            indent + "        int limit = _parentMessage.Limit();\n" +
                            indent + "        _parentMessage.Limit(limit + headerLength + length);\n" +
                            indent + "        %6$s;\n" +
                            indent + "        _buffer.PutBytes(limit + headerLength, bytes, 0, length);\n\n" +
                            indent + "        return this;\n" +
                            indent + "    }\n",
                    className,
                    formatPropertyName(propertyName),
                    characterEncoding,
                    maxLengthValue,
                    sizeOfLengthField,
                    generatePut(lengthType, "limit", "length", byteOrderStr),
                    charset(characterEncoding)));
        }
    }

    private void generateDataTypedDecoder(
            final StringBuilder sb,
            final Token token,
            final String propertyName,
            final int sizeOfLengthField,
            final String exchangeType,
            final PrimitiveType lengthType,
            final String byteOrderStr,
            final String indent) {
        sb.append(String.format(
                "\n" +
                        indent + "    public int Get%s(%s dst, int dstOffset, int length)\n" +
                        indent + "    {\n" +
                        "%s" +
                        indent + "        int headerLength = %d;\n" +
                        indent + "        int limit = _parentMessage.Limit();\n" +
                        indent + "        int dataLength = (int)%s;\n" +
                        indent + "        int bytesCopied = Math.Min(length, dataLength);\n" +
                        indent + "        _parentMessage.Limit(limit + headerLength + dataLength);\n" +
                        indent + "        _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);\n\n" +
                        indent + "        return bytesCopied;\n" +
                        indent + "    }\n",
                propertyName,
                exchangeType,
                generateArrayFieldNotPresentCondition(token.version(), indent),
                sizeOfLengthField,
                generateGet(lengthType, "limit", byteOrderStr)));
    }

    private void generateDataTypedEncoder(
            final StringBuilder sb,
            final String className,
            final String propertyName,
            final int sizeOfLengthField,
            final int maxLengthValue,
            final String exchangeType,
            final PrimitiveType lengthType,
            final String byteOrderStr,
            final String indent) {
        sb.append(String.format(
                "\n" +
                        indent + "    public %1$s Put%2$s(%3$s src, int srcOffset, int length)\n" +
                        indent + "    {\n" +
                        indent + "        if (length > %4$d)\n" +
                        indent + "        {\n" +
                        indent + "            throw new InvalidOperationException(\"length > maxValue for type: \" + length);\n" +
                        indent + "        }\n\n" +
                        indent + "        int headerLength = %5$d;\n" +
                        indent + "        int limit = _parentMessage.Limit();\n" +
                        indent + "        _parentMessage.Limit(limit + headerLength + length);\n" +
                        indent + "        %6$s;\n" +
                        indent + "        _buffer.PutBytes(limit + headerLength, src, srcOffset, length);\n\n" +
                        indent + "        return this;\n" +
                        indent + "    }\n",
                className,
                propertyName,
                exchangeType,
                maxLengthValue,
                sizeOfLengthField,
                generatePut(lengthType, "limit", "length", byteOrderStr)));
    }

    private void generateBitSet(final List<Token> tokens) throws IOException {
        final Token token = tokens.get(0);
        final String bitSetName = formatClassName(token.applicableTypeName());
        final String decoderName = decoderName(bitSetName);
        final String encoderName = encoderName(bitSetName);
        final List<Token> messageBody = getMessageBody(tokens);

        try (Writer out = outputManager.createOutput(decoderName)) {
            generateFixedFlyweightHeader(token, decoderName, out, readOnlyBuffer, fqReadOnlyBuffer);
            out.append(generateChoiceDecoders(messageBody));
            out.append(generateChoiceDisplay(messageBody));
            out.append("}\n");
            out.append("}\n");
        }

        try (Writer out = outputManager.createOutput(encoderName)) {
            generateFixedFlyweightHeader(token, encoderName, out, mutableBuffer, fqMutableBuffer);
            out.append(generateChoiceClear(encoderName, token));
            out.append(generateChoiceEncoders(encoderName, messageBody));
            out.append("}\n");
            out.append("}\n");
        }
    }

    private void generateFixedFlyweightHeader(
            final Token token,
            final String typeName,
            final Writer out,
            final String buffer,
            final String fqBuffer) throws IOException {
        out.append(generateFileHeader(namespace(), fqBuffer));
        out.append(generateDeclaration(typeName, ""));
        out.append(generateFixedFlyweightCode(typeName, token.encodedLength(), buffer));
    }

    private void generateCompositeFlyweightHeader(
            final Token token,
            final String typeName,
            final Writer out,
            final String buffer,
            final String fqBuffer,
            final String implementsString,
            final String compositeReturnType) throws IOException {
        out.append(generateFileHeader(namespace(), fqBuffer));
        out.append(generateDeclaration(typeName, implementsString));
        out.append(generateCompositeFlyweightCode(typeName, token.encodedLength(), buffer, compositeReturnType));
    }

    private void generateEnum(final List<Token> tokens) throws IOException {
        final Token enumToken = tokens.get(0);
        final String enumName = formatClassName(tokens.get(0).applicableTypeName());

        try (Writer out = outputManager.createOutput(enumName)) {
            out.append(generateEnumFileHeader(namespace()));
            out.append(generateEnumDeclaration(enumName, csharpTypeName(enumToken.encoding().primitiveType()), false));

            out.append(generateEnumValues(getMessageBody(tokens), enumToken));

            out.append(INDENT + "}\n");
            out.append("}\n");
        }
    }

    private void generateComposite(final List<Token> tokens) throws IOException {
        final Token token = tokens.get(0);
        final String compositeName = formatClassName(token.applicableTypeName());
        final String decoderName = decoderName(compositeName);
        final String encoderName = encoderName(compositeName);

        try (Writer out = outputManager.createOutput(decoderName)) {
            final String implementsString = implementsInterface(GEN_COMPOSITE_DECODER_FLYWEIGHT);
            generateCompositeFlyweightHeader(
                    token, decoderName, out, readOnlyBuffer, fqReadOnlyBuffer, implementsString, "ICompositeDecoderFlyweight");

            for (int i = 1, end = tokens.size() - 1; i < end; ) {
                final Token encodingToken = tokens.get(i);
                final String propertyName = formatPropertyName(encodingToken.name());
                final String typeName = formatClassName(decoderName(encodingToken.applicableTypeName()));

                final StringBuilder sb = new StringBuilder();
                generateEncodingOffsetMethod(sb, propertyName, encodingToken.offset(), BASE_INDENT);
                generateEncodingLengthMethod(sb, propertyName, encodingToken.encodedLength(), BASE_INDENT);

                switch (encodingToken.signal()) {
                    case ENCODING:
                        out.append(sb).append(generatePrimitiveDecoder(
                                true, propertyName, encodingToken, BASE_INDENT));
                        break;

                    case BEGIN_ENUM:
                        out.append(sb).append(generateEnumDecoder(true, encodingToken,
                                propertyName, encodingToken, BASE_INDENT));
                        break;

                    case BEGIN_SET:
                        out.append(sb).append(generateBitSetProperty(
                                true, DECODER, propertyName, encodingToken, BASE_INDENT, typeName));
                        break;

                    case BEGIN_COMPOSITE:
                        out.append(sb).append(generateCompositeProperty(
                                true, DECODER, propertyName, encodingToken, BASE_INDENT, typeName));
                        break;
                }

                i += encodingToken.componentTokenCount();
            }

            out.append(generateCompositeDecoderDisplay(tokens, BASE_INDENT));

            out.append("}\n");
            out.append("}\n");
        }

        try (Writer out = outputManager.createOutput(encoderName)) {
            final String implementsString = implementsInterface(GEN_COMPOSITE_ENCODER_FLYWEIGHT);
            generateCompositeFlyweightHeader(token, encoderName, out, mutableBuffer, fqMutableBuffer, implementsString, "IEncoderFlyweight");

            for (int i = 1, end = tokens.size() - 1; i < end; ) {
                final Token encodingToken = tokens.get(i);
                final String propertyName = formatPropertyName(encodingToken.name());
                final String typeName = formatClassName(encoderName(encodingToken.applicableTypeName()));

                final StringBuilder sb = new StringBuilder();
                generateEncodingOffsetMethod(sb, propertyName, encodingToken.offset(), BASE_INDENT);
                generateEncodingLengthMethod(sb, propertyName, encodingToken.encodedLength(), BASE_INDENT);

                switch (encodingToken.signal()) {
                    case ENCODING:
                        out.append(sb).append(generatePrimitiveEncoder(
                                encoderName, propertyName, encodingToken, BASE_INDENT));
                        break;

                    case BEGIN_ENUM:
                        out.append(sb).append(generateEnumEncoder(
                                encoderName, propertyName, encodingToken, BASE_INDENT));
                        break;

                    case BEGIN_SET:
                        out.append(sb).append(generateBitSetProperty(
                                true, ENCODER, propertyName, encodingToken, BASE_INDENT, typeName));
                        break;

                    case BEGIN_COMPOSITE:
                        out.append(sb).append(generateCompositeProperty(
                                true, ENCODER, propertyName, encodingToken, BASE_INDENT, typeName));
                        break;
                }

                i += encodingToken.componentTokenCount();
            }

            out.append(generateCompositeEncoderDisplay(decoderName, BASE_INDENT));
            out.append("}\n");
            out.append("}\n");
        }
    }

    private CharSequence generateChoiceClear(final String bitSetClassName, final Token token) {
        final StringBuilder sb = new StringBuilder();

        final Encoding encoding = token.encoding();
        final String literalValue = generateLiteral(encoding.primitiveType(), "0");
        final String byteOrderStr = byteOrderString(encoding);

        sb.append(String.format(
                "\n" +
                        "    public %s Clear()\n" +
                        "    {\n" +
                        "        %s;\n" +
                        "        return this;\n" +
                        "    }\n",
                bitSetClassName,
                generatePut(encoding.primitiveType(), "_offset", literalValue, byteOrderStr)));

        return sb;
    }

    private CharSequence generateChoiceDecoders(final List<Token> tokens) {
        final StringBuilder sb = new StringBuilder();

        for (final Token token : tokens)
        {
            if (token.signal() == Signal.CHOICE)
            {
                final String choiceName = formatPropertyName(token.name());
                final Encoding encoding = token.encoding();
                final String choiceBitIndex = encoding.constValue().toString();
                final String byteOrderStr = byteOrderString(encoding);
                final PrimitiveType primitiveType = encoding.primitiveType();
                final String argType = bitsetArgType(primitiveType);

                sb.append(String.format("\n" +
                                "    public bool %1$s()\n" +
                                "    {\n" +
                                "        return %2$s;\n" +
                                "    }\n\n" +
                                "    public static bool %1$s(%3$s value)\n" +
                                "    {\n" +
                                "        return %4$s;\n" +
                                "    }\n",
                        choiceName,
                        generateChoiceGet(primitiveType, choiceBitIndex, byteOrderStr),
                        argType,
                        generateStaticChoiceGet(primitiveType, choiceBitIndex)));
            }
        }

        return sb;
    }

    private CharSequence generateChoiceEncoders(final String bitSetClassName, final List<Token> tokens) {
        final StringBuilder sb = new StringBuilder();

        for (final Token token : tokens)
        {
            if (token.signal() == Signal.CHOICE)
            {
                final String choiceName = formatPropertyName(token.name());
                final Encoding encoding = token.encoding();
                final String choiceBitIndex = encoding.constValue().toString();
                final String byteOrderStr = byteOrderString(encoding);
                final PrimitiveType primitiveType = encoding.primitiveType();
                final String argType = bitsetArgType(primitiveType);

                sb.append(String.format(
                        "\n" +
                                "    public %1$s %2$s(bool value)\n" +
                                "    {\n" +
                                "%3$s\n" +
                                "        return this;\n" +
                                "    }\n\n" +
                                "    public static %4$s %2$s(%4$s bits, bool value)\n" +
                                "    {\n" +
                                "%5$s" +
                                "    }\n",
                        bitSetClassName,
                        choiceName,
                        generateChoicePut(encoding.primitiveType(), choiceBitIndex, byteOrderStr),
                        argType,
                        generateStaticChoicePut(encoding.primitiveType(), choiceBitIndex)));
            }
        }

        return sb;
    }

    private String bitsetArgType(final PrimitiveType primitiveType) {
        switch (primitiveType) {
            case UINT8:
                return "byte";

            case UINT16:
                return "ushort";

            case UINT32:
                return "uint";

            case UINT64:
                return "ulong";

            default:
                throw new IllegalStateException("Invalid type: " + primitiveType);
        }
    }

    private CharSequence generateEnumValues(final List<Token> tokens, final Token encodingToken) {
        final StringBuilder sb = new StringBuilder();
        final Encoding encoding = encodingToken.encoding();

        for (final Token token : tokens) {
            sb.append(INDENT).append(INDENT).append(token.name()).append(" = ")
                    .append(token.encoding().constValue()).append(",\n");
        }

        final PrimitiveValue nullVal = encoding.applicableNullValue();

        sb.append(INDENT).append(INDENT).append("NULL_VALUE = ").append(nullVal).append("\n");

        return sb;
    }

    private CharSequence generateEnumBody(final Token token, final String enumName) {
        final String javaEncodingType = primitiveTypeName(token);

        return String.format(
                "    private %1$s value;\n\n" +
                        "    %2$s(%1$s value)\n" +
                        "    {\n" +
                        "        this._value = value;\n" +
                        "    }\n\n" +
                        "    public %1$s value()\n" +
                        "    {\n" +
                        "        return value;\n" +
                        "    }\n\n",
                javaEncodingType,
                enumName);
    }

    private CharSequence generateEnumLookupMethod(final List<Token> tokens, final String enumName) {
        final StringBuilder sb = new StringBuilder();

        final PrimitiveType primitiveType = tokens.get(0).encoding().primitiveType();
        sb.append(String.format(
                "    public static %s Get(%s value)\n" +
                        "    {\n" +
                        "        switch (value)\n" +
                        "        {\n",
                enumName,
                csharpTypeName(primitiveType)));

        for (final Token token : tokens) {
            sb.append(String.format(
                    "            case %s: return %s;\n",
                    token.encoding().constValue().toString(),
                    token.name()));
        }

        final String handleUnknownLogic = shouldDecodeUnknownEnumValues ?
                INDENT + INDENT + "return SBE_UNKNOWN;\n" :
                INDENT + INDENT + "throw new ArgumentException(\"Unknown value: \" + value);\n";

        sb.append(String.format(
                "        }\n\n" +
                        "        if (%s == value)\n" +
                        "        {\n" +
                        "            return NULL_VAL;\n" +
                        "        }\n\n" +
                        "%s" +
                        "    }\n",
                generateLiteral(primitiveType, tokens.get(0).encoding().applicableNullValue().toString()),
                handleUnknownLogic));

        return sb;
    }

    private CharSequence interfaceImportLine() {
        if (!shouldGenerateInterfaces) {
            return "\n";
        }

        return String.format("using %s;\n\n", CSHARP_INTERFACE_PACKAGE);
    }

    private CharSequence generateFileHeader(final String packageName, final String fqBuffer) {
        return String.format(
                "/* Generated SBE (Simple Binary Encoding) message codec */\n" +
                        "using System;\n" +
                        "using System.Text;\n" +
                        "using %s;\n" +
                        "%s" +
                        "namespace %s {\n",
                fqBuffer,
                interfaceImportLine(),
                packageName);
    }

    private CharSequence generateMainHeader(final String packageName) {
        if (fqMutableBuffer.equals(fqReadOnlyBuffer)) {
            return String.format(
                    "/* Generated SBE (Simple Binary Encoding) message codec */\n" +
                            "using System;\n" +
                            "using System.Text;\n" +
                            "using System.Collections.Generic;\n" +
                            "using System.Collections;\n" +
                            "using %s;\n" +
                            "%s\n" +
                            "namespace %s {\n\n",
                    fqMutableBuffer,
                    interfaceImportLine(),
                    packageName);
        } else {
            return String.format(
                    "/* Generated SBE (Simple Binary Encoding) message codec */\n" +
                            "using System;\n" +
                            "using System.Text;\n" +
                            "using System.Collections.Generic;\n" +
                            "using %s;\n" +
                            "using %s;\n" +
                            "%s\n" +
                            "namespace %s {\n\n",
                    fqMutableBuffer,
                    fqReadOnlyBuffer,
                    interfaceImportLine(),
                    packageName
            );
        }
    }

    private void generateAnnotations(
            final String indent,
            final String className,
            final List<Token> tokens,
            final Appendable out,
            final int index,
            final Function<String, String> nameMapping) throws IOException {
        if (shouldGenerateGroupOrderAnnotation) {
            final List<String> groupClassNames = new ArrayList<>();
            int level = 0;
            int i = index;

            for (int size = tokens.size(); i < size; i++) {
                if (tokens.get(index).signal() == Signal.BEGIN_GROUP) {
                    if (++level == 1) {
                        final Token groupToken = tokens.get(index);
                        final String groupName = groupToken.name();
                        groupClassNames.add(formatClassName(nameMapping.apply(groupName)));
                    }
                } else if (tokens.get(index).signal() == Signal.END_GROUP && --level < 0) {
                    break;
                }
            }

            if (!groupClassNames.isEmpty()) {
                out.append(indent).append("@uk.co.real_logic.sbe.codec.java.GroupOrder({");
                i = 0;
                for (final String name : groupClassNames) {
                    out.append(className).append('.').append(name).append(".class");
                    if (++i < groupClassNames.size()) {
                        out.append(", ");
                    }
                }

                out.append("})\n");
            }
        }
    }

    private void generateMetaAttributeEnum() throws IOException {
        try (Writer out = outputManager.createOutput(META_ATTRIBUTE_ENUM)) {
            out.append(String.format(
                    "/* Generated SBE (Simple Binary Encoding) message codec */\n" +
                            "namespace %s {\n\n" +
                            "public enum MetaAttribute\n" +
                            "{\n" +
                            "    EPOCH,\n" +
                            "    TIME_UNIT,\n" +
                            "    SEMANTIC_TYPE,\n" +
                            "    PRESENCE\n" +
                            "}\n" +
                            "}\n",
                    namespace()));
        }
    }

    private CharSequence generatePrimitiveDecoder(
            final boolean inComposite, final String propertyName, final Token token, final String indent) {
        final StringBuilder sb = new StringBuilder();

        sb.append(generatePrimitiveFieldMetaData(propertyName, token, indent));

        if (token.isConstantEncoding()) {
            sb.append(generateConstPropertyMethods(propertyName, token, indent));
        } else {
            sb.append(generatePrimitivePropertyDecodeMethods(inComposite, propertyName, token, indent));
        }

        return sb;
    }

    private CharSequence generatePrimitiveEncoder(
            final String containingClassName, final String propertyName, final Token token, final String indent) {
        final StringBuilder sb = new StringBuilder();

        sb.append(generatePrimitiveFieldMetaData(propertyName, token, indent));

        if (!token.isConstantEncoding()) {
            sb.append(generatePrimitivePropertyEncodeMethods(containingClassName, propertyName, token, indent));
        }

        return sb;
    }

    private CharSequence generatePrimitivePropertyDecodeMethods(
            final boolean inComposite, final String propertyName, final Token token, final String indent) {
        return token.matchOnLength(
                () -> generatePrimitivePropertyDecode(inComposite, propertyName, token, indent),
                () -> generatePrimitiveArrayPropertyDecode(inComposite, propertyName, token, indent));
    }

    private CharSequence generatePrimitivePropertyEncodeMethods(
            final String containingClassName, final String propertyName, final Token token, final String indent) {
        return token.matchOnLength(
                () -> generatePrimitivePropertyEncode(containingClassName, propertyName, token, indent),
                () -> generatePrimitiveArrayPropertyEncode(containingClassName, propertyName, token, indent));
    }

    private CharSequence generatePrimitiveFieldMetaData(
            final String propertyName, final Token token, final String indent) {
        final StringBuilder sb = new StringBuilder();

        final PrimitiveType primitiveType = token.encoding().primitiveType();
        final String javaTypeName = csharpTypeName(primitiveType);

        sb.append(String.format(
                "\n" +
                        indent + "    public static %s %sNullValue()\n" +
                        indent + "    {\n" +
                        indent + "        return %s;\n" +
                        indent + "    }\n",
                javaTypeName,
                propertyName,
                generateLiteral(primitiveType, token.encoding().applicableNullValue().toString())));

        sb.append(String.format(
                "\n" +
                        indent + "    public static %s %sMinValue()\n" +
                        indent + "    {\n" +
                        indent + "        return %s;\n" +
                        indent + "    }\n",
                javaTypeName,
                propertyName,
                generateLiteral(primitiveType, token.encoding().applicableMinValue().toString())));

        sb.append(String.format(
                "\n" +
                        indent + "    public static %s %sMaxValue()\n" +
                        indent + "    {\n" +
                        indent + "        return %s;\n" +
                        indent + "    }\n",
                javaTypeName,
                propertyName,
                generateLiteral(primitiveType, token.encoding().applicableMaxValue().toString())));

        return sb;
    }

    private CharSequence generatePrimitivePropertyDecode(
            final boolean inComposite, final String propertyName, final Token token, final String indent) {
        final Encoding encoding = token.encoding();
        final String javaTypeName = csharpTypeName(encoding.primitiveType());

        final int offset = token.offset();
        final String byteOrderStr = byteOrderString(encoding);

        return String.format(
                "\n" +
                        indent + "    public %s %s()\n" +
                        indent + "    {\n" +
                        "%s" +
                        indent + "        return %s;\n" +
                        indent + "    }\n\n",
                javaTypeName,
                propertyName,
                generateFieldNotPresentCondition(inComposite, token.version(), encoding, indent),
                generateGet(encoding.primitiveType(), "_offset + " + offset, byteOrderStr));
    }

    private CharSequence generatePrimitivePropertyEncode(
            final String containingClassName, final String propertyName, final Token token, final String indent) {
        final Encoding encoding = token.encoding();
        final String javaTypeName = csharpTypeName(encoding.primitiveType());
        final int offset = token.offset();
        final String byteOrderStr = byteOrderString(encoding);

        return String.format(
                "\n" +
                        indent + "    public %s %s(%s value)\n" +
                        indent + "    {\n" +
                        indent + "        %s;\n" +
                        indent + "        return this;\n" +
                        indent + "    }\n\n",
                formatClassName(containingClassName),
                propertyName,
                javaTypeName,
                generatePut(encoding.primitiveType(), "_offset + " + offset, "value", byteOrderStr));
    }

    private CharSequence generateFieldNotPresentCondition(
            final boolean inComposite, final int sinceVersion, final Encoding encoding, final String indent) {
        if (inComposite || 0 == sinceVersion) {
            return "";
        }

        return String.format(
                indent + "        if (_parentMessage._actingVersion < %d)\n" +
                        indent + "        {\n" +
                        indent + "            return %s;\n" +
                        indent + "        }\n\n",
                sinceVersion,
                generateLiteral(encoding.primitiveType(), encoding.applicableNullValue().toString()));
    }

    private CharSequence generatePrimitiveArrayPropertyDecode(
            final boolean inComposite, final String propertyName, final Token token, final String indent) {
        final Encoding encoding = token.encoding();
        final String javaTypeName = csharpTypeName(encoding.primitiveType());
        final int offset = token.offset();
        final String byteOrderStr = byteOrderString(encoding);
        final int fieldLength = token.arrayLength();
        final int typeSize = sizeOfPrimitive(encoding);

        final StringBuilder sb = new StringBuilder();

        generateArrayLengthMethod(propertyName, indent, fieldLength, sb);

        sb.append(String.format(
                indent + "    public %s %s(int index)\n" +
                        indent + "    {\n" +
                        indent + "        if (index < 0 || index >= %d)\n" +
                        indent + "        {\n" +
                        indent + "            throw new IndexOutOfRangeException(\"index out of range: index=\" + index);\n" +
                        indent + "        }\n\n" +
                        "%s" +
                        indent + "        int pos = this._offset + %d + (index * %d);\n\n" +
                        indent + "        return %s;\n" +
                        indent + "    }\n\n",
                javaTypeName,
                propertyName,
                fieldLength,
                generateFieldNotPresentCondition(inComposite, token.version(), encoding, indent),
                offset,
                typeSize,
                generateGet(encoding.primitiveType(), "pos", byteOrderStr)));

        if (encoding.primitiveType() == PrimitiveType.CHAR) {
            generateCharacterEncodingMethod(sb, propertyName, encoding.characterEncoding(), indent);

            sb.append(String.format(
                    "\n" +
                            indent + "    public int Get%s(byte[] dst, int dstOffset)\n" +
                            indent + "    {\n" +
                            indent + "        int length = %d;\n" +
                            indent + "        if (dstOffset < 0 || dstOffset > (dst.Length - length))\n" +
                            indent + "        {\n" +
                            indent + "            throw new IndexOutOfRangeException(" +
                            "\"Copy will go out of range: offset=\" + dstOffset);\n" +
                            indent + "        }\n\n" +
                            "%s" +
                            indent + "        _buffer.GetBytes(this._offset + %d, dst, dstOffset, length);\n\n" +
                            indent + "        return length;\n" +
                            indent + "    }\n",
                    toUpperFirstChar(propertyName),
                    fieldLength,
                    generateArrayFieldNotPresentCondition(token.version(), indent),
                    offset));

            sb.append(String.format(
                    "\n" +
                            indent + "    public string %s()\n" +
                            indent + "    {\n" +
                            "%s" +
                            indent + "        byte[] dst = new byte[%d];\n" +
                            indent + "        _buffer.GetBytes(this._offset + %d, dst, 0, %d);\n\n" +
                            indent + "        int end = 0;\n" +
                            indent + "        for (; end < %d && dst[end] != 0; ++end);\n\n" +
                            indent + "        return %s.GetString(dst, 0, end);\n" +
                            indent + "    }\n\n",
                    formatPropertyName(propertyName),
                    generateStringNotPresentCondition(token.version(), indent),
                    fieldLength, offset,
                    fieldLength, fieldLength,
                    charset(encoding.characterEncoding())));
        }

        return sb;
    }

    private String byteOrderString(final Encoding encoding) {
        return sizeOfPrimitive(encoding) == 1 ? "" : ", " + getCSharpByteOrder(encoding.byteOrder());
    }

    private CharSequence generatePrimitiveArrayPropertyEncode(
            final String containingClassName, final String propertyName, final Token token, final String indent) {
        final Encoding encoding = token.encoding();
        final String javaTypeName = csharpTypeName(encoding.primitiveType());
        final int offset = token.offset();
        final String byteOrderStr = byteOrderString(encoding);
        final int fieldLength = token.arrayLength();
        final int typeSize = sizeOfPrimitive(encoding);

        final StringBuilder sb = new StringBuilder();

        generateArrayLengthMethod(propertyName, indent, fieldLength, sb);

        sb.append(String.format(
                indent + "    public void %s(int index, %s value)\n" +
                        indent + "    {\n" +
                        indent + "        if (index < 0 || index >= %d)\n" +
                        indent + "        {\n" +
                        indent + "            throw new IndexOutOfRangeException(\"index out of range: index=\" + index);\n" +
                        indent + "        }\n\n" +
                        indent + "        int pos = this._offset + %d + (index * %d);\n" +
                        indent + "        %s;\n" +
                        indent + "    }\n",
                propertyName,
                javaTypeName,
                fieldLength,
                offset,
                typeSize,
                generatePut(encoding.primitiveType(), "pos", "value", byteOrderStr)));

        if (encoding.primitiveType() == PrimitiveType.CHAR) {
            generateCharArrayEncodeMethods(
                    containingClassName, propertyName, indent, encoding, offset, fieldLength, sb);
        }

        return sb;
    }

    private void generateCharArrayEncodeMethods(
            final String containingClassName,
            final String propertyName,
            final String indent,
            final Encoding encoding,
            final int offset,
            final int fieldLength,
            final StringBuilder sb) {
        generateCharacterEncodingMethod(sb, propertyName, encoding.characterEncoding(), indent);

        sb.append(String.format(
                "\n" +
                        indent + "    public %s Put%s(byte[] src, int srcOffset)\n" +
                        indent + "    {\n" +
                        indent + "        int length = %d;\n" +
                        indent + "        if (srcOffset < 0 || srcOffset > (src.Length - length))\n" +
                        indent + "        {\n" +
                        indent + "            throw new IndexOutOfRangeException(" +
                        "\"Copy will go out of range: offset=\" + srcOffset);\n" +
                        indent + "        }\n\n" +
                        indent + "        _buffer.PutBytes(this._offset + %d, src, srcOffset, length);\n\n" +
                        indent + "        return this;\n" +
                        indent + "    }\n",
                formatClassName(containingClassName),
                toUpperFirstChar(propertyName),
                fieldLength,
                offset));

        if (encoding.characterEncoding().contains("ASCII")) {
            sb.append(String.format(
                    "\n" +
                            indent + "    public %1$s %2$s(string src)\n" +
                            indent + "    {\n" +
                            indent + "        int length = %3$d;\n" +
                            indent + "        int srcLength = src.Length;\n" +
                            indent + "        if (srcLength > length)\n" +
                            indent + "        {\n" +
                            indent + "            throw new IndexOutOfRangeException(" +
                            "\"String too large for copy: byte length=\" + srcLength);\n" +
                            indent + "        }\n\n" +
                            indent + "        _buffer.PutStringWithoutLengthAscii(this._offset + %4$d, src);\n\n" +
                            indent + "        for (int start = srcLength; start < length; ++start)\n" +
                            indent + "        {\n" +
                            indent + "            _buffer.PutByte(this._offset + %4$d + start, (byte)0);\n" +
                            indent + "        }\n\n" +
                            indent + "        return this;\n" +
                            indent + "    }\n",
                    formatClassName(containingClassName),
                    propertyName,
                    fieldLength,
                    offset));
        } else {
            sb.append(String.format(
                    "\n" +
                            indent + "    public %s %s(string src)\n" +
                            indent + "    {\n" +
                            indent + "        int length = %d;\n" +
                            indent + "        byte[] bytes = src.GetBytes(%s);\n" +
                            indent + "        if (bytes.Length > length)\n" +
                            indent + "        {\n" +
                            indent + "            throw new IndexOutOfRangeException(" +
                            "\"String too large for copy: byte length=\" + bytes.length);\n" +
                            indent + "        }\n\n" +
                            indent + "        buffer.PutBytes(this._offset + %d, bytes, 0, bytes.length);\n\n" +
                            indent + "        for (int start = bytes.length; start < length; ++start)\n" +
                            indent + "        {\n" +
                            indent + "            _buffer.PutByte(this._offset + %d + start, (byte)0);\n" +
                            indent + "        }\n\n" +
                            indent + "        return this;\n" +
                            indent + "    }\n",
                    formatClassName(containingClassName),
                    propertyName,
                    fieldLength,
                    charset(encoding.characterEncoding()),
                    offset,
                    offset));
        }
    }

    private CharSequence generateConstPropertyMethods(
            final String propertyName, final Token token, final String indent) {
        final Encoding encoding = token.encoding();
        if (encoding.primitiveType() != PrimitiveType.CHAR) {
            return String.format(
                    "\n" +
                            indent + "    public %s %s()\n" +
                            indent + "    {\n" +
                            indent + "        return %s;\n" +
                            indent + "    }\n",
                    csharpTypeName(encoding.primitiveType()),
                    propertyName,
                    generateLiteral(encoding.primitiveType(), encoding.constValue().toString()));
        }

        final StringBuilder sb = new StringBuilder();

        final String javaTypeName = csharpTypeName(encoding.primitiveType());
        final byte[] constBytes = encoding.constValue().byteArrayValue(encoding.primitiveType());
        final CharSequence values = generateByteLiteralList(
                encoding.constValue().byteArrayValue(encoding.primitiveType()));

        sb.append(String.format(
                "\n" +
                        indent + "    private static byte[] %s_VALUE = { %s };\n",
                propertyName.toUpperCase(),
                values));

        generateArrayLengthMethod(propertyName, indent, constBytes.length, sb);

        sb.append(String.format(
                indent + "    public %s %s(int index)\n" +
                        indent + "    {\n" +
                        indent + "        return %s_VALUE[index];\n" +
                        indent + "    }\n\n",
                javaTypeName,
                propertyName,
                propertyName.toUpperCase()));

        sb.append(String.format(
                indent + "    public int Get%s(byte[] dst, int offset, int length)\n" +
                        indent + "    {\n" +
                        indent + "        int bytesCopied = Math.Min(length, %d);\n" +
                        indent + "        Array.Copy(%s_VALUE, 0, dst, offset, bytesCopied);\n\n" +
                        indent + "        return bytesCopied;\n" +
                        indent + "    }\n",
                toUpperFirstChar(propertyName),
                constBytes.length,
                propertyName.toUpperCase()));

        if (constBytes.length > 1) {
            sb.append(String.format(
                    "\n" +
                            indent + "    public string %s()\n" +
                            indent + "    {\n" +
                            indent + "        return \"%s\";\n" +
                            indent + "    }\n\n",
                    propertyName,
                    encoding.constValue()));
        } else {
            sb.append(String.format(
                    "\n" +
                            indent + "    public byte %s()\n" +
                            indent + "    {\n" +
                            indent + "        return (byte)%s;\n" +
                            indent + "    }\n\n",
                    propertyName,
                    encoding.constValue()));
        }

        return sb;
    }

    private CharSequence generateDecoderFlyweightCode(final String className, final Token token) {
        final String wrapMethod = String.format(
                "    public IMessageDecoderFlyweight Wrap(\n" +
                        "        %2$s buffer, int offset, int actingBlockLength, int actingVersion)\n" +
                        "    {\n" +
                        "        this._buffer = buffer;\n" +
                        "        this._offset = offset;\n" +
                        "        this._actingBlockLength = actingBlockLength;\n" +
                        "        this._actingVersion = actingVersion;\n" +
                        "        Limit(offset + actingBlockLength);\n\n" +
                        "        return this;\n" +
                        "    }\n\n",
                className,
                readOnlyBuffer);

        return generateFlyweightCode(DECODER, className, token, wrapMethod, readOnlyBuffer);
    }

    private CharSequence generateFlyweightCode(
            final CodecType codecType,
            final String className,
            final Token token,
            final String wrapMethod,
            final String bufferImplementation) {
        final HeaderStructure headerStructure = ir.headerStructure();
        final String blockLengthType = csharpTypeName(headerStructure.blockLengthType());
        final String templateIdType = csharpTypeName(headerStructure.templateIdType());
        final String schemaIdType = csharpTypeName(headerStructure.schemaIdType());
        final String schemaVersionType = csharpTypeName(headerStructure.schemaVersionType());
        final String semanticType = token.encoding().semanticType() == null ? "" : token.encoding().semanticType();
        final String actingFields = codecType == ENCODER ?
                "" :
                "    protected int _actingBlockLength;\n" +
                        "    protected int _actingVersion;\n";

        return String.format(
                "    public const %1$s BLOCK_LENGTH = %2$s;\n" +
                        "    public const %3$s TEMPLATE_ID = %4$s;\n" +
                        "    public const %5$s SCHEMA_ID = %6$s;\n" +
                        "    public const %7$s SCHEMA_VERSION = %8$s;\n\n" +
                        "    private %9$s _parentMessage;\n" +
                        "    private %11$s _buffer;\n" +
                        "    protected int _offset;\n" +
                        "    protected int _limit;\n" +
                        "%13$s" +
                        "\n" +
                        "    public %9$s()\n" +
                        "    {\n" +
                        "        _parentMessage = this;\n" +
                        "    }\n\n" +
                        "    public int SbeBlockLength()\n" +
                        "    {\n" +
                        "        return BLOCK_LENGTH;\n" +
                        "    }\n\n" +
                        "    public int SbeTemplateId()\n" +
                        "    {\n" +
                        "        return TEMPLATE_ID;\n" +
                        "    }\n\n" +
                        "    public int SbeSchemaId()\n" +
                        "    {\n" +
                        "        return SCHEMA_ID;\n" +
                        "    }\n\n" +
                        "    public int SbeSchemaVersion()\n" +
                        "    {\n" +
                        "        return SCHEMA_VERSION;\n" +
                        "    }\n\n" +
                        "    public string SbeSemanticType()\n" +
                        "    {\n" +
                        "        return \"%10$s\";\n" +
                        "    }\n\n" +
                        "    public %11$s Buffer()\n" +
                        "    {\n" +
                        "        return _buffer;\n" +
                        "    }\n\n" +
                        "    public int Offset()\n" +
                        "    {\n" +
                        "        return _offset;\n" +
                        "    }\n\n" +
                        "%12$s" +
                        "    public int EncodedLength()\n" +
                        "    {\n" +
                        "        return _limit - _offset;\n" +
                        "    }\n\n" +
                        "    public int Limit()\n" +
                        "    {\n" +
                        "        return _limit;\n" +
                        "    }\n\n" +
                        "    public void Limit(int limit)\n" +
                        "    {\n" +
                        "        this._limit = limit;\n" +
                        "    }\n",
                blockLengthType,
                generateLiteral(headerStructure.blockLengthType(), Integer.toString(token.encodedLength())),
                templateIdType,
                generateLiteral(headerStructure.templateIdType(), Integer.toString(token.id())),
                schemaIdType,
                generateLiteral(headerStructure.schemaIdType(), Integer.toString(ir.id())),
                schemaVersionType,
                generateLiteral(headerStructure.schemaVersionType(), Integer.toString(ir.version())),
                className,
                semanticType,
                bufferImplementation,
                wrapMethod,
                actingFields);
    }

    private CharSequence generateEncoderFlyweightCode(final String className, final Token token) {
        final String wrapMethod = String.format(
                "    public %1$s Wrap(%2$s buffer, int offset)\n" +
                        "    {\n" +
                        "        this._buffer = buffer;\n" +
                        "        this._offset = offset;\n" +
                        "        Limit(offset + BLOCK_LENGTH);\n\n" +
                        "        return this;\n" +
                        "    }\n\n",
                className,
                mutableBuffer);

        final String wrapAndApplyHeaderMethod = String.format(
                "    public %1$s WrapAndApplyHeader(\n" +
                        "        %2$s buffer, int offset, %3$s headerEncoder)\n" +
                        "    {\n" +
                        "        headerEncoder\n" +
                        "            .Wrap(buffer, offset);\n" +
                        "        headerEncoder\n" +
                        "            .BlockLength(BLOCK_LENGTH)\n" +
                        "            .TemplateId(TEMPLATE_ID)\n" +
                        "            .SchemaId(SCHEMA_ID)\n" +
                        "            .Version(SCHEMA_VERSION);\n\n" +
                        "        return Wrap(buffer, offset + %3$s.ENCODED_LENGTH);\n" +
                        "    }\n\n",
                className,
                mutableBuffer,
                formatClassName(ir.headerStructure().tokens().get(0).applicableTypeName() + "Encoder"));

        return generateFlyweightCode(
                ENCODER, className, token, wrapMethod + wrapAndApplyHeaderMethod, mutableBuffer);
    }

    private CharSequence generateEncoderFields(
            final String containingClassName, final List<Token> tokens, final String indent) {
        final StringBuilder sb = new StringBuilder();

        Generators.forEachField(
                tokens,
                (fieldToken, typeToken) ->
                {
                    final String propertyName = formatPropertyName(fieldToken.name());
                    final String typeName = formatClassName(encoderName(typeToken.name()));

                    generateEncodingOffsetMethod(sb, fieldToken.name(), fieldToken.offset(), indent);
                    generateEncodingLengthMethod(sb, fieldToken.name(), typeToken.encodedLength(), indent);

                    switch (typeToken.signal()) {
                        case ENCODING:
                            sb.append(generatePrimitiveEncoder(containingClassName, propertyName, typeToken, indent));
                            break;

                        case BEGIN_ENUM:
                            sb.append(generateEnumEncoder(containingClassName, propertyName, typeToken, indent));
                            break;

                        case BEGIN_SET:
                            sb.append(generateBitSetProperty(false, ENCODER, propertyName, typeToken, indent, typeName));
                            break;

                        case BEGIN_COMPOSITE:
                            sb.append(generateCompositeProperty(false, ENCODER, propertyName, typeToken, indent, typeName));
                            break;
                    }
                });

        return sb;
    }

    private CharSequence generateDecoderFields(final List<Token> tokens, final String indent) {
        final StringBuilder sb = new StringBuilder();

        Generators.forEachField(
                tokens,
                (fieldToken, typeToken) ->
                {
                    final String propertyName = formatPropertyName(fieldToken.name());
                    final String typeName = decoderName(formatClassName(typeToken.name()));

                    generateFieldIdMethod(sb, fieldToken, indent);
                    generateFieldSinceVersionMethod(sb, fieldToken, indent);
                    generateEncodingOffsetMethod(sb, fieldToken.name(), fieldToken.offset(), indent);
                    generateEncodingLengthMethod(sb, fieldToken.name(), typeToken.encodedLength(), indent);
                    generateFieldMetaAttributeMethod(sb, fieldToken, indent);

                    switch (typeToken.signal()) {
                        case ENCODING:
                            sb.append(generatePrimitiveDecoder(false, propertyName, typeToken, indent));
                            break;

                        case BEGIN_ENUM:
                            sb.append(generateEnumDecoder(false, fieldToken, propertyName, typeToken, indent));
                            break;

                        case BEGIN_SET:
                            sb.append(generateBitSetProperty(false, DECODER, propertyName, typeToken, indent, typeName));
                            break;

                        case BEGIN_COMPOSITE:
                            sb.append(generateCompositeProperty(false, DECODER, propertyName, typeToken, indent, typeName));
                            break;
                    }
                });

        return sb;
    }

    private String namespace() {
        // Preserve '.' but upcase between them
        String[] tokens = ir.applicableNamespace().split("\\.");
        final StringBuilder sb = new StringBuilder();
        for (final String t : tokens) {
            sb.append(toUpperFirstChar(t)).append(".");
        }
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }

        // Remove '_' but upcase between them
        tokens = sb.toString().split("-");
        sb.setLength(0);

        for (final String t : tokens) {
            sb.append(toUpperFirstChar(t));
        }

        return sb.toString();
    }

    private CharSequence generateEnumDecoder(
            final boolean inComposite,
            final Token signalToken,
            final String propertyName,
            final Token token,
            final String indent) {
        final String enumName = formatClassName(token.applicableTypeName());
        final String typePrefix = toUpperFirstChar(token.encoding().primitiveType().primitiveName());
        final String byteOrderStr = byteOrderString(token.encoding());

        if (token.isConstantEncoding()) {
            return String.format(
                    "\n" +
                            indent + "    public %s %s()\n" +
                            indent + "    {\n" +
                            indent + "        return %s.%s;\n" +
                            indent + "    }\n\n",
                    enumName,
                    propertyName,
                    namespace(),
                    signalToken.encoding().constValue().toString());
        } else {
            return String.format(
                    "\n" +
                            indent + "    public %1$s %2$s()\n" +
                            indent + "    {\n" +
                            "%3$s" +
                            indent + "        return (%4$s)%5$s;\n" +
                            indent + "    }\n\n",
                    enumName,
                    propertyName,
                    generateEnumFieldNotPresentCondition(token.version(), namespace(), enumName, indent),
                    enumName,
                    generateGet(token.encoding().primitiveType(), "_offset + " + token.offset(), byteOrderStr)
            );
        }
    }

    private CharSequence generateEnumFieldNotPresentCondition(
            final int sinceVersion,
            final String namespace,
            final String enumName,
            final String indent
    ) {
        if (0 == sinceVersion) {
            return "";
        }

        return String.format(
                indent + INDENT + INDENT + "if (_actingVersion < %d) return %s.%s.NULL_VALUE;\n\n",
                sinceVersion,
                namespace,
                enumName);
    }

    private CharSequence generateEnumEncoder(
            final String containingClassName, final String propertyName, final Token token, final String indent) {
        if (token.isConstantEncoding()) {
            return "";
        }

        final String enumName = formatClassName(token.applicableTypeName());
        final Encoding encoding = token.encoding();
        final int offset = token.offset();

        return String.format(
                "\n" +
                        indent + "    public %s %s(%s value)\n" +
                        indent + "    {\n" +
                        indent + "        %s;\n" +
                        indent + "        return this;\n" +
                        indent + "    }\n",
                formatClassName(containingClassName),
                propertyName,
                enumName,
                generatePut(encoding.primitiveType(), "_offset + " + offset,
                        "(" + csharpTypeName(encoding.primitiveType()) + ")value", byteOrderString(encoding)));
    }

    private CharSequence generateBitSetProperty(
            final boolean inComposite,
            final CodecType codecType,
            final String propertyName,
            final Token token,
            final String indent,
            final String bitSetName) {
        final StringBuilder sb = new StringBuilder();

        sb.append(String.format(
                "\n" +
                        indent + "    private %s _%s = new %s();\n",
                bitSetName,
                propertyName,
                bitSetName));

        sb.append(String.format(
                "\n" +
                        indent + "    public %s %s()\n" +
                        indent + "    {\n" +
                        "%s" +
                        indent + "        _%s.Wrap(_buffer, _offset + %d);\n" +
                        indent + "        return _%s;\n" +
                        indent + "    }\n",
                bitSetName,
                propertyName,
                generatePropertyNotPresentCondition(inComposite, codecType, token.version(), indent),
                propertyName,
                token.offset(),
                propertyName));

        return sb;
    }

    private CharSequence generateCompositeProperty(
            final boolean inComposite,
            final CodecType codecType,
            final String propertyName,
            final Token token,
            final String indent,
            final String compositeName) {
        final StringBuilder sb = new StringBuilder();

        sb.append(String.format(
                "\n" +
                        indent + "    private %s _%s = new %s();\n",
                compositeName,
                propertyName,
                compositeName));

        sb.append(String.format(
                "\n" +
                        indent + "    public %s %s()\n" +
                        indent + "    {\n" +
                        "%s" +
                        indent + "        _%s.Wrap(_buffer, _offset + %d);\n" +
                        indent + "        return _%s;\n" +
                        indent + "    }\n",
                compositeName,
                propertyName,
                generatePropertyNotPresentCondition(inComposite, codecType, token.version(), indent),
                propertyName,
                token.offset(),
                propertyName));

        return sb;
    }

    private String generateGet(final PrimitiveType type, final String index, final String byteOrder) {
        switch (type) {
            case INT8:
                return "unchecked((sbyte)_buffer.GetByte(" + index + "))";

            case CHAR:
            case UINT8:
                return "_buffer.GetByte(" + index + ")";

            case INT16:
                return "_buffer.GetShort(" + index + byteOrder + ")";

            case UINT16:
                return "unchecked((ushort)_buffer.GetShort(" + index + byteOrder + "))";

            case INT32:
                return "_buffer.GetInt(" + index + byteOrder + ")";

            case UINT32:
                return "unchecked((uint)_buffer.GetInt(" + index + byteOrder + "))";

            case FLOAT:
                // TODO reinstate byte order when DirectBuffer supports byte order on GetFloat
                // return "_buffer.GetFloat(" + index + byteOrder + ")";
                return "_buffer.GetFloat(" + index + ")";

            case INT64:
                return "_buffer.GetLong(" + index + byteOrder + ")";

            case UINT64:
                return "unchecked((ulong)_buffer.GetLong(" + index + byteOrder + "))";

            case DOUBLE:
                return "_buffer.GetDouble(" + index + byteOrder + ")";
        }

        throw new IllegalArgumentException("primitive type not supported: " + type);
    }

    private String generatePut(
            final PrimitiveType type, final String index, final String value, final String byteOrder) {
        switch (type) {
            case CHAR:
            case INT8:
                return "_buffer.PutByte(" + index + ", unchecked((byte)" + value + "))";

            case UINT8:
                return "_buffer.PutByte(" + index + ", " + value + ")";

            case INT16:
                return "_buffer.PutShort(" + index + ", " + value + byteOrder + ")";

            case UINT16:
                return "_buffer.PutShort(" + index + ", unchecked((short)" + value + ")" + byteOrder + ")";

            case INT32:
                return "_buffer.PutInt(" + index + ", " + value + byteOrder + ")";

            case UINT32:
                return "_buffer.PutInt(" + index + ", unchecked((int)" + value + ")" + byteOrder + ")";

            case FLOAT:
                // TODO reinstate when DirectBuffer supports byte order on PutFloat
                // return "_buffer.PutFloat(" + index + ", " + value + byteOrder + ")";
                return "_buffer.PutFloat(" + index + ", " + value + ")";

            case INT64:
                return "_buffer.PutLong(" + index + ", " + value + byteOrder + ")";

            case UINT64:
                return "_buffer.PutLong(" + index + ", unchecked((long)" + value + ")" + byteOrder + ")";

            case DOUBLE:
                return "_buffer.PutDouble(" + index + ", " + value + byteOrder + ")";
        }

        throw new IllegalArgumentException("primitive type not supported: " + type);
    }

    private String generateChoiceGet(final PrimitiveType type, final String bitIndex, final String byteOrder) {
        switch (type) {
            case UINT8:
                return "0 != (_buffer.GetByte(_offset) & (1 << " + bitIndex + "))";

            case UINT16:
                return "0 != (_buffer.GetShort(_offset" + byteOrder + ") & (1 << " + bitIndex + "))";

            case UINT32:
                return "0 != (_buffer.GetInt(_offset" + byteOrder + ") & (1 << " + bitIndex + "))";

            case UINT64:
                return "0 != (_buffer.GetLong(_offset" + byteOrder + ") & (1L << " + bitIndex + "))";
        }

        throw new IllegalArgumentException("primitive type not supported: " + type);
    }

    private String generateStaticChoiceGet(final PrimitiveType type, final String bitIndex) {
        switch (type) {
            case UINT8:
                return "0 != (value & (1 << " + bitIndex + "))";

            case UINT16:
                return "0 != (value & (1 << " + bitIndex + "))";

            case UINT32:
                return "0 != (value & (1 << " + bitIndex + "))";

            case UINT64:
                return "0 != (value & (1L << " + bitIndex + "))";
        }

        throw new IllegalArgumentException("primitive type not supported: " + type);
    }

    private String generateChoicePut(final PrimitiveType type, final String bitIdx, final String byteOrder) {
        switch (type) {
            case UINT8:
                return
                        "        byte bits = _buffer.GetByte(_offset);\n" +
                                "        bits = (byte)(value ? bits | (1 << " + bitIdx + ") : bits & ~(1 << " + bitIdx + "));\n" +
                                "        _buffer.PutByte(_offset, bits);";

            case UINT16:
                return
                        "        short bits = _buffer.GetShort(_offset" + byteOrder + ");\n" +
                                "        bits = (short)(value ? bits | (1 << " + bitIdx + ") : bits & ~(1 << " + bitIdx + "));\n" +
                                "        _buffer.PutShort(_offset, bits" + byteOrder + ");";

            case UINT32:
                return
                        "        int bits = _buffer.GetInt(_offset" + byteOrder + ");\n" +
                                "        bits = value ? bits | (1 << " + bitIdx + ") : bits & ~(1 << " + bitIdx + ");\n" +
                                "        _buffer.PutInt(_offset, bits" + byteOrder + ");";

            case UINT64:
                return
                        "        long bits = _buffer.GetLong(_offset" + byteOrder + ");\n" +
                                "        bits = value ? bits | (1L << " + bitIdx + ") : bits & ~(1L << " + bitIdx + ");\n" +
                                "        _buffer.PutLong(_offset, bits" + byteOrder + ");";
        }

        throw new IllegalArgumentException("primitive type not supported: " + type);
    }

    private String generateStaticChoicePut(final PrimitiveType type, final String bitIdx) {
        switch (type) {
            case UINT8:
                return
                        "        return (byte)(value ? bits | (1 << " + bitIdx + ") : bits & ~(1 << " + bitIdx + "));\n";

            case UINT16:
                return
                        "        return (short)(value ? bits | (1 << " + bitIdx + ") : bits & ~(1 << " + bitIdx + "));\n";

            case UINT32:
                return
                        "        return value ? bits | (1 << " + bitIdx + ") : bits & ~(1 << " + bitIdx + ");\n";

            case UINT64:
                return
                        "        return value ? bits | (1L << " + bitIdx + ") : bits & ~(1L << " + bitIdx + ");\n";
        }

        throw new IllegalArgumentException("primitive type not supported: " + type);
    }

    private CharSequence generateEncoderDisplay(final String decoderName, final String baseIndent) {
        final String indent = baseIndent + INDENT;
        final StringBuilder sb = new StringBuilder();

        sb.append('\n');
        appendToString(sb, indent);
        sb.append('\n');
        append(sb, indent, "public StringBuilder AppendTo(StringBuilder builder)");
        append(sb, indent, "{");
        append(sb, indent, INDENT + decoderName + " writer = new " + decoderName + "();");
        append(sb, indent, "    writer.Wrap(_buffer, _offset, BLOCK_LENGTH, SCHEMA_VERSION);");
        sb.append('\n');
        append(sb, indent, "    return writer.AppendTo(builder);");
        append(sb, indent, "}");

        return sb.toString();
    }

    private CharSequence generateCompositeEncoderDisplay(final String decoderName, final String baseIndent) {
        final String indent = baseIndent + INDENT;
        final StringBuilder sb = new StringBuilder();
        appendToString(sb, indent);
        sb.append('\n');
        append(sb, indent, "public StringBuilder AppendTo(StringBuilder builder)");
        append(sb, indent, "{");
        append(sb, indent, INDENT + decoderName + " writer = new " + decoderName + "();");
        append(sb, indent, "    writer.Wrap(_buffer, _offset);");
        sb.append('\n');
        append(sb, indent, "    return writer.AppendTo(builder);");
        append(sb, indent, "}");

        return sb.toString();
    }

    private CharSequence generateCompositeDecoderDisplay(final List<Token> tokens, final String baseIndent) {
        final String indent = baseIndent + INDENT;
        final StringBuilder sb = new StringBuilder();

        appendToString(sb, indent);
        sb.append('\n');
        append(sb, indent, "public StringBuilder AppendTo(StringBuilder builder)");
        append(sb, indent, "{");
        Separators.BEGIN_COMPOSITE.appendToGeneratedBuilder(sb, indent + INDENT, "builder");

        int lengthBeforeLastGeneratedSeparator = -1;

        for (int i = 1, end = tokens.size() - 1; i < end; ) {
            final Token encodingToken = tokens.get(i);
            final String propertyName = formatPropertyName(encodingToken.name());
            lengthBeforeLastGeneratedSeparator = writeTokenDisplay(propertyName, encodingToken, sb, indent + INDENT);
            i += encodingToken.componentTokenCount();
        }

        if (-1 != lengthBeforeLastGeneratedSeparator) {
            sb.setLength(lengthBeforeLastGeneratedSeparator);
        }

        Separators.END_COMPOSITE.appendToGeneratedBuilder(sb, indent + INDENT, "builder");
        sb.append('\n');
        append(sb, indent, "    return builder;");
        append(sb, indent, "}");

        return sb.toString();
    }

    private CharSequence generateChoiceDisplay(final List<Token> tokens) {
        final String indent = INDENT;
        final StringBuilder sb = new StringBuilder();

        appendToString(sb, indent);
        sb.append('\n');
        append(sb, indent, "public StringBuilder AppendTo(StringBuilder builder)");
        append(sb, indent, "{");
        Separators.BEGIN_SET.appendToGeneratedBuilder(sb, indent + INDENT, "builder");
        append(sb, indent, "    bool atLeastOne = false;");

        tokens
                .stream()
                .filter((token) -> token.signal() == Signal.CHOICE)
                .forEach(token ->
                {
                    final String choiceName = formatPropertyName(token.name());
                    append(sb, indent, "    if (" + choiceName + "())");
                    append(sb, indent, "    {");
                    append(sb, indent, "        if (atLeastOne)");
                    append(sb, indent, "        {");
                    Separators.ENTRY.appendToGeneratedBuilder(sb, indent + INDENT + INDENT + INDENT, "builder");
                    append(sb, indent, "        }");
                    append(sb, indent, "        builder.Append(\"" + choiceName + "\");");
                    append(sb, indent, "        atLeastOne = true;");
                    append(sb, indent, "    }");
                });

        Separators.END_SET.appendToGeneratedBuilder(sb, indent + INDENT, "builder");
        sb.append('\n');
        append(sb, indent, "    return builder;");
        append(sb, indent, "}");

        return sb.toString();
    }

    private CharSequence generateDecoderDisplay(
            final String name,
            final List<Token> tokens,
            final List<Token> groups,
            final List<Token> varData,
            final String baseIndent) throws IOException {
        final String indent = baseIndent + INDENT;
        final StringBuilder sb = new StringBuilder();

        sb.append('\n');
        appendToString(sb, indent);
        sb.append('\n');
        append(sb, indent, "public StringBuilder AppendTo(StringBuilder builder)");
        append(sb, indent, "{");
        append(sb, indent, "    int originalLimit = Limit();");
        append(sb, indent, "    Limit(_offset + _actingBlockLength);");
        append(sb, indent, "    builder.Append(\"[" + name + "](sbeTemplateId=\");");
        append(sb, indent, "    builder.Append(TEMPLATE_ID);");
        append(sb, indent, "    builder.Append(\"|sbeSchemaId=\");");
        append(sb, indent, "    builder.Append(SCHEMA_ID);");
        append(sb, indent, "    builder.Append(\"|sbeSchemaVersion=\");");
        append(sb, indent, "    if (_parentMessage._actingVersion != SCHEMA_VERSION)");
        append(sb, indent, "    {");
        append(sb, indent, "        builder.Append(_parentMessage._actingVersion);");
        append(sb, indent, "        builder.Append('/');");
        append(sb, indent, "    }");
        append(sb, indent, "    builder.Append(SCHEMA_VERSION);");
        append(sb, indent, "    builder.Append(\"|sbeBlockLength=\");");
        append(sb, indent, "    if (_actingBlockLength != BLOCK_LENGTH)");
        append(sb, indent, "    {");
        append(sb, indent, "        builder.Append(_actingBlockLength);");
        append(sb, indent, "        builder.Append('/');");
        append(sb, indent, "    }");
        append(sb, indent, "    builder.Append(BLOCK_LENGTH);");
        append(sb, indent, "    builder.Append(\"):\");");
        appendDecoderDisplay(sb, tokens, groups, varData, indent + INDENT);
        sb.append('\n');
        append(sb, indent, "    Limit(originalLimit);");
        sb.append('\n');
        append(sb, indent, "    return builder;");
        append(sb, indent, "}");

        return sb.toString();
    }

    private void appendGroupInstanceDecoderDisplay(
            final StringBuilder sb,
            final List<Token> fields,
            final List<Token> groups,
            final List<Token> varData,
            final String baseIndent) {
        final String indent = baseIndent + INDENT;

        sb.append('\n');
        appendToString(sb, indent);
        sb.append('\n');
        append(sb, indent, "public StringBuilder AppendTo(StringBuilder builder)");
        append(sb, indent, "{");
        Separators.BEGIN_COMPOSITE.appendToGeneratedBuilder(sb, indent + INDENT, "builder");
        appendDecoderDisplay(sb, fields, groups, varData, indent + INDENT);
        Separators.END_COMPOSITE.appendToGeneratedBuilder(sb, indent + INDENT, "builder");
        append(sb, indent, "    return builder;");
        append(sb, indent, "}");
    }

    private void appendDecoderDisplay(
            final StringBuilder sb,
            final List<Token> fields,
            final List<Token> groups,
            final List<Token> varData,
            final String indent) {
        int lengthBeforeLastGeneratedSeparator = -1;

        for (int i = 0, size = fields.size(); i < size; ) {
            final Token fieldToken = fields.get(i);
            if (fieldToken.signal() == Signal.BEGIN_FIELD) {
                final Token encodingToken = fields.get(i + 1);

                final String fieldName = formatPropertyName(fieldToken.name());
                append(sb, indent, "//" + fieldToken);
                lengthBeforeLastGeneratedSeparator = writeTokenDisplay(fieldName, encodingToken, sb, indent);

                i += fieldToken.componentTokenCount();
            } else {
                ++i;
            }
        }

        for (int i = 0, size = groups.size(); i < size; i++) {
            final Token groupToken = groups.get(i);
            if (groupToken.signal() != Signal.BEGIN_GROUP) {
                throw new IllegalStateException("tokens must begin with BEGIN_GROUP: token=" + groupToken);
            }

            append(sb, indent, "//" + groupToken);

            final String groupName = formatPropertyName(groupToken.name());
            final String groupDecoderName = decoderName(formatClassName(groupToken.name()));

            append(
                    sb, indent, "builder.Append(\"" + groupName + Separators.KEY_VALUE + Separators.BEGIN_GROUP + "\");");
            append(sb, indent, groupDecoderName + " " + groupName + " = this." + groupName + "();");
            append(sb, indent, "if (" + groupName + ".Count() > 0)");
            append(sb, indent, "{");
            append(sb, indent, "    while (" + groupName + ".HasNext())");
            append(sb, indent, "    {");
            append(sb, indent, "        " + groupName + ".Next().AppendTo(builder);");
            Separators.ENTRY.appendToGeneratedBuilder(sb, indent + INDENT + INDENT, "builder");
            append(sb, indent, "    }");
            append(sb, indent, "    builder.Length = builder.Length - 1;");
            append(sb, indent, "}");
            Separators.END_GROUP.appendToGeneratedBuilder(sb, indent, "builder");

            lengthBeforeLastGeneratedSeparator = sb.length();
            Separators.FIELD.appendToGeneratedBuilder(sb, indent, "builder");

            i = findEndSignal(groups, i, Signal.END_GROUP, groupToken.name());
        }

        for (int i = 0, size = varData.size(); i < size; ) {
            final Token varDataToken = varData.get(i);
            if (varDataToken.signal() != Signal.BEGIN_VAR_DATA) {
                throw new IllegalStateException("tokens must begin with BEGIN_VAR_DATA: token=" + varDataToken);
            }

            append(sb, indent, "//" + varDataToken);

            final String characterEncoding = varData.get(i + 3).encoding().characterEncoding();
            final String varDataName = formatPropertyName(varDataToken.name());
            append(sb, indent, "builder.Append(\"" + varDataName + Separators.KEY_VALUE + "\");");
            if (null == characterEncoding) {
                append(sb, indent, "builder.Append(" + varDataName + "Length() + \" raw bytes\");");
            } else {
                append(sb, indent, "builder.Append(" + varDataName + "());");
            }

            lengthBeforeLastGeneratedSeparator = sb.length();
            Separators.FIELD.appendToGeneratedBuilder(sb, indent, "builder");

            i += varDataToken.componentTokenCount();
        }

        if (-1 != lengthBeforeLastGeneratedSeparator) {
            sb.setLength(lengthBeforeLastGeneratedSeparator);
        }
    }

    private int writeTokenDisplay(
            final String fieldName,
            final Token typeToken,
            final StringBuilder sb,
            final String indent) {
        append(sb, indent, "//" + typeToken);

        if (typeToken.encodedLength() <= 0 || typeToken.isConstantEncoding()) {
            return -1;
        }

        append(sb, indent, "builder.Append(\"" + fieldName + Separators.KEY_VALUE + "\");");

        switch (typeToken.signal()) {
            case ENCODING:
                if (typeToken.arrayLength() > 1) {
                    if (typeToken.encoding().primitiveType() == PrimitiveType.CHAR) {
                        append(sb, indent,
                                "for (int i = 0; i < " + fieldName + "Length() && " + fieldName + "(i) > 0; i++)");
                        append(sb, indent, "{");
                        append(sb, indent, "    builder.Append((char)" + fieldName + "(i));");
                        append(sb, indent, "}");
                    } else {
                        Separators.BEGIN_ARRAY.appendToGeneratedBuilder(sb, indent, "builder");
                        append(sb, indent, "if (" + fieldName + "Length() > 0)");
                        append(sb, indent, "{");
                        append(sb, indent, "    for (int i = 0; i < " + fieldName + "Length(); i++)");
                        append(sb, indent, "    {");
                        append(sb, indent, "        builder.Append(" + fieldName + "(i));");
                        Separators.ENTRY.appendToGeneratedBuilder(sb, indent + INDENT + INDENT, "builder");
                        append(sb, indent, "    }");
                        append(sb, indent, "    builder.Length = builder.Length - 1;");
                        append(sb, indent, "}");
                        Separators.END_ARRAY.appendToGeneratedBuilder(sb, indent, "builder");
                    }
                } else {
                    // have to duplicate because of checkstyle :/
                    append(sb, indent, "builder.Append(" + fieldName + "());");
                }
                break;

            case BEGIN_ENUM:
            case BEGIN_SET:
                append(sb, indent, "builder.Append(" + fieldName + "());");
                break;

            case BEGIN_COMPOSITE:
                append(sb, indent, fieldName + "().AppendTo(builder);");
                break;
        }

        final int lengthBeforeFieldSeparator = sb.length();
        Separators.FIELD.appendToGeneratedBuilder(sb, indent, "builder");

        return lengthBeforeFieldSeparator;
    }

    private void appendToString(final StringBuilder sb, final String indent) {
        sb.append('\n');
        append(sb, indent, "public override string ToString()");
        append(sb, indent, "{");
        append(sb, indent, "    return AppendTo(new StringBuilder(100)).ToString();");
        append(sb, indent, "}");
    }

    enum CodecType {
        DECODER,
        ENCODER
    }
}
