package sbe.generation.csharp;

import uk.co.real_logic.sbe.generation.CodeGenerator;
import uk.co.real_logic.sbe.generation.TargetCodeGenerator;
import uk.co.real_logic.sbe.ir.Ir;

import static uk.co.real_logic.sbe.SbeTool.*;

public class CSharp implements TargetCodeGenerator {
  public CodeGenerator newInstance(final Ir ir, final String outputDir) {
    return new CSharpGenerator(
      ir,
      System.getProperty(JAVA_ENCODING_BUFFER_TYPE, "IMutableDirectBuffer"),
      System.getProperty(JAVA_DECODING_BUFFER_TYPE, "IDirectBuffer"),
      Boolean.getBoolean(JAVA_GROUP_ORDER_ANNOTATION),
      Boolean.getBoolean(JAVA_GENERATE_INTERFACES),
      Boolean.getBoolean(DECODE_UNKNOWN_ENUM_VALUES),
      new CSharpOutputManager(outputDir, ir.applicableNamespace()));
  }
}
