# sbe-generators

## Usage
1. Get sbe-all.jar from maven
1. Run `gradlew clean build`
1. Get build/libs/sbe-generators-csharp-1.0-SNAPSHOT.jar
1. Run the following command to generate code with this generator

```
java -Dsbe.target.language=sbe.generation.csharp.CSharp -Dsbe.xinclude.aware=true
    -cp sbe-all-1.12.3-all.jar;sbe-generators-csharp-1.0-SNAPSHOT.jar
    uk.co.real_logic.sbe.SbeTool schema.xml
```