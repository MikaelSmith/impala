---
applyTo: "fe/src/test/java/**/*Test.java,fe/src/main/java/**/*.java,java/**/src/test/java/**/*Test.java,java/**/src/main/java/**/*.java"
---

## Running Unit Tests
To execute Java JUnit tests, you must run commands from the `fe` directory. Use this command format:
```bash
cd fe && mvn -Dtest=ClassNameTest#testFunctionName
```
Where `ClassNameTest` is the name of the JUnit Java class and `testFunctionName` is the name of the individual JUnit test to execute.

To execute all Java JUnit tests in a single JUnit class, you must run commands from the `fe` directory. Use this command format:
```bash
cd fe && mvn -Dtest=ClassNameTest
```
Where `ClassNameTest` is the name of the JUnit Java class.


## Handling Dependencies of Classes Under Test
This project is not written using the Inversion of Control pattern with dependency injection. Instead, each class is typically responsible for instantiating its own dependencies. This design pattern makes JUnit testing difficult and usually requires setting the scope of variables in classes under test to `protected` or creating `protected` scoped functions to allow modifying dependencies.

## Mocking
If possible, mock or stub dependencies using the Mockito library.
