# RepCRec

This project is a course project of Advanced Database System in NYU. 

Professor is Dennis Shaha

## Run this project using jar file
You could run this application directly through the jar file under root directory of the porject

There are a bunch of test cases we've already created under TestInput. You could run any of them once a time by giving the path of the test input file related to project root path.

Firstly, you have to make sure you are in the root directory of the project right now: ..../adb-repcrec/ 

Then if you want to test the third test case, you could directly run

```
java -jar adb-repcrec-all-1.0-SNAPSHOT.jar ./TestInput/test3.txt
```

The program will run after this command, and you could see the result both from console and an output file.

The output will automatically generate under directory TestOutput' under project root path.

## Run this project using gradle

If you have gradle installed on your machine, you could also choose to run this project using
gradle. For example, if you want to run test case 1, you could simple run command

```$xslt
./gradlew run --args='./TestInput/test1.txt'
```
