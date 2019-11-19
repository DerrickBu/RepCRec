# RepCRec

This project is a course project of Advanced Database System in NYU. 
Professor is Dennis Shaha

We implement a tiny distributed database, complete
with multi-version concurrency control, deadlock detection, replication, and
failure recovery

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
## Algorithm we use

1. We use strict two phase locking (using read and write locks) at each site and validation at commit time.

2. Read-only transactions should use multi-version read consistency.

3. We use DFS to detect deadlock cycle and abort the youngest transaction.

## Attention

1. We will ensure that when a transaction is waiting, it will not receive another operation

2. Our application could prevent write starvation. 

   For example, if requests arrive in the order R(T1,x), R(T2,x), W(T3,x,73), R(T4,x) then,
   assuming no transaction aborts, first T1 and T2 will share read locks on x, 
   then T3 will obtain a write lock on x and then T4 a read lock on x. Note that T4 does not skip in front of T3, 
   as that might result in starvation for writes.

