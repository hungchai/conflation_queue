# Conflating Queue

Two interfaces, `ConflatingQueue` and `KeyValue` are provided.

The assignment is to provide an implementation of the `ConflatingQueue` interface conforming to the Javadoc specification provided within the interface.  **You may only use the classes and APIs available in the J2SE standard library version 8, and no third party libraries.**  You may use any third party libraries within your tests, but not in your implementation of `ConflatingQueue` itself.

Your solution is expected to be unit tested against the requirements.  It is **not** a requirement to test either the blocking behaviour, or the thread-safety of the `ConflatingQueue` although you may do so if you wish.  Use the standard Gradle/Maven directory for your test classes.  

You are not required to provide an implementation of `KeyValue` but may find it useful for your tests.

Correctness and quality of the solution as a whole is more important than performance or extensibility, however your solution should not be needlessly inefficient.

## Submission

You can run

```bash
gradle clean srcZip
```

and this will generate a zip archive of the src and build file under `build/distributions/conflating-queue-src.zip`.  Reply to the assignment instructions email, attaching this zip archive.