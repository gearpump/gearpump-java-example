How to Build
=============
mvn package

After build, there is a jar under target, streaming-java-template-1.0-SNAPSHOT.jar

How to run
============
1. Start the gearpump cluster (0.3.4)

  a) Download from http://www.gearpump.io/site/downloads/

  b) After extraction,  start the local cluster,
  ```bash
  bin/local
  ```

  c) Start the UI server
  ```bash
  bin/services
  ```

2. Submit the wordcount-jar
  ```bash
  bin/gear app -jar path/to/streaming-java-template-1.0-SNAPSHOT.jar javatemplate.WordCount
  ```
  
3. Check the UI
  http://127.0.0.1:8090/  
  

NOTE:
```
Please use Java7 to run the cluster

You can set the ENV JAVA_HOME

On windows:
set JAVA_HOME={path_to_java_7}

on Linux
export JAVA_HOME={path_to_java_7}
```
