How to Build
=============
mvn package

After build, there is a jar under target, streaming-java-template-1.0-SNAPSHOT.jar

How to run
============
1. Start the gearpump cluster (0.3.1)

  a) Download from https://github.com/intel-hadoop/gearpump/releases/download/0.3.1/binary.gearpump.tar.gz

  b) After extraction,  start the local cluster,
  ```bash
  bin/local -ip 127.0.0.1 -port 3000
  ```

  c) Start the UI server
  ```bash
  bin/services -master 127.0.0.1:3000
  ```

2. Submit the wordcount-jar
  bin/gear app -jar path/to/streaming-java-template-1.0-SNAPSHOT.jar javatemplate.WordCount 127.0.0.1:3000

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