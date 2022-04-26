# big-data-example
Example code word count already running in Hadoop using MapReduce

## In Local
Please use the command below
1. ```mvn clean compile package```
2. ```java -jar target/*.jar input output```, remove folder output if already exist

## In Hadoop
Please use the command ```hadoop jar *.jar [folder input] [folder output]```

**Disclaimer**
1. for input file could spesific file, such as ```java -jar target/*.jar input/input01.txt output```
2. Edit file `pom.xml` to change main class in properties `<mainClass>jobsheet11.App</mainClass>`
