HADOOP_HOME=/usr/local/cs/hadoop-0.20.2/
NAME=WikipediaBFS

default:
	javac -classpath ${HADOOP_HOME}/hadoop-0.20.2-core.jar:${HADOOP_HOME}/cloud9.jar:${HADOOP_HOME}/lib/log4j-1.2.15.jar *.java
	jar -cf ${NAME}.jar *.class -C ${HADOOP_HOME} lib/cloud9.jar

clean:
	rm -v *.class ${NAME}.jar