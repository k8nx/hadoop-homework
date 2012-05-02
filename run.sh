#!/bin/sh
#maven 미설치시 아래 주석 푸시면 됩니다.
#wget http://ftp.daum.net/apache//maven/binaries/apache-maven-2.2.1-bin.tar.gz
#tar xvfz apache-maven-2.2.1-bin.tar.gz
#PATH=$PATH:./apache-maven-2.2.1

rm -rf homework-0.1.0-dev.jar
mvn clean package

# test skip?
# mvn clean package -DskipTests=true 

cp target/homework-0.1.0-dev.jar .

$HADOOP_HOME/bin/hadoop fs -rmr /nexr/input
$HADOOP_HOME/bin/hadoop fs -rmr /nexr/output
$HADOOP_HOME/bin/hadoop fs -copyFromLocal emp.txt /nexr/input/emp/emp1.txt
$HADOOP_HOME/bin/hadoop fs -copyFromLocal dept.txt /nexr/input/dept/dept.txt

$HADOOP_HOME/bin/hadoop jar homework-0.1.0-dev.jar kr.devpub.nexr.ex.problem.Problem2V2

