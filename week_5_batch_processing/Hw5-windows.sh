
export JAVA_HOME="/D/tools/jdk-11.0.17"
export PATH="${JAVA_HOME}/bin:${PATH}"

export HADOOP_HOME="/D/tools/hadoop-3.2.0"
export PATH="${HADOOP_HOME}/bin:${PATH}"

export SPARK_HOME="/D/tools/spark-3.3.2-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"


export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"