# 104dtt-spark-guide

Spark Guide 是一個讓104同仁可以一起熟悉spark的小天地，除了Spark以外也歡迎大家一起討論大數據的操作技巧喔

>spark-demo* 是2021年4月ETC demo code

## 讀取 ipynb

- 方法
  - GitHub Viewer (有可能開不起來)
  - 安裝Jupyter Notebook or JupyterLab
  - use google [Colab](https://colab.research.google.com/)

## Spark 安裝

### Spark 下載安裝

Ubuntu 20.04 ＋
無 cluster 設定

1. Install Oracle Java JDK or OpenJDK(要用root)

    ```shell
    apt-get update
    apt install -y openjdk-11-jre-headless
    ```

2. Check Java version

    ```shell
    java -version
    javac -version
    update-alternatives --display java
    ```

3. Download Apache Spark

    ![image](https://github.com/104corp/104dtt-spark-guide/blob/master/img/download_apache_spark.png)

    ```shell
    wget https://downloads.apache.org/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz
    ```

4. extract spark tar file to /usr/local

    ``` shell
    tar -xvf spark-3.1.1-bin-hadoop3.2.tgz -C /usr/local
    mv /usr/local/spark-3.1.1-bin-hadoop3.2 /usr/local/spark
    ```

5. Set Spark environment variables (on all nodes)

    ```shell
    vi ~/.bashrc
    # Set SPARK_HOME
    export SPARK_HOME=/usr/local/spark
    # Set JAVA_HOME
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
    source ~/.bashrc
    ```

6. Configure spark-env.sh

    ```shell
    cd /usr/local/spark/conf
    cp spark-env.sh.template spark-env.sh
    nano spark-env.sh
    export SPARK_MASTER_HOST="master1.example.org"
    export PYSPARK_PYTHON=python3
    ```

### DockerStart

如果熟悉docker，也可以用docker啟動Spark，還可以玩最新的Kubenetes mode，[Dockerfile](https://github.com/104corp/104dtt-spark-guide/blob/master/Dockerfile)

當前目錄在 104dtt-spark-guide 目錄中

``` shell
docker build . -t pyspark:test
docker run --rm -ti -p 8888:8888 -v $(pwd):/opt/workspace/104dtt-spark-guide pyspark:test
```

在網址輸入： http://localhost:8888/lab
就可以開始玩demo code了


## Reference
