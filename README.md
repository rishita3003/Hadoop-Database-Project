# Hadoop Database Project - Implementing MapReduce 

## Steps to run the project

### Follow the steps in the link below to install Hadoop on Ubuntu 22.04  -:
https://medium.com/@abhikdey06/apache-hadoop-3-3-6-installation-on-ubuntu-22-04-14516bceec85

### After Hadoop is installed, follow the steps below -:

Include the line at the top of your mapper and reducer files:

```
#!/usr/bin/env python3
```

## How to run

1. Install Python 3.x

```bash
sudo apt update
```

```bash
sudo apt install python3
```

```bash
python3 --version
```

2. Clone the repository

3. In WSL switch user to hadoop by running the command:

```bash
su - hadoop
```

4. Run the command to start the hadoop cluster:

```bash
start-dfs.sh
```
or

```bash
start-all.sh
```

5. Run the command to check status of the hadoop cluster:

```bash
jps
```

6. If not made already, make the directories in HDFS:

```bash
hadoop fs -mkdir -p /home/hadoop/hadoopdata/hdfs/data
```
Note : To delete the directory in HDFS:

```bash
hadoop fs -rm -r /home/hadoop/hadoopdata/hdfs/data
```

7. Copy the file to HDFS:

Small dataset:

```bash
hadoop fs -put /mnt/c/Users/hp/OneDrive/Desktop/hadoop_database/tripdata.csv /home/hadoop/hadoopdata/hdfs/data
```

Large dataset:

```bash
hadoop fs -put /mnt/c/Users/hp/OneDrive/Desktop/hadoop_database/largetripdata.csv /home/hadoop/hadoopdata/hdfs/data
```

8. Import data to HDFS:

First save the file in a local folder and then copy it in wsl : 
```bash
cp /mnt/c/Users/hp/OneDrive/Desktop/hadoop_database/import_data.py ~/import_data.py
```

```bash
python3 import_data.py /mnt/c/Users/hp/OneDrive/Desktop/hadoop_database/largetripdata.csv /mnt/c/Users/hp/OneDrive/Desktop/hadoop_database/headers.csv
```

9. Run the command to format the namenode (if not formatted already):

```bash
hadoop dfs -rm -r /user/hadoop/output
```

10. Command to copy a file from windows to WSL:

```bash
cp /mnt/c/Users/hp/OneDrive/Desktop/hadoop_database/mapper.py ~/mapper.py

```

```bash
cp /mnt/c/Users/hp/OneDrive/Desktop/hadoop_database/reducer.py ~/reducer.py

```

11. Convert mapper.py and reducer.py to executables by running:

```
chmod +x ~/mapper.py
chmod +x ~/reducer.py

```

12. Convert Windows Line Endings to Unix line Endings (if you edited your script in windows, it might have Windows-style line endings - '\r\n' which should be converted to Unix Style Line Endings - '\n')

Go to root user:

```bash
su - root
```

Install dos2unix:

```bash
sudo apt install dos2unix
```

Convert the files:

```bash
dos2unix ~/mapper.py
dos2unix ~/reducer.py
```

13. test locally before running on hadoop:

```bash
hadoop fs -cat /home/hadoop/hadoopdata/hdfs/data/shoppingdata.csv | python3 mapper.py | sort | python3 reducer.py 
```

For testing the functions in the files locally, run:

```bash
hadoop fs -cat /home/hadoop/hadoopdata/hdfs/data/shoppingdata.csv | python3 mapper.py argument_name | sort | python3 reducer.py argument_name
```

14. Remove the existing output directory if needed by running the command:

```bash
hadoop fs -rm -r /home/hadoop/hadoopdata/hdfs/output
```

15. Run the hadoop streaming Job:

```bash
hadoop jar /home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar   -input /home/hadoop/hadoopdata/hdfs/data/   -output /home/hadoop/hadoopdata/hdfs/output/   -mapper "python3 mapper.py argument_name"   -reducer "python3 reducer.py argument_name"
```

eg. ```hadoop jar /home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar   -input /home/hadoop/hadoopdata/hdfs/data/largetripdata.csv   -output /home/hadoop/hadoopdata/hdfs/output/   -mapper "python3 mapper.py filter"   -reducer "python3 reducer.py filter" ``` \
eg. ```hadoop jar /home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar   -input /home/hadoop/hadoopdata/hdfs/data/tripdata.csv   -output /home/hadoop/hadoopdata/hdfs/output/   -mapper "python3 mapper.py group"   -reducer "python3 reducer.py group"```

16. Check the output:

```bash
hadoop fs -ls /home/hadoop/hadoopdata/hdfs/output/
hadoop fs -cat /home/hadoop/hadoopdata/hdfs/output/part-*
```

17. To edit the mapper.py file in wsl itself (no need for steps 10,11,12 after every change in the file):

```bash
nano  /mnt/c/Users/hp/OneDrive/Desktop/hadoop_database/mapper.py
```


To switch between large and small dataset:

1. copy the import_data.py file to wsl:
```bash
cp /mnt/c/Users/hp/OneDrive/Desktop/hadoop_database/import_data.py ~/import_data.py
```

```bash
python3 import_data.py /mnt/c/Users/hp/OneDrive/Desktop/hadoop_database/largetripdata.csv /mnt/c/Users/hp/OneDrive/Desktop/hadoop_database/headers.csv
```

```bash
python3 import_data.py /mnt/c/Users/hp/OneDrive/Desktop/hadoop_database/tripdata.csv /mnt/c/Users/hp/OneDrive/Desktop/hadoop_database/headers.csv
```

2. change the queries in the mapper.py file to switch between large and small dataset.
