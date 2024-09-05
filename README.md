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

4. run the command to start the hadoop cluster:

```bash
start-dfs.sh
```
or

```bash
start-all.sh
```

5. run the command to check status of the hadoop cluster:

```bash
jps
```

6. If not made already, make the directories in HDFS:

```bash
hadoop fs -mkdir -p /home/hadoop/hadoopdata/hdfs/data
```

7. Copy the file to HDFS:

Small dataset:

```bash
hadoop fs -put /mnt/c/Users/hp/OneDrive/Desktop/hadoop_database/tripdata.csv /home/hadoop/hadoopdata/hdfs/data
```

Large dataset:



8. Run the command to format the namenode (if not formatted already):

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
hadoop fs -cat /home/hadoop/hadoopdata/hdfs/data/tripdata.csv | python3 mapper.py argument_name | sort | python3 reducer.py argument_name
```

For testing the functions in the files locally, rum:

```bash
hadoop fs -cat /home/hadoop/hadoopdata/hdfs/data/tripdata.csv | python3 mapper.py function_name | sort | python3 reducer.py function_name
```

9. Remove the existing output directory if needed by running the command:

```bash
hadoop fs -rm -r /home/hadoop/hadoopdata/hdfs/output
```

14. Run the hadoop streaming Job:

```bash
hadoop jar /home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar   -input /home/hadoop/hadoopdata/hdfs/data/   -output /home/hadoop/hadoopdata/hdfs/output/   -mapper "python3 mapper.py argument_name"   -reducer "python3 reducer.py argument_name"
```

15. Check the output:

```bash
hadoop fs -ls /home/hadoop/hadoopdata/hdfs/output/
hadoop fs -cat /home/hadoop/hadoopdata/hdfs/output/part-*
```

16. To edit the mapper.py file in wsl itself (no need for steps 10,11,12 after every change in the file):

```bash
nano  /mnt/c/Users/hp/OneDrive/Desktop/hadoop_database/mapper.py
```




