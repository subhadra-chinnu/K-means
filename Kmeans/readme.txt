Open IntellijIdea and created project as kmeans.
installed jar files of hadoop with maven and saved all hadoop jar files in /kmeans/target
Under kmeans-create com.mamatha package and in this package created Cluster.java program
Gave the path of input and output paths of HDFS in java program using addInputPath and setOutputPath 


In HDFS
su -hduser
cd /usr/local/hadoop
bin/hadoop dfs -mkdir /kmeans
bin/hadoop dfs -mkdir /kmeans/input
bin/hadoop dfs -mkdir /kmeans/input/data
bin/hadoop dfs -mkdir /kmeans/centroids
bin/hadoop dfs -mkdir /kmeans/centroids/centroids.txt
bin/hadoop dfs -mkdir /kmeans/output
bin/hadoop jar /home/mamatha/IdeaProjects/kmeans/target/kmean-1.0-SNAPSHOT  com.mamatha.Cluster
bin/hadoop dfs -cat output





