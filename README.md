### Spark/GraphX application for analysing Maven dependencies as a graph

This Spark/GraphX application allows you to analyse the Maven dependencies dataset as in https://ogirardot.wordpress.com/2013/01/11/state-of-the-mavenjava-dependency-graph/

The application runs on Spark and takes as extra commandline argument the location of the data. 
The output of the application is the 25 top ranking maven dependencies (according to the PageRank algorithm), i.e. the combination of (groupId,artifactId,version) on which other projects depend most.
