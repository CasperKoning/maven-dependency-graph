### Spark/GraphX application for analysing Maven dependencies as a graph

This Spark/GraphX application allows you to analyse the Maven dependencies dataset as in https://ogirardot.wordpress.com/2013/01/11/state-of-the-mavenjava-dependency-graph/

#### Input
The application runs on Spark and takes two extra commandline arguments:
* args[0]: File path of input file.
* args[1]: Output directory. (directory has to exist, no trailing \'s)

#### Output
The application writes the following files as output:
* the 25 top ranking maven dependencies (according to the PageRank algorithm), i.e. the combination of (groupId,artifactId,version) on which other projects depend most.
* the vertices of the graph
* the edges of the graph

#### Requirements
* Scala 2.10.5
* SBT (Scala 2.10 compatible version)
* Spark 1.4.0
