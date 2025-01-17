Scaling Out Schema-free Stream Joins
======

---

Introduction
------

---

This work focuses on computing natural joins over a stream of schema-free JSON documents. Initially, the data is partitioned through a partitioning algorithm that uses the main principles of association
analysis to identify patterns of co-occurrence of the attribute-value pairs within the documents. Based on the computed partitions, the data is forwarded to the responsible compute nodes. 
Every compute node, computes the joinable documents by using a join algorithm based on FP-trees. Additionally, we provide a practical solution for cases of low attribute-value variety. [1]

If you compare with this code or use it in your research, please cite:   


```
    @inproceedings{scaling-out-schema-free-stream-joins,    
        author    = {Damjan Gjurovski and Sebastian Michel},    
        title     = {Scaling Out Schema-free Stream Joins},    
        booktitle = {36th {IEEE} International Conference on Data Engineering, {ICDE} 2020,    
                    Dallas, Texas, April 20-24, 2020},    
        year      = {2020}    
    }
```   

A link to the pdf is available on the author's [website](https://dbis.informatik.uni-kl.de/index.php/en/people/damjan-gjurovski). 

Dataset
------

---
As a dataset, we use NoBench [2]. In this repository, there is a sample of 10,000,000 JSON objects (which we treat as documents) generated with the NoBench generator that can be used for testing the streaming application. 
The documents are located in the folder `data/`. 
There are 100 documents where every document contains 100,000 JSON objects. 

Running the application
------

---

The main class consisting of the settings for running the proposed Apache Storm topology is called *AssociationGroupsTopology.java*. 

For every component (spout or bolt), the number of instances can be varied by modifying the respective variable in the *AssociationGroupsTopology* class. The required number of partitions
can be specified through the variable `k`. The number of partitions is directly connected to the number of *FPTreeJoinerBolt* instances, so always the number of partitions `k` needs to be equal
to the number of joiner instances defined by the variable `numberOfInstancesForFpTreeJoinerBolt`.

The window size, representing the number of documents that will be used for partitioning, can be varied by modifying the `windowSize` variable. 
The currently specified size of the window is 430,000 documents, which corresponds to a 3-minute window. For a 6 and 9 minute window, this number needs to be multiplied by 2 and 3 respectively. 

The path to the input folder consisting of JSON documents generated by NoBench can be specified through the `pathToStoringFolder` variable. If there is a need for using different documents, the *NoBenchSpout*
responsible for parsing the documents should be modified. 

If there is a need for expanding attributes, the `performJoinOfKVPairs` variable should be set to true. The expansion of attributes is required when there is an attribute whose number of unique values
is smaller than the required number of partitions. 

To store the computed join results in CSV files first the path to the output folder needs to be set in the *JoinResultsCsvOutput* class by modifying the `pathToFolder` variable. Secondly, the required lines 
that perform the creation of the files need to be uncommented in the *FPTreeJoinerBolt* class.

#### Local mode 

Using the command: `mvn compile exec:java -Dstorm.topology=ssfsj.AssociationGroupsTopology`

#### Cluster
The needed JAR can be created using the command: `mvn assembly:assembly`. Once the JAR is transfered to the cluster, 

the command `path-to-storm/storm jar ScalingOutSchemaFreeStreamJoins-1.0-SNAPSHOT-jar-with-dependencies.jar ssfsj.AssociationGroupsTopology association-groups-topology`,

can be used for starting the application on the Storm cluster.
  

References
------

---
[1] - Damjan Gjurovski and Sebastian Michel. "Scaling Out Schema-free Stream Joins," ICDE 2020, 2020. 

[2] - C. Chasseur, Y. Li, and J. M. Patel, “Enabling JSON document stores in relational systems,” WebDB 2013, pp. 1–6, 2013.