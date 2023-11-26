![Untitled Diagram-Page-2](https://github.com/IlayaBharathi260199/IlayaBharathi260199/assets/151670523/3373dfe1-7690-44ee-b4b9-132db137a83d)







Streaming with NIFI,KAFKA & SPARK Integration
=============================================

RDD Streaming
-------------


Procedure
=========

Step 1: Start Nifi services and open Nifi UI

Step 2: Create suitable processors (eg: Getfile & Putkafka ) and configure

Getfile
=======
1.configure input path and schedule time and etc......

PutKafka
========
1.configure Topic name

2.Scheduling time

3.host or known brokers

Step 3: Start Zookeeper services (QuorumPeerMain) , used to store meta data from kafka

Step 4: Start kafka (*** w/o zookeeper kafka will not run ***)

Step 5: check if all services are running or not by typing "jps" in terminal ,if not start all

eg:

2648 QuorumPeerMain

3050 Kafka

2555 RunNiFi

15724 Jps

2572 NiFi

Step 6: Create topic(channel) in kakfa  (in this project we are using nifi as producer and Spark as Consumer)

step 7: Kafka + spark integration (Refer:https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html)

           Totaly 9 steps are there
           ========================
           Step 1: Add connectivity JARS (add dependencies in build.sbt then build) for more understanding watch build.sbt
           Step 2: Add necessary Imports
           Step 3: Initialize streaming context (val ssc= streamingContext)
           Step 4: KafkaParams----localhost:9092 (val kafkapararams = localhost:9092)
           Step 5: Topic Name (val topicname =Array("ilaya")
           Step 6: kafkaUtils (Streaming context,kafka-pararams,TopicName)  (Key value)
           Step 7: kafkaUtils---getting value from above step--stream data
           Step 8: print Stream
           Step 9: start Stream

           eg:

           import org.apache.kafka.clients.consumer.ConsumerRecord
           import org.apache.kafka.common.serialization.StringDeserializer
           import org.apache.spark.streaming.kafka010._
           import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
           import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

           val kafkaParams = Map[String, Object](
             "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
             "key.deserializer" -> classOf[StringDeserializer],
             "value.deserializer" -> classOf[StringDeserializer],
             "group.id" -> "use_a_separate_group_id_for_each_stream",
             "auto.offset.reset" -> "latest",
             "enable.auto.commit" -> (false: java.lang.Boolean)
           )

           val topics = Array("topicA", "topicB")
           val stream = KafkaUtils.createDirectStream[String, String](
             streamingContext,
             PreferConsistent,
             Subscribe[String, String](topics, kafkaParams)
           )

           stream.map(record => (record.key, record.value))

Structured Streaming
====================

        When compare to rdd streaming , Structured is bit different

        Follow Step 1 to Step 6 from RDD Streaming

        We need to Add one more dependency in build.sbt
       "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.8"

       Default Schema for structured from Kafka
           kafkaStreamDF.printSchema()

           Column	            Type
           key	                binary
           value	              binary
           topic	              string
           partition	          int
           offset	            long
           timestamp	          timestamp
           timestampType	      int
           headers (optional)	array

        While writing structured streaming ,
        Whenever we make change write path, at same time make sure to change checkpoint path as well or else will throw error

Nifi with HDFS 
==============

Goal: Want to read file from HDFS and write it in Different Hdfs path

Adding 2 Processors
===================
1.GetHDFS
2.PutHDFS
3.Configure it with HDFS Read & Write Path
4.******** Dont forget to add Hadoop config**********--------->core-site.xml

 ![GetHDFStoPutHDFS_Nifi](https://github.com/IlayaBharathi260199/IlayaBharathi260199/assets/151670523/63ba74b3-fb6c-4adf-b4ed-e657940fc901)


![GetHDFS](https://github.com/IlayaBharathi260199/IlayaBharathi260199/assets/151670523/e56dc006-5069-4731-86c7-d1fc096614b6)


![puthdfs](https://github.com/IlayaBharathi260199/IlayaBharathi260199/assets/151670523/e87a248a-0801-4919-9d2e-4fc705e6bc7a)












