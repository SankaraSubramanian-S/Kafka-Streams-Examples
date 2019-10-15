
1. Start Zookeeper by executing start-zookeeper.cmd file in scripts folder

2. Start Kafka Server by executing start-kafka-server.cmd file in scripts folder

3. Create input topic(stockdata) by executing create-topic.cmd file in scripts folder
  ![alt text](https://github.com/SankaraSubramanian-S/Kafka-Streams-Examples/blob/master/images/Input_Topic_Creation.jpg)

4. Create output topic(highest-traded-stock) by executing create-output-topic.cmd file in scripts folder
  ![alt text](https://github.com/SankaraSubramanian-S/Kafka-Streams-Examples/blob/master/images/Output_Topic_Creation.JPG)

5. Exeute ProducerApp.java which takes the following program arguments
   !!!TopicName!!! !!!List of InputFileNames separated by comma!!!
   Ex: stockdata C:/Projects/Kafka/KafkaRequirementPOC/src/main/resources/data/cm07JAN2019bhav.csv C:/Projects/Kafka/KafkaRequirementPOC/src/main/resources/data/cm08JAN2019bhav.csv C:/Projects/Kafka/KafkaRequirementPOC/src/main/resources/data/cm09JAN2019bhav.csv





