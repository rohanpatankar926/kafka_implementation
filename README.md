# kafka_implementation

Apache Kafka is a publish-subscribe messaging queue used for real-time streams of data. Apache Kafka lets you send and receive messages between various Microservices. In this article, we will see how to send JSON messages using Python and Confluent-Kafka Library.JavaScript Object Notation (JSON) is a standard text-based format for representing structured data. It is a common data format with diverse uses in electronic data interchange, including that of web applications with servers.

Prerequisites:

Good knowledge of Kafka Basic Concepts (e.g. Kafka Topics, Brokers, Partitions, Offset, Producer, Consumer, etc).
Good knowledge of Python Basics (pip install <package>, writing python methods).
Solution :

Kafka Python Producer has different syntax and behaviors based on the Kafka Library we are using. So the First Step is choosing the Right Kafka Library for our Python Program.

Popular Kafka Libraries for Python:

While working on Kafka Automation with Python we have 3 popular choices of Libraries on the Internet:

```
PyKafka
Kafka-python
Confluent Kafka
```
 
Each of these Libraries has its own Pros and Cons So we will have to choose based on our Project Requirements.

Step 1: Choosing the right  Kafka Library
If we are using Amazon MSK clusters then We can build our Kafka Framework using PyKafka or Kafka-python (both are Open Source and most popular for Apache Kafka). If we are using Confluent Kafka clusters then We have to use Confluent Kafka Library as we will get Library support for Confluent specific features like ksqlDB, REST Proxy, and Schema Registry. 

We will use Confluent Kafka Library for Python Kafka Producer as we can handle both Apache Kafka cluster and Confluent Kafka cluster with this Library.

We need Python 3.x and Pip already installed. We can execute the below command to install the Library in our System.

`pip install confluent-kafka`
Step 2: Kafka Authentication Setup.
Unlike most of the Kafka Python Tutorials available on the Internet, We will not work on localhost. Instead, We will try to connect to the Remote Kafka cluster with SSL Authentication. In order to connect to Kafka clusters, Generally, We get 1 JKS File and one Password for this JKS file from the Infra Support Team. This JKS file works fine with Java/Spring but not with Python.

So our job is to convert this JKS file into the appropriate format (as expected by the Python Kafka Library).
For Confluent Kafka Library, We need to convert the JKS file into PKCS12 format in order to connect to remote Kafka clusters.

To learn more visit the below pages:

How to convert JKS to PKCS12?
How to receive messages using Confluent Kafka Python Consumer
Step 3: Confluent Kafka Python Producer with SSL Authentication.

We will use the same PKCS12 file that was generated during JKS to the PKCS conversion step mentioned above
