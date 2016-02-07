#!/bin/bash

# The following 7 parameters are required to run TwitterKafkaProducer. 
#String consumerKey, String consumerSecret, String token, String secret, String kafkaBrokerList, String twitterTopicName, String twitterTrackTerm

java -cp ~/streaming/target/streaming-0.0.1-SNAPSHOT.jar com.pavan.streaming.TwitterKafkaProducer \
"BlOWut5cFs7F2xJL4wFgJIR00", "iJSEb1hH7ISjJZkjIVEjO4SUEfPwaQV4vHiVg0WodsZO63buSP", \
"2365498483-KQEjssBQrBclC9dF8oZkLNnOWqosfLIFWRjwCor", "39Ncxsgn6d9dov7ZN1kYKx1eMQe2zi3autgZyF7rR4q9h", \
"sandbox.hortonworks.com:6667", "twitter-topic-pavan", "pavan"