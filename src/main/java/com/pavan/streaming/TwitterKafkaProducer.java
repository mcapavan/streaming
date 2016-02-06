package com.pavan.streaming;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterKafkaProducer {

//	private static final String topic = "twitter-topic-pavan";

	public static void run(String consumerKey, String consumerSecret,
			String token, String secret, String kafkaBrokerList, String twitterTopicName, String twitterTrackTerm) throws InterruptedException {

		Properties properties = new Properties();
//		properties.put("metadata.broker.list", "localhost:9092");
		properties.put("metadata.broker.list", kafkaBrokerList);
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
//		properties.put("client.id","camus");
		ProducerConfig producerConfig = new ProducerConfig(properties);
		kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(
				producerConfig);

		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		// add some track terms
//		endpoint.trackTerms(Lists.newArrayList("twitterapi", "echoOfecho", "terminator"));
		endpoint.trackTerms(Lists.newArrayList(twitterTrackTerm));

		Authentication auth = new OAuth1(consumerKey, consumerSecret, token,
				secret);

		// Create a new BasicClient. By default gzip is enabled.
		Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
				.endpoint(endpoint).authentication(auth)
				.processor(new StringDelimitedProcessor(queue)).build();

		// Establish a connection
		client.connect();

		// Do whatever needs to be done with messages
		for (int msgRead = 0; msgRead < 1000; msgRead++) {
			KeyedMessage<String, String> message = null;
			try {
				message = new KeyedMessage<String, String>(twitterTopicName, queue.take());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			producer.send(message);
		}
		producer.close();
		client.stop();

	}

	public static void main(String[] args) {
		try {
			TwitterKafkaProducer.run(args[0], args[1], args[2], args[3], args[4], args[5], args[6]);
			//TwitterKafkaProducer.run("BlOWut5cFs7F2xJL4wFgJIR00", "iJSEb1hH7ISjJZkjIVEjO4SUEfPwaQV4vHiVg0WodsZO63buSP", "2365498483-KQEjssBQrBclC9dF8oZkLNnOWqosfLIFWRjwCor", "39Ncxsgn6d9dov7ZN1kYKx1eMQe2zi3autgZyF7rR4q9h");
		} catch (InterruptedException e) {
			System.out.println(e);
		}
	}
}
