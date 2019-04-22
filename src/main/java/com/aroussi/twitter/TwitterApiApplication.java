package com.aroussi.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.PropertySources;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Log4j2
@PropertySource(value = "classpath:twitter-access.properties")
@SpringBootApplication
public class TwitterApiApplication {

	//public static final String CONSUMER_KEY = "GCn5BEhukvFSEfkpUoVxxYavY";
	//public static final String CONSUMER_SECRET = "wTKdLLuOVcKsPlT9PX7jSVqN7Zsa53pFDAO78RVolvFxYyfO4F";
	//public static final String TOKEN = "2485967425-iO41xYNhO3bv5gsY2HOERGohYm1eHye0S1XEQie";
	//public static final String TOKEN_SECRET = "X5JA072EiC76mUIWcAPspL8UprtECrNoASIdtNnVvgEd1";
	@Value("${TWITTER_CONSUMER_KEY}")
	private String twitterKey;
	@Value("${TWITTER_CONSUMER_SECRET}")
	private String twitterSecret;
	@Value("${TWITTER_TOKEN}")
	private String twitterToken;
	@Value("${TWITTER_SECRET}")
	private String twitterTokenSecret;

	public static void main(String[] args) {
		SpringApplication.run(TwitterApiApplication.class, args);
	}

	@Bean
	CommandLineRunner runner(KafkaTemplate<String, String> kafkaTemplate){
		return (args)->{
			/** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
			BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100);
			Client client= buildTwitterClient(msgQueue);
			client.connect();
			// on a different thread, or multiple different threads....
			while (!client.isDone()) {
				String msg = msgQueue.poll(5, TimeUnit.SECONDS);
				ProducerRecord<String, String> record= new ProducerRecord<>("twitter", msg);
				ListenableFuture<SendResult<String, String>> msgFuture = kafkaTemplate.send(record);
				msgFuture.addCallback(new ListenableFutureCallback<>() {
					@Override
					public void onFailure(Throwable ex) {
						log.error("----- {}", ex);
					}

					@Override
					public void onSuccess(SendResult<String, String> result) {
						log.info("----- msg sent to kafka {}", result);
					}
				});
				log.info("message = {}", msg);
			}
		};
	}
	public Client buildTwitterClient(BlockingQueue<String> msgQueue){


		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hostKafkaPoc = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		List<Long> followings = Lists.newArrayList(1234L, 566788L);
		List<String> terms = Lists.newArrayList("kafka", "spring");
		hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication kafkaPocAuth = new OAuth1(
				twitterKey,
				twitterSecret,
				twitterToken,
				twitterTokenSecret);
		ClientBuilder builder = new ClientBuilder()
				.name("kafka-poc-0-client-01")                              // optional: mainly for the logs
				.hosts(hostKafkaPoc)
				.authentication(kafkaPocAuth)
				.endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		return builder.build();
	}
}
