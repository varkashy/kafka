package com.varkashy.kafka.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.varkashy.kafka.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Twitter producer has 3 parts or process
 * 1. Create a twitter client
 * 2. Create a Kafka Producer
 * 3. Loop to send tweets to kafka
 */
public class TwitterProducer {
	private final Logger _log = LoggerFactory.getLogger(TwitterProducer.class);

	public TwitterProducer(){
		_log.debug("hello");
	}

	public static void main(String[] args) {
		new TwitterProducer().run();
	}

	public void run() {
		//Create twitter client
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
		BasicClient twitterClient=null;
		try {
			twitterClient = createTwitterClient(msgQueue);
			twitterClient.connect();
			// on a different thread, or multiple different threads....
			while (!twitterClient.isDone()) {
				String message = msgQueue.poll(5, TimeUnit.SECONDS);
				_log.info("message is " + message);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally{
			if(twitterClient!=null){
				twitterClient.stop();
				_log.info("Stopping the client");
			}
		}
	}

	private BasicClient createTwitterClient(final BlockingQueue<String> msgQueue){
		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		List<String> terms = Lists.newArrayList("india");
		hosebirdEndpoint.trackTerms(terms);
		Authentication hosebirdAuth = new OAuth1(PropertyUtil.properties.getProperty("consumerKey"),
				PropertyUtil.properties.getProperty("consumerSecret"),
				PropertyUtil.properties.getProperty("token"),
				PropertyUtil.properties.getProperty("secret"));

		ClientBuilder builder = new ClientBuilder()
				.name("Hosebird-Client-01")// this could be any name
				.hosts(hosebirdHosts)
				.authentication(hosebirdAuth)
				.endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		return builder.build();
	}
}
