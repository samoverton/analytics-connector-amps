package com.acunu.analytics.amps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.crankuptheamps.client.exception.ConnectionException;
import com.crankuptheamps.client.exception.DisconnectedException;
import com.crankuptheamps.client.exception.StoreException;

public class AmpsPublisher {
	private static Logger logger = LoggerFactory.getLogger(AmpsPublisher.class);
	protected AmpsClient client;
	private String url;
	
    public AmpsPublisher(String url) {
    	this.client = new AmpsClient();
    	this.url = url;
    }

    public void initialize() throws ConnectionException, StoreException {
		client.connect(url);
	}
	
    public void publish(String topic, String message) throws DisconnectedException, StoreException {
		client.publish(topic, message);
	}
	
    public void shutdown() {
		client.shutdown();
	}
}
