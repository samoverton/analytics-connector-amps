package com.acunu.analytics.amps;

import java.beans.ExceptionListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.crankuptheamps.client.Client;
import com.crankuptheamps.client.ClientDisconnectHandler;
import com.crankuptheamps.client.CommandId;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.MessageHandler;
import com.crankuptheamps.client.exception.AMPSException;
import com.crankuptheamps.client.exception.ConnectionException;
import com.crankuptheamps.client.exception.DisconnectedException;
import com.crankuptheamps.client.exception.StoreException;

public class AmpsClient {
	private static Logger logger = LoggerFactory.getLogger(AmpsClient.class);
    
	public static String APPLICATION_NAME = "acunu";
	public static final int RECONNECT_INTERVAL = 5000;

    protected HAClient amps;

    /**
     * Open connection to amps.
     */
    public void connect(String url) throws ConnectionException, StoreException {
		// open AMPS connection
		logger.warn("Connecting to AMPS at {}", url);
		amps = HAClient.createMemoryBacked(APPLICATION_NAME);
		amps.setExceptionListener(new ExceptionListener() {
			public void exceptionThrown(Exception ex) {
				logger.error("AMPS exception", ex);
			}
		});
		amps.setDisconnectHandler(new AmpsDisconnectHandler(url));
		amps.setReconnectDelay(RECONNECT_INTERVAL);
		AcunuServerChooser chooser = new AcunuServerChooser();
		chooser.add(url);
		amps.setServerChooser(chooser);
		amps.connectAndLogon();
    }

    class AmpsDisconnectHandler implements ClientDisconnectHandler
    {
    	private final String url;
		public AmpsDisconnectHandler(String url) {
    		this.url = url;
    	}

    	public void invoke(Client client) {
    		try{
    			logger.info("Disconnected, retrying...");
    			client.connect(url);
    			Thread.sleep(5000);
    		} catch(Exception e){
    		}
    	}
    }
    
    /**
     * Shut down the connection
     */
    public void shutdown() {
		logger.warn("AMPS shutting down");
		if (amps != null)
			amps.close();
    }

	public CommandId subscribe(MessageHandler messageHandler, String topic, int timeout) throws AMPSException {
		return amps.subscribe(messageHandler, topic, timeout);
	}

	public void sowAndSubscribe(MessageHandler messageHandler, String topic, String filter, int batch, boolean oofEnabled, int timeout) throws AMPSException {
		amps.sowAndSubscribe(messageHandler, topic, filter, batch, oofEnabled, timeout);
	}

	public void publish(String topic, String message) throws DisconnectedException, StoreException {
		amps.publish(topic, message);
	}

	public void unsubscribe(CommandId commandId) {
		try {
			amps.unsubscribe(commandId);
		} catch (DisconnectedException ignored) {

		}
	}
}
