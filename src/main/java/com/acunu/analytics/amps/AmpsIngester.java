package com.acunu.analytics.amps;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acunu.analytics.conf.ConfigProperties;
import com.acunu.analytics.Context;
import com.acunu.analytics.Flow;
import com.acunu.analytics.ingest.AbstractIngester;
import com.acunu.analytics.ingest.FlowSource;
import com.acunu.analytics.ingest.IngestException;
import com.crankuptheamps.client.CommandId;
import com.crankuptheamps.client.MessageHandler;
import com.crankuptheamps.client.exception.AMPSException;

/**
 * Ingester for the AMPS message queue.
 * 
 * @author tmoreton
 * 
 */
public class AmpsIngester extends AbstractIngester {

	private static Logger logger = LoggerFactory.getLogger(AmpsIngester.class);

	/**
	 * AMPS client url for this flow
	 */
	protected String url;

	/**
	 * Client wrapper
	 */
	protected AmpsClient client;


	/**
	 * Init the AMPS ingester for the given server URL.
	 */
	public AmpsIngester(String name, Context context) throws IngestException {
		super(name, context);
		Object url = config.get("url");
		if (!(url instanceof String))
			throw new RuntimeException("Must specify valid AMPS server URL in the 'url' ingester property");
		
		this.url = (String) url;
		this.client = new AmpsClient();
		try {
			client.connect(this.url);
		} catch (Exception e) {
			throw new RuntimeException("Could not connect to AMPS server", e);
		}
	}

	/**
	 * Shut down the ingester.
	 */
	@Override
	public void shutdown() {
		super.shutdown();

		if (this.client!=null) {
			this.client.shutdown();
		}
	}

	@Override
	protected FlowSource<? extends AbstractIngester> createFlowSource(Flow flow) throws IngestException {
		return new AmpsFlowSource(this, flow);
	}

	/**
	 * A single flow of data from an AMPS server on a particular topic.
	 */
	public static class AmpsFlowSource extends FlowSource<AmpsIngester> {

		protected AmpsFlowSource(AmpsIngester ingester, Flow flow) throws IngestException {
			super(ingester, flow);
			topic = flow.getProperties().getString("topic");
			if (topic == null) {
				throw new IngestException("Missing parameter 'topic' for Flow on AmpsIngester");
			}
		}

		/**
		 * AMPS topic name for this flow. Can be a regex.
		 */
		protected final String topic;
		private CommandId commandId;

		/**
		 * Override the main ingest loop so we can use AMPS "push" interface.
		 */
		@Override
		protected void ingestLoop() throws IngestException, InterruptedException {
			try {
				commandId = ingester.client.subscribe(new MessageHandler() {
					@Override
					public void invoke(com.crankuptheamps.client.Message message) {
						try {
							if (logger.isDebugEnabled()) {
								logger.info("Received message on topic {}, sending to flow {}",
									message.getTopic(), flow.getName());
							}
							// receive message string from AMPS messaging system
							String messageStr = message.getData();
							ingester.enqueueEventsForFlow(flow, Collections.singletonList(messageStr));
						} catch (InterruptedException e) {
							// do nothing
						}
					}
				}, topic, 10000); // subscribe to topic on AMPS message bus

				logger.warn("Waiting for messages");
				while (true) {
					Thread.sleep(60000);
				}
			} catch (AMPSException exn) {
				logger.error("Error reading from AMPS", exn);
				throw new IngestException(exn);
			}
		}

		@Override
		protected List<?> ingestSomeMore() throws IngestException, InterruptedException {
			throw new UnsupportedOperationException();
		}

		@Override
		public void stop() {
			ingester.client.unsubscribe(commandId);
		}
	};
}
