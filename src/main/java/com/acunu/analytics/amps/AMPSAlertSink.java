package com.acunu.analytics.amps;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acunu.analytics.Context;
import com.acunu.analytics.alerts.AbstractAlertSink;
import com.acunu.analytics.alerts.AlertMonitorConfig;
import com.acunu.analytics.alerts.AlertServer;
import com.acunu.analytics.alerts.Parameters;
import com.acunu.util.Pair;
import com.crankuptheamps.client.exception.ConnectionException;
import com.crankuptheamps.client.exception.StoreException;

public class AMPSAlertSink extends AbstractAlertSink {
	public static final String PARAM_AMPS_URL = "amps_url";
	public static final String DEFAULT_AMPS_URL = "tcp://localhost:9004/nvfix";
	private static final Logger logger = LoggerFactory.getLogger(AMPSAlertSink.class);

	protected String ampsURL = null;
	protected AmpsPublisher publisher = null;

	public AMPSAlertSink(String name, Context context) {
		super(name, context);
		ampsURL = getConfig().getString(PARAM_AMPS_URL, DEFAULT_AMPS_URL);
		if (ampsURL == null)
			throw new IllegalArgumentException("Cannot create AMPS alert sink without property '" + PARAM_AMPS_URL + "'");
	}

	@Override
	public void init() throws IOException {
		super.init();
		publisher = new AmpsPublisher(ampsURL);
		try {
			publisher.initialize();
		} catch (StoreException exn) {
			throw new IOException(exn);
		} catch (ConnectionException exn) {
			throw new IOException(exn);
		}
	}

	// TODO support multiple AMPS clients here
	
	@Override
	public void sendAlert(String destination, String body, Map<String, Object> varMap) throws IOException {
		assert destination == ampsURL;

		if (destination == null)
			throw new IllegalArgumentException("Cannot send alert to null destination -- expecting parameter '" + AlertServer.VAR_ALERT_DESTINATION
					+ "'");

		InetSocketAddress address = null;
		final URL url = new URL(ampsURL);

		// We potentially need to add this host to the list of hosts we are
		// checking for availability.
		// Since the address may have been generated dynamically
		address = new InetSocketAddress(url.getHost(), url.getPort());
		if (!hostUp.containsKey(address)) {
			hostUp.put(address, true);
		}

		AlertDeliveryTask task = new AMPSAlertDeliveryTask(ampsURL, body, address, varMap);
		alertDeliveryPool.submit(task);
	}

	/**
	 * Task to asynchronously submit alerts.
	 * 
	 * @author abyde
	 */
	class AMPSAlertDeliveryTask extends AlertDeliveryTask {

		public AMPSAlertDeliveryTask(String topic, String body, InetSocketAddress address, Map<String, Object> alertVars) {
			super(address, alertVars);
			this.topic = topic;
			this.body = body;
		}

		protected final String topic;

		protected final String body;

		protected Pair<Exception,Boolean> trySubmissionAndReportAlert() {
			logger.info("ALERT PUBLISH: topic = {}, message = {}", topic, body);
			// publish to amps
			try {
				publisher.publish(topic, body);
				logger.debug("Successfully submitted alert to {}", topic);
				return null;
			}
			catch (StoreException e) {
				return new Pair<Exception,Boolean>(e, false);
			}
			catch (Exception e) {
				return new Pair<Exception,Boolean>(e, true);
			}
		}
	}

	@Override
	public void shutdown() throws IOException {
		super.shutdown();
	}

}
