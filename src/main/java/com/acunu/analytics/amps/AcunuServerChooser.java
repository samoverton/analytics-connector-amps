package com.acunu.analytics.amps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.crankuptheamps.client.ConnectionInfo;
import com.crankuptheamps.client.DefaultServerChooser;

/**
 * A ServerChooser that logs errors.
 * @author thong
 */
public class AcunuServerChooser extends DefaultServerChooser {
    private static Logger logger = LoggerFactory.getLogger(AcunuServerChooser.class);

	public AcunuServerChooser() {
		super();
	}

	@Override
	public void reportFailure(Exception exception, ConnectionInfo info) {
		logger.warn("Error connecting to {}", info, exception);
		super.reportFailure(exception, info);
	}
}
