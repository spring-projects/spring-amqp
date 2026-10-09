/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.junit;

/**
 * Global convenience class for all integration tests, carrying constants and other utilities for broker set up.
 *
 * @author Dave Syer
 * @author Gary Russell
 *
 */
public final class BrokerTestUtils {

	public static final int DEFAULT_PORT = 5672;

	private BrokerTestUtils() {
	}

	/**
	 * The port that the broker is listening on (e.g. as input for a
	 * {@link com.rabbitmq.client.ConnectionFactory}).
	 *
	 * @return a port number
	 */
	public static int getPort() {
		return DEFAULT_PORT;
	}

}
