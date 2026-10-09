/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.core;

/**
 * Enumeration for the message delivery mode. Can be persistent or
 * non-persistent. Use the method 'toInt' to get the appropriate value
 * that is used by the AMQP protocol instead of the ordinal() value when
 * passing into AMQP APIs.
 *
 * @author Mark Pollack
 * @author Gary Russell
 * @author Artem Bilan
 *
 */
public enum MessageDeliveryMode {

	/**
	 * Non persistent.
	 */
	NON_PERSISTENT,

	/**
	 * Persistent.
	 */
	PERSISTENT;

	public static int toInt(MessageDeliveryMode mode) {
		return switch (mode) {
			case NON_PERSISTENT -> 1;
			case PERSISTENT -> 2;
		};
	}

	public static MessageDeliveryMode fromInt(int modeAsNumber) {
		return switch (modeAsNumber) {
			case 1 -> NON_PERSISTENT;
			case 2 -> PERSISTENT;
			default -> throw new IllegalArgumentException("Unknown mode: " + modeAsNumber);
		};
	}

}
