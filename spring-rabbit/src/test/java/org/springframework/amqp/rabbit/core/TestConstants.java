/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.core;

/**
 * Exchange, queue, and routing key constants for the testing code.
 */
public final class TestConstants {

	public static String EXCHANGE_NAME = "";

	public static String QUEUE_NAME = "foo";

	public static String ROUTING_KEY = "foo";

	public static int NUM_MESSAGES = 500;

	private TestConstants() {
	}

}
