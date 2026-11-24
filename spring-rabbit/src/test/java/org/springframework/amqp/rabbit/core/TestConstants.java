/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
