/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.amqp.rabbit.support;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Envelope;

/**
 * Encapsulates an arbitrary message - simple "bean" holder structure.
 *
 * @author Gary Russell
 * @since 2.0
 */
public class Delivery {

	private final String consumerTag;

	private final Envelope envelope;

	private final String queue;

	private final AMQP.BasicProperties properties;

	private final byte[] body;

	public Delivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body, // NOSONAR
			String queue) {
		this.consumerTag = consumerTag;
		this.envelope = envelope;
		this.properties = properties;
		this.body = body; // NOSONAR
		this.queue = queue;
	}

	/**
	 * Retrieve the consumer tag.
	 * @return the consumer tag.
	 */
	public String getConsumerTag() {
		return this.consumerTag;
	}

	/**
	 * Retrieve the message envelope.
	 * @return the message envelope.
	 */
	public Envelope getEnvelope() {
		return this.envelope;
	}

	/**
	 * Retrieve the message properties.
	 * @return the message properties.
	 */
	public BasicProperties getProperties() {
		return this.properties;
	}

	/**
	 * Retrieve the message body.
	 * @return the message body.
	 */
	public byte[] getBody() {
		return this.body; // NOSONAR
	}

	/**
	 * Retrieve the queue.
	 * @return the queue.
	 */
	public String getQueue() {
		return this.queue;
	}

}
