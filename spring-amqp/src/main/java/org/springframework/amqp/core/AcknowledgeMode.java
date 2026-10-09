/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.core;

/**
 * Acknowledgment modes supported by the listener container.
 *
 * @author Dave Syer
 * @author Gary Russell
 *
 */
public enum AcknowledgeMode {

	/**
	 * No acks - {@code autoAck=true} in {@code Channel.basicConsume()}.
	 */
	NONE,

	/**
	 * Manual acks - user must ack/nack via a channel aware listener.
	 */
	MANUAL,

	/**
	 * Auto - the container will issue the ack/nack based on whether
	 * the listener returns normally, or throws an exception.
	 * <p><em>Do not confuse with RabbitMQ {@code autoAck} which is
	 * represented by {@link #NONE} here</em>.
	 */
	AUTO;

	/**
	 * Return if transactions are allowed - if the mode is {@link #AUTO} or
	 * {@link #MANUAL}.
	 * @return true if transactions are allowed.
	 */
	public boolean isTransactionAllowed() {
		return this == AUTO || this == MANUAL;
	}

	/**
	 * Return if the mode is {@link #NONE} (which is called {@code autoAck}
	 * in RabbitMQ).
	 * @return true if the mode is {@link #NONE}.
	 */
	public boolean isAutoAck() {
		return this == NONE;
	}

	/**
	 * Return true if the mode is {@link #MANUAL}.
	 * @return true if manual.
	 */
	public boolean isManual() {
		return this == MANUAL;
	}

}
