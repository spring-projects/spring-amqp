/*
 * Copyright 2020-present the original author or authors.
 */

package org.springframework.amqp.core;

/**
 * Returned message and its metadata.
 *
 * @author Gary Russell
 * @since 2.3
 *
 */
public class ReturnedMessage {

	private final Message message;

	private final int replyCode;

	private final String replyText;

	private final String exchange;

	private final String routingKey;

	public ReturnedMessage(Message message, int replyCode, String replyText, String exchange, String routingKey) {
		this.message = message;
		this.replyCode = replyCode;
		this.replyText = replyText;
		this.exchange = exchange;
		this.routingKey = routingKey;
	}

	/**
	 * Get the message.
	 * @return the message.
	 */
	public Message getMessage() {
		return this.message;
	}

	/**
	 * Get the reply code.
	 * @return the reply code.
	 */
	public int getReplyCode() {
		return this.replyCode;
	}

	/**
	 * Get the reply text.
	 * @return the reply text.
	 */
	public String getReplyText() {
		return this.replyText;
	}

	/**
	 * Get the exchange.
	 * @return the exchange name.
	 */
	public String getExchange() {
		return this.exchange;
	}

	/**
	 * Get the routing key.
	 * @return the routing key.
	 */
	public String getRoutingKey() {
		return this.routingKey;
	}

	@Override
	public String toString() {
		return "ReturnedMessage [message=" + this.message + ", replyCode=" + this.replyCode + ", replyText="
				+ this.replyText + ", exchange=" + this.exchange + ", routingKey=" + this.routingKey + "]";
	}

}
