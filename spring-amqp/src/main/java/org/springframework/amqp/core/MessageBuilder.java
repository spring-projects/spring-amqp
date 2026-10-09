/*
 * Copyright 2014-present the original author or authors.
 */

package org.springframework.amqp.core;

import java.util.Arrays;

import org.springframework.util.Assert;

/**
 * Builds a Spring AMQP Message either from a byte[] body or
 * another Message using a fluent API.
 *
 * @author Gary Russell
 * @since 1.3
 *
 */
public final class MessageBuilder extends MessageBuilderSupport<Message> {

	private static final String MESSAGE_CANNOT_BE_NULL = "'message' cannot be null";

	private static final String BODY_CANNOT_BE_NULL = "'body' cannot be null";

	private final byte[] body;

	/**
	 * The final message body will be a direct reference to 'body'.
	 * @param body The body.
	 * @return The builder.
	 */
	public static MessageBuilder withBody(byte[] body) {
		Assert.notNull(body, BODY_CANNOT_BE_NULL);
		return new MessageBuilder(body);
	}

	/**
	 * The final message body will be a copy of 'body' in a new array.
	 * @param body The body.
	 * @return The builder.
	 */
	public static MessageBuilder withClonedBody(byte[] body) {
		Assert.notNull(body, BODY_CANNOT_BE_NULL);
		return new MessageBuilder(Arrays.copyOf(body, body.length));
	}

	/**
	 * The final message body will be a new array containing the byte range from
	 * 'body'.
	 * @param body The body.
	 * @param from The starting index.
	 * @param to The ending index.
	 * @return The builder.
	 *
	 * @see Arrays#copyOfRange(byte[], int, int)
	 */
	public static MessageBuilder withBody(byte[] body, int from, int to) {
		Assert.notNull(body, BODY_CANNOT_BE_NULL);
		return new MessageBuilder(Arrays.copyOfRange(body, from, to));
	}

	/**
	 * The final message body will be a direct reference to the message
	 * body, the MessageProperties will be a shallow copy.
	 * @param message The message.
	 * @return The builder.
	 */
	public static MessageBuilder fromMessage(Message message) {
		Assert.notNull(message, MESSAGE_CANNOT_BE_NULL);
		return new MessageBuilder(message);
	}

	/**
	 * The final message will have a copy of the message
	 * body, the MessageProperties will be cloned (top level only).
	 * @param message The message.
	 * @return The builder.
	 */
	public static MessageBuilder fromClonedMessage(Message message) {
		Assert.notNull(message, MESSAGE_CANNOT_BE_NULL);
		byte[] body = message.getBody();
		Assert.notNull(body, BODY_CANNOT_BE_NULL);
		return new MessageBuilder(Arrays.copyOf(body, body.length), message.getMessageProperties());
	}

	private MessageBuilder(byte[] body) { //NOSONAR
		this.body = body;
	}

	private MessageBuilder(Message message) {
		this(message.getBody(), message.getMessageProperties());
	}

	private MessageBuilder(byte[] body, MessageProperties properties) { //NOSONAR
		this.body = body;
		this.copyProperties(properties);
	}

	/**
	 * Makes this builder's properties builder use a reference to properties.
	 * @param properties The properties.
	 * @return this.
	 */
	public MessageBuilder andProperties(MessageProperties properties) {
		this.setProperties(properties);
		return this;
	}

	@Override
	public Message build() {
		return new Message(this.body, this.buildProperties());
	}

}
