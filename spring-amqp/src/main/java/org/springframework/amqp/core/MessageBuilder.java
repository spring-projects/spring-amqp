/*
 * Copyright 2014-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

	private final byte[] body;

	/**
	 * The final message body will be a direct reference to 'body'.
	 * @param body The body.
	 * @return The builder.
	 */
	public static MessageBuilder withBody(byte[] body) {
		Assert.notNull(body, "'body' cannot be null");
		return new MessageBuilder(body);
	}

	/**
	 * The final message body will be a copy of 'body' in a new array.
	 * @param body The body.
	 * @return The builder.
	 */
	public static MessageBuilder withClonedBody(byte[] body) {
		Assert.notNull(body, "'body' cannot be null");
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
		Assert.notNull(body, "'body' cannot be null");
		return new MessageBuilder(Arrays.copyOfRange(body, from, to));
	}

	/**
	 * The final message body will be a direct reference to the message
	 * body, the MessageProperties will be a shallow copy.
	 * @param message The message.
	 * @return The builder.
	 */
	public static MessageBuilder fromMessage(Message message) {
		Assert.notNull(message, "'message' cannot be null");
		return new MessageBuilder(message);
	}

	/**
	 * The final message will have a copy of the message
	 * body, the MessageProperties will be cloned (top level only).
	 * @param message The message.
	 * @return The builder.
	 */
	public static MessageBuilder fromClonedMessage(Message message) {
		Assert.notNull(message, "'message' cannot be null");
		byte[] body = message.getBody();
		Assert.notNull(body, "'body' cannot be null");
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
