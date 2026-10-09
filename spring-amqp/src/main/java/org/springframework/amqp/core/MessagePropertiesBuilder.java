/*
 * Copyright 2014-present the original author or authors.
 */

package org.springframework.amqp.core;

/**
 * Builds a Spring AMQP MessageProperties object using a fluent API.
 *
 * @author Gary Russell
 * @since 1.3
 *
 */
public final class MessagePropertiesBuilder extends MessageBuilderSupport<MessageProperties> {

	/**
	 * Returns a builder with an initial set of properties.
	 * @return The builder.
	 */
	public static MessagePropertiesBuilder newInstance() {
		return new MessagePropertiesBuilder();
	}

	/**
	 * Initializes the builder with the supplied properties; the same
	 * object will be returned by {@link #build()}.
	 * @param properties The properties.
	 * @return The builder.
	 */
	public static MessagePropertiesBuilder fromProperties(MessageProperties properties) {
		return new MessagePropertiesBuilder(properties);
	}

	/**
	 * Performs a shallow copy of the properties for the initial value.
	 * @param properties The properties.
	 * @return The builder.
	 */
	public static MessagePropertiesBuilder fromClonedProperties(MessageProperties properties) {
		MessagePropertiesBuilder builder = newInstance();
		return builder.copyProperties(properties);
	}

	private MessagePropertiesBuilder() {
	}

	private MessagePropertiesBuilder(MessageProperties properties) {
		super(properties);
	}

	@Override
	public MessagePropertiesBuilder copyProperties(MessageProperties properties) {
		super.copyProperties(properties);
		return this;
	}

	@Override
	public MessageProperties build() {
		return this.buildProperties();
	}

}
