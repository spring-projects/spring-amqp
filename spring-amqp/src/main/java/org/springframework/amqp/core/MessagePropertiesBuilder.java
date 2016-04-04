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
		super();
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
