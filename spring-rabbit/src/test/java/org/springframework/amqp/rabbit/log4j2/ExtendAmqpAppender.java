/*
 * Copyright 2020-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.log4j2;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;

import org.springframework.amqp.core.Message;

/**
 * Extended Class for {@link AmqpAppender}
 * @author Francesco Scipioni
 *
 * @since 2.2.4
 */
@Plugin(name = "TestRabbitMQ", category = "Core", elementType = "appender", printObject = true)
public class ExtendAmqpAppender extends AmqpAppender {

	private final String foo;
	private final String bar;

	public ExtendAmqpAppender(String name, Filter filter, Layout<? extends Serializable> layout,
			boolean ignoreExceptions, AmqpManager manager, BlockingQueue<Event> eventQueue, String foo, String bar) {
		super(name, filter, layout, ignoreExceptions, Property.EMPTY_ARRAY, manager, eventQueue);
		this.foo = foo;
		this.bar = bar;
	}

	@Override
	protected Message postProcessMessageBeforeSend(Message message, Event event) {
		message.getMessageProperties().setHeader(this.foo, this.bar);
		return message;
	}

	@PluginBuilderFactory
	public static Builder newBuilder() {
		return new ExtendBuilder();
	}

	protected static class ExtendBuilder extends Builder {

		@PluginBuilderAttribute("foo")
		private String foo = "defaultFoo";

		@PluginBuilderAttribute("bar")
		private String bar = "defaultBar";

		public Builder setFoo(String foo) {
			this.foo = foo;
			return this;
		}

		public Builder setBar(String bar) {
			this.bar = bar;
			return this;
		}

		@Override
		protected AmqpAppender buildInstance(String name, Filter filter, Layout<? extends Serializable> layout,
				boolean ignoreExceptions, AmqpManager manager, BlockingQueue<Event> eventQueue) {
			return new ExtendAmqpAppender(name, filter, layout, ignoreExceptions, manager, eventQueue, this.foo, this.bar);
		}
	}

}
