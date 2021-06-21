/*
 * Copyright 2021 the original author or authors.
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

package org.springframework.rabbit.stream.support;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.lang.Nullable;

import com.rabbitmq.stream.MessageHandler.Context;

/**
 * {@link MessageProperties} extension for stream messages.
 *
 * @author Gary Russell
 * @since 2.4
 *
 */
public class StreamMessageProperties extends MessageProperties {

	private static final long serialVersionUID = 1L;

	private transient Context context;

	/**
	 * Create a new instance with the provided context.
	 * @param context the context.
	 */
	public StreamMessageProperties(@Nullable Context context) {
		this.context = context;
	}

	/**
	 * Return the stream {@link Context} for the message.
	 * @return the context.
	 */
	@Nullable
	public Context getContext() {
		return this.context;
	}

}
