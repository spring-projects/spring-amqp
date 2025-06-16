/*
 * Copyright 2002-present the original author or authors.
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

package org.springframework.amqp.core;

import java.util.Map;

/**
 * Represents an anonymous, non-durable, exclusive, auto-delete queue. The name has the
 * form 'spring.gen-&lt;base64UUID&gt;' by default, but it can be modified by providing a
 * {@link NamingStrategy}. Two naming strategies {@link Base64UrlNamingStrategy} and
 * {@link UUIDNamingStrategy} are provided but you can implement your own. Names should be
 * unique.
 * @author Dave Syer
 * @author Gary Russell
 *
 */
public class AnonymousQueue extends Queue {

	/**
	 * Construct a queue with a Base64-based name.
	 */
	public AnonymousQueue() {
		this((Map<String, Object>) null);
	}

	/**
	 * Construct a queue with a Base64-based name with the supplied arguments.
	 * @param arguments the arguments.
	 */
	public AnonymousQueue(Map<String, Object> arguments) {
		this(org.springframework.amqp.core.Base64UrlNamingStrategy.DEFAULT, arguments);
	}

	/**
	 * Construct a queue with a name provided by the supplied naming strategy.
	 * @param namingStrategy the naming strategy.
	 * @since 2.1
	 */
	public AnonymousQueue(org.springframework.amqp.core.NamingStrategy namingStrategy) {
		this(namingStrategy, null);
	}

	/**
	 * Construct a queue with a name provided by the supplied naming strategy with the
	 * supplied arguments. By default, these queues will be created on the node the
	 * application is connected to.
	 * @param namingStrategy the naming strategy.
	 * @param arguments the arguments.
	 * @since 2.1
	 */
	public AnonymousQueue(org.springframework.amqp.core.NamingStrategy namingStrategy, Map<String, Object> arguments) {
		super(namingStrategy.generateName(), false, true, true, arguments);
		if (!getArguments().containsKey(X_QUEUE_LEADER_LOCATOR)) {
			setLeaderLocator("client-local");
		}
	}

}
