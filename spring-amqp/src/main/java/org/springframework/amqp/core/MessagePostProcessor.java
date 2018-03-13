/*
 * Copyright 2002-2017 the original author or authors.
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

import org.springframework.amqp.AmqpException;

/**
 * Used in several places in the framework, such as
 * {@code AmqpTemplate#convertAndSend(Object, MessagePostProcessor)} where it can be used
 * to add/modify headers or properties after the message conversion has been performed. It
 * also can be used to modify inbound messages when receiving messages in listener
 * containers and {@code AmqpTemplate}s.
 *
 * <p>
 * It is a {@link FunctionalInterface} and is often used as a lambda:
 * <pre class="code">
 * amqpTemplate.convertAndSend(routingKey, m -&gt; {
 *     m.getMessageProperties().setDeliveryMode(DeliveryMode.NON_PERSISTENT);
 *     return m;
 * });
 * </pre>
 *
 * @author Mark Pollack
 * @author Gary Russell
 */
@FunctionalInterface
public interface MessagePostProcessor {

	/**
	 * Change (or replace) the message.
	 * @param message the message.
	 * @return the message.
	 * @throws AmqpException an exception.
	 */
	Message postProcessMessage(Message message) throws AmqpException;

	/**
	 * Change (or replace) the message and/or change its correlation data.
	 * @param message the message.
	 * @param correlation the correlation data.
	 * @return the message.
	 * @since 1.6.7
	 */
	default Message postProcessMessage(Message message, Correlation correlation) {
		return postProcessMessage(message);
	}

}
