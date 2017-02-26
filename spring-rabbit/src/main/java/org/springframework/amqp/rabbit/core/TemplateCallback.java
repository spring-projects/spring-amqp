/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.amqp.rabbit.core;

import com.rabbitmq.client.Channel;

/**
 * Callback for using the same channel for multiple RabbitTemplate
 * operations.
 * @param <T> the type the callback returns.
 *
 * @author Gary Russell
 * @since 2.0
 */
@FunctionalInterface
public interface TemplateCallback<T> {

	/**
	 * Execute any number of operations using a dedicated {@link Channel} as long as those
	 * operations are performed on the template argument and on the calling thread. The
	 * channel will be physically closed when the callback exits.
	 *
	 * @param template The template.
	 * @return The result.
	 */
	T doInRabbit(RabbitTemplate template);

}
