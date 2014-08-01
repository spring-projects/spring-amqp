/*
 * Copyright 2002-2014 the original author or authors.
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

package org.springframework.amqp.rabbit.config;


import org.springframework.amqp.rabbit.listener.MessageListenerContainer;

/**
 * Factory of {@link MessageListenerContainer} based on a
 * {@link RabbitListenerEndpoint} definition.
 *
 * @author Stephane Nicoll
 * @since 2.0
 * @see RabbitListenerEndpoint
 */
public interface RabbitListenerContainerFactory<C extends MessageListenerContainer> {

	/**
	 * Create a {@link MessageListenerContainer} for the given {@link RabbitListenerEndpoint}.
	 * @param endpoint the endpoint to configure
	 * @return the created container
	 */
	C createListenerContainer(RabbitListenerEndpoint endpoint);

}
