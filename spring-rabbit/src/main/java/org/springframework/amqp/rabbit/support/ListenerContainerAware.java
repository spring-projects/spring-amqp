/*
 * Copyright 2015-2016 the original author or authors.
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

package org.springframework.amqp.rabbit.support;

import java.util.Collection;

import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;

/**
 * {@link MessageListener}s and {@link ChannelAwareMessageListener}s that also implement this
 * interface can have configuration verified during initialization.
 *
 * @author Gary Russell
 * @since 1.5
 *
 */
@FunctionalInterface
public interface ListenerContainerAware {

	/**
	 * Return the queue names that the listener expects to listen to.
	 *
	 * @return the queue names.
	 */
	Collection<String> expectedQueueNames();

}
