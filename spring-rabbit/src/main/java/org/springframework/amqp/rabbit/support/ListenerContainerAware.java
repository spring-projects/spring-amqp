/*
 * Copyright 2015-present the original author or authors.
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

package org.springframework.amqp.rabbit.support;

import java.util.Collection;

import org.jspecify.annotations.Nullable;

/**
 * {@link org.springframework.amqp.core.MessageListener}s that also implement this
 * interface can have configuration verified during initialization.
 *
 * @author Gary Russell
 * @author Jeongjun Min
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
	@Nullable
	Collection<String> expectedQueueNames();

	/**
	 * Return a counter for pending replies, if any.
	 * @return the counter, or null.
	 * @since 4.0
	 */
	default @Nullable ActiveObjectCounter<Object> getPendingReplyCounter() {
		return null;
	}
}
