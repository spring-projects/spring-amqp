/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.amqp.rabbit.listener.adapter;

/**
 * A Message Listener that returns a reply - intended for lambda use in a
 * {@link MessageListenerAdapter}.
 * @param <T> the request type.
 * @param <R> the reply type.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
@FunctionalInterface
public interface ReplyingMessageListener<T, R> {

	/**
	 * Handle the message and return a reply.
	 * @param t the request.
	 * @return the reply.
	 */
	R handleMessage(T t);

}
