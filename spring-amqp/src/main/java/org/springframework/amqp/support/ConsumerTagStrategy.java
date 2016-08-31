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

package org.springframework.amqp.support;

/**
 * A strategy interface to determine the consumer tag to be used when issuing a
 * {@code basicConsume} operation.
 *
 * @author Gary Russell
 * @since 1.4.5
 *
 */
@FunctionalInterface
public interface ConsumerTagStrategy {

	/**
	 * Create the consumer tag, optionally based on the queue name that the consumer
	 * will listen to. Consumer tags must be unique.
	 * @param queue The queue name that this consumer will listen to.
	 * @return The consumer tag.
	 */
	String createConsumerTag(String queue);

}
