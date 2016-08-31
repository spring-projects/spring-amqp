/*
 * Copyright 2002-2016 the original author or authors.
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

package org.springframework.amqp.rabbit.retry;

import org.springframework.amqp.core.Message;

/**
 * @author Dave Syer
 * @author Gary Russell
 *
 */
@FunctionalInterface
public interface MessageKeyGenerator {

	/**
	 * Generate a unique key for the message that is repeatable on redelivery. Implementations should be very careful
	 * about assuming uniqueness of any element of the message, especially considering the requirement that it be
	 * repeatable. A message id is ideal, but may not be present (AMQP does not mandate it), and the message body is a
	 * byte array whose contents might be repeatable, but its object value is not.
	 *
	 * @param message the message to generate a key for
	 * @return a unique key for this message
	 */
	Object getKey(Message message);

}
