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

package org.springframework.amqp.rabbit.connection;

import org.springframework.amqp.AmqpException;

/**
 * An exception thrown if the connection is an auto recover connection that is not
 * currently open and is in the process of being recovered.
 * <p>
 * Spring AMQP has had its own recovery logic since day one. The amqp-client library now
 * supports automatic connection and topology recovery; and, since 4.0.0, it is enabled by
 * default. While Spring AMQP is compatible with the client recovery logic, it's generally
 * not necessary to use it; in fact, you may recover faster when relying on Spring AMQP's
 * recovery, especially on the producer side, when a
 * {@link org.springframework.amqp.rabbit.core.RabbitTemplate} has a
 * {@link org.springframework.retry.support.RetryTemplate}.
 * <p>
 * If you get this exception, consider disabling the client auto recovery. Spring AMQP
 * disables it by default, unless you configure the underlying rabbit connection factory
 * yourself.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
@SuppressWarnings("serial")
public class AutoRecoverConnectionNotCurrentlyOpenException extends AmqpException {

	AutoRecoverConnectionNotCurrentlyOpenException(String message) {
		super(message);
	}

}
