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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException;

/**
 * MessageRecover that causes the listener container to reject
 * the message without requeuing. This enables failed messages
 * to be sent to a Dead Letter Exchange/Queue, if the broker is
 * so configured.
 *
 * @author Gary Russell
 * @since 1.1.2
 *
 */
public class RejectAndDontRequeueRecoverer implements MessageRecoverer {

	protected Log logger = LogFactory.getLog(RejectAndDontRequeueRecoverer.class);

	@Override
	public void recover(Message message, Throwable cause) {
		if (this.logger.isWarnEnabled()) {
			this.logger.warn("Retries exhausted for message " + message, cause);
		}
		throw new ListenerExecutionFailedException("Retry Policy Exhausted",
					new AmqpRejectAndDontRequeueException(cause), message);
	}

}
