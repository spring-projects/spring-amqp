/*
 * Copyright 2016-2024 the original author or authors.
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

package org.springframework.amqp.support.converter;

import org.springframework.amqp.AmqpRemoteException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.util.Assert;

/**
 * A delegating adapter that unwraps {@link RemoteInvocationResult} after invoking
 * the delegate to convert from a message.
 * Delegates to a {@link SimpleMessageConverter} by default.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
public class RemoteInvocationAwareMessageConverterAdapter implements MessageConverter {

	private final MessageConverter delegate;

	public RemoteInvocationAwareMessageConverterAdapter() {
		this.delegate = new SimpleMessageConverter();
	}

	public RemoteInvocationAwareMessageConverterAdapter(MessageConverter delegate) {
		Assert.notNull(delegate, "'delegate' converter cannot be null");
		this.delegate = delegate;
	}

	@Override
	public Message toMessage(Object object, MessageProperties messageProperties) throws MessageConversionException {
		return this.delegate.toMessage(object, messageProperties);
	}

	@Override
	public Object fromMessage(Message message) throws MessageConversionException {
		Object result = this.delegate.fromMessage(message);
		if (result instanceof RemoteInvocationResult remoteInvocationResult) {
			try {
				result = remoteInvocationResult.recreate();
				if (result == null) {
					throw new MessageConversionException("RemoteInvocationResult returned null");
				}
			}
			catch (Throwable e) { // NOSONAR
				throw new AmqpRemoteException(e);
			}
		}
		return result;
	}

}
