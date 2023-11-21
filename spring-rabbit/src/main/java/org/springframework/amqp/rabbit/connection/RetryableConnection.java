/*
 * Copyright 2002-2023 the original author or authors.
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

package org.springframework.amqp.rabbit.connection;

import org.springframework.amqp.AmqpResourceNotAvailableException;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.support.RetryTemplate;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.springframework.util.Assert;

/**
 *
 * extend from {@link SimpleConnection} provide retry ability
 * @author salkli
 * @since 3.1
 */
public class RetryableConnection extends SimpleConnection {

	private RetryTemplate retryTemplate;

	public RetryableConnection(Connection delegate, int closeTimeout, RetryTemplate retryTemplate) {
		super(delegate, closeTimeout);
		Assert.notNull(retryTemplate,"retryTemplate can not be null.");
		this.retryTemplate = retryTemplate;
	}

	@Override
	public Channel createChannel(boolean transactional) {
		try {
			Channel channel = retryTemplate.execute((RetryCallback<Channel, Exception>) context -> {
				Channel channel0 = super.getDelegate().createChannel();
				if (channel0 == null) {
					throw new AmqpResourceNotAvailableException("The channelMax limit is reached. Try later.");
				}
				return channel0;
			});
			if (transactional) {
				channel.txSelect();
			}
			return channel;

		}
		catch (Exception e) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(e);
		}
	}
}
