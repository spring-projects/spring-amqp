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

import java.io.IOException;

import org.springframework.amqp.AmqpResourceNotAvailableException;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.util.Assert;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 *
 * extend from {@link SimpleConnection} provide retry ability
 * @author salkli
 * @date 2023/11/13
 */
public class RetryableConnection extends SimpleConnection {

	private final com.rabbitmq.client.Connection delegate;

	private int createTimeOut;

	private int createTryTimes;

	public RetryableConnection(Connection delegate, int closeTimeout, int createTimeOut, int createTryTimes) {
		super(delegate, closeTimeout);
		Assert.isTrue(createTimeOut >= 1, "channel create wait timeout must be 1 or higher");
		Assert.isTrue(createTryTimes >= 1, "channel create retry times must be 1 or higher");
		this.delegate = delegate;
		this.createTimeOut = createTimeOut;
		this.createTryTimes = createTryTimes;
	}

	@Override
	public Channel createChannel(boolean transactional) {
		try {
			Channel channel = null;
			for (int i = 0; i < this.createTryTimes; i++) {
				channel = this.delegate.createChannel();
				if (this.createTimeOut > 0) {
					try {
						Thread.sleep(createTimeOut * 1000);
					}
					catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				}
				if (channel != null) {
					break;
				}
			}
			if (channel == null) {
				throw new AmqpResourceNotAvailableException("The channelMax limit is reached. Try later.");
			}
			if (transactional) {
				// Just created so we want to start the transaction
				channel.txSelect();
			}
			return channel;
		}
		catch (IOException e) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(e);
		}
	}
}
