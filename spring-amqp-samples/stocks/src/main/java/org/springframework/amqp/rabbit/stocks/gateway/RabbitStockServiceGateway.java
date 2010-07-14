/*
 * Copyright 2002-2010 the original author or authors.
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

package org.springframework.amqp.rabbit.stocks.gateway;

import java.io.UnsupportedEncodingException;
import java.util.UUID;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.support.RabbitGatewaySupport;
import org.springframework.amqp.rabbit.stocks.domain.TradeRequest;

/**
 * Rabbit implementation of {@link StockServiceGateway} to send trade requests to an external process.
 * 
 * @author Mark Pollack
 */
public class RabbitStockServiceGateway extends RabbitGatewaySupport implements StockServiceGateway {

	private String defaultReplyToQueue;
	
	public void setDefaultReplyToQueue(String defaultReplyToQueue) {
		this.defaultReplyToQueue = defaultReplyToQueue;
	}
	
	public void setDefaultReplyToQueue(Queue defaultReplyToQueue) {
		this.defaultReplyToQueue = defaultReplyToQueue.getName();
	}

	public void send(TradeRequest tradeRequest) {
		getRabbitTemplate().convertAndSend(tradeRequest, new MessagePostProcessor() {
			public Message postProcessMessage(Message message) throws AmqpException {
				message.getMessageProperties().setReplyTo(new Address(defaultReplyToQueue));
				try {
					message.getMessageProperties().setCorrelationId(UUID.randomUUID().toString().getBytes("UTF-8"));
				}
				catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
				return message;
			}
		});
	}

}
