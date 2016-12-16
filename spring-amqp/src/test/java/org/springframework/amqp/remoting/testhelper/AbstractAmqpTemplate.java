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

package org.springframework.amqp.remoting.testhelper;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.ReceiveAndReplyCallback;
import org.springframework.amqp.core.ReplyToAddressCallback;
import org.springframework.core.ParameterizedTypeReference;

/**
 * @author David Bilge
 * @author Ernest Sadykov
 * @since 1.2
 */
public abstract class AbstractAmqpTemplate implements AmqpTemplate {

	@Override
	public void send(Message message) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void send(String routingKey, Message message) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void send(String exchange, String routingKey, Message message) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void convertAndSend(Object message) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void convertAndSend(String routingKey, Object message) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void convertAndSend(String exchange, String routingKey, Object message) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void convertAndSend(Object message, MessagePostProcessor messagePostProcessor) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void convertAndSend(String routingKey, Object message, MessagePostProcessor messagePostProcessor)
			throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void convertAndSend(String exchange, String routingKey, Object message,
			MessagePostProcessor messagePostProcessor) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Message receive() throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Message receive(String queueName) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Message receive(long timeoutMillis) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Message receive(String queueName, long timeoutMillis) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object receiveAndConvert() throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object receiveAndConvert(String queueName) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object receiveAndConvert(long timeoutMillis) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object receiveAndConvert(String queueName, long timeoutMillis) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public <R, S> boolean receiveAndReply(ReceiveAndReplyCallback<R, S> callback) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public <R, S> boolean receiveAndReply(String queueName, ReceiveAndReplyCallback<R, S> callback) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public <R, S> boolean receiveAndReply(ReceiveAndReplyCallback<R, S> callback, String exchange, String routingKey)
			throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public <R, S> boolean receiveAndReply(String queueName, ReceiveAndReplyCallback<R, S> callback, String replyExchange,
										  String replyRoutingKey) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public <R, S> boolean receiveAndReply(ReceiveAndReplyCallback<R, S> callback, ReplyToAddressCallback<S> replyToAddressCallback)
			throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public <R, S> boolean receiveAndReply(String queueName, ReceiveAndReplyCallback<R, S> callback,
										  ReplyToAddressCallback<S> replyToAddressCallback) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Message sendAndReceive(Message message) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Message sendAndReceive(String routingKey, Message message) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Message sendAndReceive(String exchange, String routingKey, Message message) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object convertSendAndReceive(Object message) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object convertSendAndReceive(String routingKey, Object message) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object convertSendAndReceive(String exchange, String routingKey, Object message) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object convertSendAndReceive(Object message, MessagePostProcessor messagePostProcessor) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object convertSendAndReceive(String routingKey, Object message, MessagePostProcessor messagePostProcessor)
			throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object convertSendAndReceive(String exchange, String routingKey, Object message,
			MessagePostProcessor messagePostProcessor) throws AmqpException {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T convertSendAndReceiveAsType(final String exchange, final String routingKey, final Object message, final MessagePostProcessor messagePostProcessor, ParameterizedTypeReference<T> responseType)
			throws AmqpException {
		return null;
	}

	@Override
	public <T> T convertSendAndReceiveAsType(final String routingKey, final Object message, final MessagePostProcessor messagePostProcessor, ParameterizedTypeReference<T> responseType)
			throws AmqpException {
		return null;
	}

	@Override
	public <T> T convertSendAndReceiveAsType(final Object message, final MessagePostProcessor messagePostProcessor, ParameterizedTypeReference<T> responseType) throws AmqpException {
		return null;
	}

	@Override
	public <T> T convertSendAndReceiveAsType(final String exchange, final String routingKey, final Object message, ParameterizedTypeReference<T> responseType)
			throws AmqpException {
		return null;
	}

	@Override
	public <T> T convertSendAndReceiveAsType(final String routingKey, final Object message, ParameterizedTypeReference<T> responseType) throws AmqpException {
		return null;
	}

	@Override
	public <T> T convertSendAndReceiveAsType(final Object message, ParameterizedTypeReference<T> responseType) throws AmqpException {
		return null;
	}

	@Override
	public <T> T receiveAndConvert(String queueName, long timeoutMillis, ParameterizedTypeReference<T> type) throws AmqpException {
		return null;
	}

	@Override
	public <T> T receiveAndConvert(long timeoutMillis, ParameterizedTypeReference<T> type) throws AmqpException {
		return null;
	}

	@Override
	public <T> T receiveAndConvert(String queueName, ParameterizedTypeReference<T> type) throws AmqpException {
		return null;
	}

	@Override
	public <T> T receiveAndConvert(ParameterizedTypeReference<T> type) throws AmqpException {
		return null;
	}

}
