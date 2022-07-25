/*
 * Copyright 2014-2022 the original author or authors.
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

package org.springframework.amqp.rabbit.batch;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.support.ListenerExecutionFailedException;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.beans.BeanUtils;
import org.springframework.util.Assert;

/**
 * A simple batching strategy that supports only one exchange/routingKey; includes a batch
 * size, a batched message size limit and a timeout. The message properties from the first
 * message in the batch is used in the batch message. Each message is preceded by a 4 byte
 * length field.
 *
 * @author Gary Russell
 * @since 1.4.1
 *
 */
public class SimpleBatchingStrategy implements BatchingStrategy {

	private final int batchSize;

	private final int bufferLimit;

	private final long timeout;

	private final List<Message> messages = new ArrayList<Message>();

	private String exchange;

	private String routingKey;

	private int currentSize;

	/**
	 * @param batchSize the batch size.
	 * @param bufferLimit the max buffer size; could trigger a short batch. Does not apply
	 * to a single message.
	 * @param timeout the batch timeout.
	 */
	public SimpleBatchingStrategy(int batchSize, int bufferLimit, long timeout) {
		this.batchSize = batchSize;
		this.bufferLimit = bufferLimit;
		this.timeout = timeout;
	}

	@Override
	public MessageBatch addToBatch(String exch, String routKey, Message message) {
		if (this.exchange != null) {
			Assert.isTrue(this.exchange.equals(exch), "Cannot send to different exchanges in the same batch");
		}
		else {
			this.exchange = exch;
		}
		if (this.routingKey != null) {
			Assert.isTrue(this.routingKey.equals(routKey),
					"Cannot send with different routing keys in the same batch");
		}
		else {
			this.routingKey = routKey;
		}
		int bufferUse = Integer.BYTES + message.getBody().length;
		MessageBatch batch = null;
		if (this.messages.size() > 0 && this.currentSize + bufferUse > this.bufferLimit) {
			batch = doReleaseBatch();
			this.exchange = exch;
			this.routingKey = routKey;
		}
		this.currentSize += bufferUse;
		this.messages.add(message);
		if (batch == null && (this.messages.size() >= this.batchSize
								|| this.currentSize >= this.bufferLimit)) {
			batch = doReleaseBatch();
		}
		return batch;
	}

	@Override
	public Date nextRelease() {
		if (this.messages.size() == 0 || this.timeout <= 0) {
			return null;
		}
		else if (this.currentSize >= this.bufferLimit) {
			// release immediately, we're already over the limit
			return new Date();
		}
		else {
			return new Date(System.currentTimeMillis() + this.timeout);
		}
	}

	@Override
	public Collection<MessageBatch> releaseBatches() {
		MessageBatch batch = doReleaseBatch();
		if (batch == null) {
			return Collections.emptyList();
		}
		else {
			return Collections.singletonList(batch);
		}
	}

	private MessageBatch doReleaseBatch() {
		if (this.messages.size() < 1) {
			return null;
		}
		Message message = assembleMessage();
		MessageBatch messageBatch = new MessageBatch(this.exchange, this.routingKey, message);
		this.messages.clear();
		this.currentSize = 0;
		this.exchange = null;
		this.routingKey = null;
		return messageBatch;
	}

	private Message assembleMessage() {
		if (this.messages.size() == 1) {
			return this.messages.get(0);
		}
		MessageProperties messageProperties = this.messages.get(0).getMessageProperties();
		byte[] body = new byte[this.currentSize];
		ByteBuffer bytes = ByteBuffer.wrap(body);
		for (Message message : this.messages) {
			bytes.putInt(message.getBody().length);
			bytes.put(message.getBody());
		}
		messageProperties.getHeaders().put(MessageProperties.SPRING_BATCH_FORMAT,
				MessageProperties.BATCH_FORMAT_LENGTH_HEADER4);
		messageProperties.getHeaders().put(AmqpHeaders.BATCH_SIZE, this.messages.size());
		return new Message(body, messageProperties);
	}

	@Override
	public boolean canDebatch(MessageProperties properties) {
		return MessageProperties.BATCH_FORMAT_LENGTH_HEADER4.equals(properties
				.getHeaders()
				.get(MessageProperties.SPRING_BATCH_FORMAT));
	}

	/**
	 * Debatch a message that has a header with {@link MessageProperties#SPRING_BATCH_FORMAT}
	 * set to {@link MessageProperties#BATCH_FORMAT_LENGTH_HEADER4}.
	 * @param message the batched message.
	 * @param fragmentConsumer a consumer for each fragment.
	 * @since 2.2
	 */
	@Override
	public void deBatch(Message message, Consumer<Message> fragmentConsumer) {
		ByteBuffer byteBuffer = ByteBuffer.wrap(message.getBody());
		MessageProperties messageProperties = message.getMessageProperties();
		messageProperties.getHeaders().remove(MessageProperties.SPRING_BATCH_FORMAT);
		while (byteBuffer.hasRemaining()) {
			int length = byteBuffer.getInt();
			if (length < 0 || length > byteBuffer.remaining()) {
				throw new ListenerExecutionFailedException("Bad batched message received",
						new MessageConversionException("Insufficient batch data at offset " + byteBuffer.position()),
						message);
			}
			byte[] body = new byte[length];
			byteBuffer.get(body);
			messageProperties.setContentLength(length);
			// Caveat - shared MessageProperties, except for last
			Message fragment;
			if (byteBuffer.hasRemaining()) {
				 fragment = new Message(body, messageProperties);
			}
			else {
				MessageProperties lastProperties = new MessageProperties();
				BeanUtils.copyProperties(messageProperties, lastProperties);
				lastProperties.setLastInBatch(true);
				fragment = new Message(body, lastProperties);
			}
			fragmentConsumer.accept(fragment);
		}
	}

}
