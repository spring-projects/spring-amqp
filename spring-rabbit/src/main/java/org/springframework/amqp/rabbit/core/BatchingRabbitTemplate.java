/*
 * Copyright 2014-2024 the original author or authors.
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

package org.springframework.amqp.rabbit.core;

import java.util.Date;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.batch.BatchingStrategy;
import org.springframework.amqp.rabbit.batch.MessageBatch;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.lang.Nullable;
import org.springframework.scheduling.TaskScheduler;

/**
 * A {@link RabbitTemplate} that permits batching individual messages into a larger
 * message. All {@code send()} methods (except
 * {@link #send(String, String, org.springframework.amqp.core.Message,
 * org.springframework.amqp.rabbit.connection.CorrelationData)})
 * are eligible for batching.
 * <p>
 * <b>Experimental - APIs may change.</b>
 *
 * @author Gary Russell
 * @since 1.4.1
 *
 */
public class BatchingRabbitTemplate extends RabbitTemplate {

	private final Lock lock = new ReentrantLock();

	private final BatchingStrategy batchingStrategy;

	private final TaskScheduler scheduler;

	private volatile ScheduledFuture<?> scheduledTask;

	/**
	 * Create an instance with the supplied parameters.
	 * @param batchingStrategy the batching strategy.
	 * @param scheduler the scheduler.
	 */
	public BatchingRabbitTemplate(BatchingStrategy batchingStrategy, TaskScheduler scheduler) {
		this.batchingStrategy = batchingStrategy;
		this.scheduler = scheduler;
	}

	/**
	 * Create an instance with the supplied parameters.
	 * @param connectionFactory the connection factory.
	 * @param batchingStrategy the batching strategy.
	 * @param scheduler the scheduler.
	 * @since 2.2
	 */
	public BatchingRabbitTemplate(ConnectionFactory connectionFactory, BatchingStrategy batchingStrategy,
			TaskScheduler scheduler) {

		super(connectionFactory);
		this.batchingStrategy = batchingStrategy;
		this.scheduler = scheduler;
	}

	@Override
	public void send(String exchange, String routingKey, Message message,
			@Nullable CorrelationData correlationData) throws AmqpException {
		this.lock.lock();
		try {
			if (correlationData != null) {
				if (this.logger.isDebugEnabled()) {
					this.logger.debug("Cannot use batching with correlation data");
				}
				super.send(exchange, routingKey, message, correlationData);
			}
			else {
				if (this.scheduledTask != null) {
					this.scheduledTask.cancel(false);
				}
				MessageBatch batch = this.batchingStrategy.addToBatch(exchange, routingKey, message);
				if (batch != null) {
					super.send(batch.getExchange(), batch.getRoutingKey(), batch.getMessage(), null);
				}
				Date next = this.batchingStrategy.nextRelease();
				if (next != null) {
					this.scheduledTask = this.scheduler.schedule(this::releaseBatches, next.toInstant());
				}
			}
		}
		finally {
			this.lock.unlock();
		}
	}

	/**
	 * Flush any partial in-progress batches.
	 */
	public void flush() {
		releaseBatches();
	}

	private void releaseBatches() {
		this.lock.lock();
		try {
			for (MessageBatch batch : this.batchingStrategy.releaseBatches()) {
				super.send(batch.getExchange(), batch.getRoutingKey(), batch.getMessage(), null);
			}
		}
		finally {
			this.lock.unlock();
		}
	}

	@Override
	public void doStart() {
	}

	@Override
	public void doStop() {
		flush();
	}

	@Override
	public boolean isRunning() {
		return true;
	}

}
