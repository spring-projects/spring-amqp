/*
 * Copyright 2014-2016 the original author or authors.
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

package org.springframework.amqp.rabbit.core;

import java.util.Date;
import java.util.concurrent.ScheduledFuture;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.support.BatchingStrategy;
import org.springframework.amqp.rabbit.core.support.MessageBatch;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.context.Lifecycle;
import org.springframework.scheduling.TaskScheduler;

/**
 * A {@link RabbitTemplate} that permits batching individual messages into a larger
 * message. All {@code send()} methods (except
 * {@link #send(String, String, org.springframework.amqp.core.Message,
 * org.springframework.amqp.rabbit.support.CorrelationData)})
 * are eligible for batching.
 * <p>
 * <b>Experimental - APIs may change.</b>
 *
 * @author Gary Russell
 * @since 1.4.1
 *
 */
public class BatchingRabbitTemplate extends RabbitTemplate implements Lifecycle {

	private final BatchingStrategy batchingStrategy;

	private final TaskScheduler scheduler;

	private volatile ScheduledFuture<?> scheduledTask;

	/**
	 * @param batchingStrategy the batching strategy.
	 * @param scheduler the scheduler.
	 */
	public BatchingRabbitTemplate(BatchingStrategy batchingStrategy, TaskScheduler scheduler) {
		this.batchingStrategy = batchingStrategy;
		this.scheduler = scheduler;
	}

	@Override
	public synchronized void send(String exchange, String routingKey, Message message, CorrelationData correlationData)
			throws AmqpException {
		if (correlationData != null) {
			if (logger.isDebugEnabled()) {
				logger.debug("Cannot use batching with correlation data");
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
				this.scheduledTask = this.scheduler.schedule((Runnable) () -> releaseBatches(), next);
			}
		}
	}

	/**
	 * Flush any partial in-progress batches.
	 */
	public void flush() {
		releaseBatches();
	}

	private synchronized void releaseBatches() {
		for (MessageBatch batch : this.batchingStrategy.releaseBatches()) {
			super.send(batch.getExchange(), batch.getRoutingKey(), batch.getMessage(), null);
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
