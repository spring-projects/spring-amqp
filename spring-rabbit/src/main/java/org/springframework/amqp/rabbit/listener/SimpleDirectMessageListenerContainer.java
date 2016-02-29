/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.amqp.rabbit.listener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.util.Assert;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * The {@code SimpleMessageListenerContainer} is not so simple. Recent changes to the
 * rabbitmq java client has facilitated a much simpler listener container that invokes the
 * listener directly on the rabbit client consumer thread. There is no txSize property -
 * each message is acked (or nacked) individually.
 * <p>
 * This should be considered experimental at this time; a fully-functional listener container
 * based on this is planned for the 2.0 release.
 * <p>
 * TODO: declare queues; advice chain; error handler; AmqpRejectAndDontRequeueException;
 * listener container factory; ...
 * @author Gary Russell
 * @since 1.6
 *
 */
public class SimpleDirectMessageListenerContainer extends AbstractMessageListenerContainer {

	private final List<SimpleConsumer> consumers = new ArrayList<SimpleConsumer>();

	private final MessagePropertiesConverter messagePropertiesConverter = new DefaultMessagePropertiesConverter();

	private volatile TaskExecutor taskExecutor;

	private volatile int consumersPerQueue = 1;

	private volatile int prefetch = 1;

	private volatile boolean requeueRejected = false;

	/**
	 * Create an instance with the provided connection factory.
	 * @param connectionFactory the connection factory.
	 */
	public SimpleDirectMessageListenerContainer(ConnectionFactory connectionFactory) {
		setConnectionFactory(connectionFactory);
	}

	@Override
	public final void setConnectionFactory(ConnectionFactory connectionFactory) {
		super.setConnectionFactory(connectionFactory);
	}

	/**
	 * Set a task executor for the container - used to create the consumers not at
	 * runtime.
	 * @param taskExecutor the task excutor.
	 */
	public void setTaskExecutor(TaskExecutor taskExecutor) {
		this.taskExecutor = taskExecutor;
	}

	/**
	 * Each queue runs in its own consumer; set this property to create multiple
	 * consumers for each queue.
	 * @param consumersPerQueue the consumers per queue.
	 */
	public void setConsumersPerQueue(int consumersPerQueue) {
		this.consumersPerQueue = consumersPerQueue;
	}

	/**
	 * Set to false to requeue failed deliveries.
	 * @param requeueRejected the requeue rejected.
	 */
	public void setRequeueRejected(boolean requeueRejected) {
		this.requeueRejected = requeueRejected;
	}

	@Override
	protected void doInitialize() throws Exception {
		Assert.notNull(getQueueNames(), "queue(s) are required");
	}

	@Override
	protected void doStart() throws Exception {
		super.doStart();
		if (this.taskExecutor == null) {
			this.taskExecutor = new SimpleAsyncTaskExecutor((getBeanName() == null ? "container" : getBeanName()) + "-");
		}
		this.taskExecutor.execute(new Runnable() {

			boolean initialized = false;

			@Override
			public void run() {
				while (!initialized) {
					try {
						for (int i = 0; i < consumersPerQueue; i++) {
							for (String queue : getQueueNames()) {
								Connection connection = getConnectionFactory().createConnection();
								Channel channel = connection.createChannel(false);
								channel.basicQos(prefetch);
								RabbitUtils.setPhysicalCloseRequired(true);
								SimpleConsumer consumer = new SimpleConsumer(channel, queue);
								consumers.add(consumer);
								channel.basicConsume(queue, consumer);
							}
						}
					}
					catch (Exception e) {
						logger.error("Error creating consumer", e);
						doShutdown();
						try {
							Thread.sleep(5000);
						}
						catch (InterruptedException e1) {
							Thread.currentThread().interrupt();
						}
						continue;
					}
					initialized = true;
				}
			}

		});
	}

	@Override
	protected void doShutdown() {
		Assert.state(this.taskExecutor != null, "Cannot shut down if not initialized");
		for (DefaultConsumer consumer : this.consumers) {
			try {
				consumer.getChannel().basicCancel(consumer.getConsumerTag());
				consumer.getChannel().close();
			}
			catch (IOException e) {
				logger.error("Cancel Error", e);
			}
			catch (TimeoutException e) {
				logger.error("Timeout Error on channel close", e);
			}
		}
		this.consumers.clear();
	}

	private final class SimpleConsumer extends DefaultConsumer {

		private final String queue;

		public SimpleConsumer(Channel channel, String queue) {
			super(channel);
			this.queue = queue;
		}

		@Override
		public void handleDelivery(String consumerTag, Envelope envelope,
				BasicProperties properties, byte[] body) throws IOException {
			MessageProperties messageProperties = messagePropertiesConverter.toMessageProperties(
					properties, envelope, "UTF-8");
			messageProperties.setMessageCount(0);
			messageProperties.setConsumerTag(consumerTag);
			messageProperties.setConsumerQueue(queue);
			Message message = new Message(body, messageProperties);
			try {
				invokeListener(message, getChannel());
				getChannel().basicAck(envelope.getDeliveryTag(), false);
			}
			catch (Exception e) {
				getChannel().basicNack(envelope.getDeliveryTag(), false, requeueRejected);
			}
		}

		private void invokeListener(Message message, Channel channel) throws Exception {
			Object messageListener = getMessageListener();
			if (messageListener instanceof ChannelAwareMessageListener) {
				((ChannelAwareMessageListener) messageListener).onMessage(message, channel);
			}
			else if (messageListener instanceof MessageListener) {
					((MessageListener) messageListener).onMessage(message);
			}
			else {
				throw new IllegalStateException();
			}
		}

	}

}
