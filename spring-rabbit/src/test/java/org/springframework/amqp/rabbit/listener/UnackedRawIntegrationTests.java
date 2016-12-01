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

package org.springframework.amqp.rabbit.listener;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.support.Delivery;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;

/**
 * Used to verify raw Rabbit Java Client behaviour for corner cases.
 *
 * @author Dave Syer
 *
 */
@Ignore
public class UnackedRawIntegrationTests {

	private final ConnectionFactory factory = new ConnectionFactory();
	private Connection conn;
	private Channel noTxChannel;
	private Channel txChannel;

	@Before
	public void init() throws Exception {

		factory.setHost("localhost");
		factory.setPort(BrokerTestUtils.getPort());
		conn = factory.newConnection();
		noTxChannel = conn.createChannel();
		txChannel = conn.createChannel();
		txChannel.basicQos(1);
		txChannel.txSelect();

		try {
			noTxChannel.queueDelete("test.queue");
		}
		catch (IOException e) {
			noTxChannel = conn.createChannel();
		}
		noTxChannel.queueDeclare("test.queue", true, false, false, null);

	}

	@After
	public void clear() throws Exception {
		if (txChannel != null) {
			try {
				txChannel.close();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
		if (noTxChannel != null) {
			try {
				noTxChannel.close();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
		conn.close();
	}

	@Test
	public void testOnePublishUnackedRequeued() throws Exception {

		noTxChannel.basicPublish("", "test.queue", null, "foo".getBytes());

		BlockingConsumer callback = new BlockingConsumer(txChannel);
		txChannel.basicConsume("test.queue", callback);
		Delivery next = callback.nextDelivery(10_000L);
		assertNotNull(next);
		txChannel.basicReject(next.getEnvelope().getDeliveryTag(), true);
		txChannel.txRollback();

		GetResponse get = noTxChannel.basicGet("test.queue", true);
		assertNotNull(get);

	}

	@Test
	public void testFourPublishUnackedRequeued() throws Exception {

		noTxChannel.basicPublish("", "test.queue", null, "foo".getBytes());
		noTxChannel.basicPublish("", "test.queue", null, "bar".getBytes());
		noTxChannel.basicPublish("", "test.queue", null, "one".getBytes());
		noTxChannel.basicPublish("", "test.queue", null, "two".getBytes());

		BlockingConsumer callback = new BlockingConsumer(txChannel);
		txChannel.basicConsume("test.queue", callback);
		Delivery next = callback.nextDelivery(10_000L);
		assertNotNull(next);
		txChannel.basicReject(next.getEnvelope().getDeliveryTag(), true);
		txChannel.txRollback();

		GetResponse get = noTxChannel.basicGet("test.queue", true);
		assertNotNull(get);

	}

	public class BlockingConsumer extends DefaultConsumer {

		private final BlockingQueue<Delivery> queue = new LinkedBlockingQueue<>();

		public BlockingConsumer(Channel channel) {
			super(channel);
		}

		public Delivery nextDelivery(long timeout) throws InterruptedException {
			return this.queue.poll(timeout, TimeUnit.MILLISECONDS);
		}

		@Override
		public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
				throws IOException {
			try {
				this.queue.put(new Delivery(consumerTag, envelope, properties, body));
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IOException(e);
			}
		}

	}

}
