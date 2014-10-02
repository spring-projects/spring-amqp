/*
 * Copyright 2013 the original author or authors.
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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Executors;

import org.junit.Test;

import org.springframework.amqp.AmqpIOException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * @author Gary Russell
 * @since 3.0
 *
 */
public class Tester {

	@Test
	public void testDealLockOnConfirmChannelClose() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		Connection conn = factory.newConnection();
		final Channel channel = conn.createChannel();

		final SortedSet<Long> unconfirmedSet = Collections.<Long>synchronizedSortedSet(new TreeSet<Long>());

		channel.addConfirmListener(new ConfirmListener() {
			public void handleAck(long seqNo, boolean multiple) {
				System.out.println(seqNo + " " + multiple);
				if (multiple) {
					unconfirmedSet.headSet(seqNo + 1).clear();
				}
				else {
					unconfirmedSet.remove(seqNo);
				}
			}

			public void handleNack(long seqNo, boolean multiple) {
				// handle the lost messages somehow
			}
		});

		channel.confirmSelect();
		for (long i = 0; i < 10; ++i) {
			unconfirmedSet.add(channel.getNextPublishSeqNo());
			channel.basicPublish("", "test.queue", new AMQP.BasicProperties.Builder().build(), "nop".getBytes());
		}

		while (unconfirmedSet.size() > 0)
			Thread.sleep(100);

		Executors.newSingleThreadExecutor().execute(new Runnable() {
			@Override
			public void run() {
				try {
					channel.close();
				}
				catch (IOException e) {
					throw new AmqpIOException(e);
				}
			}
		});

		assertTrue(unconfirmedSet.isEmpty());
		conn.close();
	}

}
