/*
 * Copyright 2002-2012 the original author or authors.
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

import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Gary Russell
 * @since 1.1.3
 *
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
public class StopStartIntegrationTests {

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunning();

	private static AtomicInteger deliveries = new AtomicInteger();

	private static int COUNT = 300000;

	@Autowired
	private ApplicationContext ctx;

	@Autowired
	private RabbitTemplate rabbitTemplate;

	@Autowired
	private SimpleMessageListenerContainer container;

	@Test
	public void test() throws Exception {
		for (int i = 0; i < COUNT; i++) {
			rabbitTemplate.convertAndSend("foo" + i);
		}
		long t = System.currentTimeMillis();
		container.start();
		int n;
		int lastN = 0;
		while ((n = deliveries.get()) < COUNT) {
			Thread.sleep(5000);
			container.stop();
			container.start();
			if (System.currentTimeMillis() - t > 240000 && lastN == n) {
				fail("Only received " + deliveries.get());
			}
			lastN = n;
		}
	}

	public static class StopStartListener implements MessageListener {

		@Override
		public void onMessage(Message message) {
			deliveries.incrementAndGet();
		}

	}
}
