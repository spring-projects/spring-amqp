/*
 * Copyright 2018 the original author or authors.
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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;

/**
 * @author Gary Russell
 *
 * @since 2.1
 *
 */
@RabbitAvailable(queues = SimplePublisherConfirmsTests.QUEUE)
public class SimplePublisherConfirmsTests {

	public static final String QUEUE = "simple.confirms";

	@Test
	public void testConfirms() {
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		cf.setSimplePublisherConfirms(true);
		RabbitTemplate template = new RabbitTemplate(cf);
		template.setRoutingKey(QUEUE);
		assertTrue(template.invoke(t -> {
			template.convertAndSend("foo");
			template.convertAndSend("bar");
			template.waitForConfirmsOrDie(10_000);
			return true;
		}));
		cf.destroy();
	}

	@Test
	public void testConfirmsWithCallbacks() {
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		cf.setSimplePublisherConfirms(true);
		RabbitTemplate template = new RabbitTemplate(cf);
		template.setRoutingKey(QUEUE);
		AtomicReference<MessageProperties> finalProperties = new AtomicReference<>();
		AtomicLong lastAck = new AtomicLong();
		assertTrue(template.invoke(t -> {
			template.convertAndSend("foo");
			template.convertAndSend("bar", m -> {
				finalProperties.set(m.getMessageProperties());
				return m;
			});
			template.waitForConfirmsOrDie(10_000);
			return true;
		}, (tag, multiple) -> {
			lastAck.set(tag);
		}, (tag, multiple) -> { }));
		assertThat(lastAck.get(), equalTo(finalProperties.get().getPublishSequenceNumber()));
		cf.destroy();
	}

}
