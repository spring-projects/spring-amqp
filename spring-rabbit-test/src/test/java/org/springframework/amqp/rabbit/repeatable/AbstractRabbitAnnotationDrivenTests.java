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

package org.springframework.amqp.rabbit.repeatable;

import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.config.RabbitListenerContainerTestFactory;
import org.springframework.amqp.rabbit.listener.MethodRabbitListenerEndpoint;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

/**
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 *
 * @since 1.6
 */
public abstract class AbstractRabbitAnnotationDrivenTests {

	@Rule
	public final ExpectedException thrown = ExpectedException.none();

	@Test
	public abstract void rabbitListenerIsRepeatable();

	/**
	 * Test for {@link RabbitListenerRepeatableBean} that validates that the
	 * {@code @RabbitListener} annotation is repeatable and generate one specific container per annotation.
	 */
	public void testRabbitListenerRepeatable(ApplicationContext context) {
		RabbitListenerContainerTestFactory simpleFactory =
				context.getBean("rabbitListenerContainerFactory", RabbitListenerContainerTestFactory.class);
		assertEquals(4, simpleFactory.getListenerContainers().size());

		MethodRabbitListenerEndpoint first = (MethodRabbitListenerEndpoint)
				simpleFactory.getListenerContainer("first").getEndpoint();
		assertEquals("first", first.getId());
		assertEquals("myQueue", first.getQueueNames().iterator().next());

		MethodRabbitListenerEndpoint second = (MethodRabbitListenerEndpoint)
				simpleFactory.getListenerContainer("second").getEndpoint();
		assertEquals("second", second.getId());
		assertEquals("anotherQueue", second.getQueueNames().iterator().next());

		MethodRabbitListenerEndpoint third = (MethodRabbitListenerEndpoint)
				simpleFactory.getListenerContainer("third").getEndpoint();
		assertEquals("third", third.getId());
		assertEquals("class1", third.getQueueNames().iterator().next());

		MethodRabbitListenerEndpoint fourth = (MethodRabbitListenerEndpoint)
				simpleFactory.getListenerContainer("fourth").getEndpoint();
		assertEquals("fourth", fourth.getId());
		assertEquals("class2", fourth.getQueueNames().iterator().next());
	}

	@Component
	static class RabbitListenerRepeatableBean {

		@RabbitListener(id = "first", queues = "myQueue")
		@RabbitListener(id = "second", queues = "anotherQueue")
		public void repeatableHandle(String msg) {
		}

	}

	@Component
	@RabbitListener(id = "third", queues = "class1")
	@RabbitListener(id = "fourth", queues = "class2")
	static class ClassLevelRepeatableBean {

		@RabbitHandler
		public void repeatableHandle(String msg) {
		}

	}

}
