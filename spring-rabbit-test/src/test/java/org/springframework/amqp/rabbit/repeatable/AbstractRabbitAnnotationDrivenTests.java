/*
 * Copyright 2016-present the original author or authors.
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

package org.springframework.amqp.rabbit.repeatable;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.config.RabbitListenerContainerTestFactory;
import org.springframework.amqp.rabbit.listener.MethodRabbitListenerEndpoint;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 *
 * @since 1.6
 */
public abstract class AbstractRabbitAnnotationDrivenTests {

	@Test
	public abstract void rabbitListenerIsRepeatable();

	/**
	 * Test for {@link RabbitListenerRepeatableBean} that validates that the
	 * {@code @RabbitListener} annotation is repeatable and generate one specific container per annotation.
	 */
	public void testRabbitListenerRepeatable(ApplicationContext context) {
		RabbitListenerContainerTestFactory simpleFactory =
				context.getBean("rabbitListenerContainerFactory", RabbitListenerContainerTestFactory.class);
		assertThat(simpleFactory.getListenerContainers()).hasSize(4);

		MethodRabbitListenerEndpoint first = (MethodRabbitListenerEndpoint)
				simpleFactory.getListenerContainer("first").getEndpoint();
		assertThat(first.getId()).isEqualTo("first");
		assertThat(first.getQueueNames().iterator().next()).isEqualTo("myQueue");

		MethodRabbitListenerEndpoint second = (MethodRabbitListenerEndpoint)
				simpleFactory.getListenerContainer("second").getEndpoint();
		assertThat(second.getId()).isEqualTo("second");
		assertThat(second.getQueueNames().iterator().next()).isEqualTo("anotherQueue");

		MethodRabbitListenerEndpoint third = (MethodRabbitListenerEndpoint)
				simpleFactory.getListenerContainer("third").getEndpoint();
		assertThat(third.getId()).isEqualTo("third");
		assertThat(third.getQueueNames().iterator().next()).isEqualTo("class1");

		MethodRabbitListenerEndpoint fourth = (MethodRabbitListenerEndpoint)
				simpleFactory.getListenerContainer("fourth").getEndpoint();
		assertThat(fourth.getId()).isEqualTo("fourth");
		assertThat(fourth.getQueueNames().iterator().next()).isEqualTo("class2");
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
