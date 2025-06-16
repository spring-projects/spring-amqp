/*
 * Copyright 2019-present the original author or authors.
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

package org.springframework.amqp.rabbit.listener;

import org.apache.commons.logging.Log;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.ImmediateRequeueAmqpException;
import org.springframework.amqp.rabbit.listener.support.ContainerUtils;
import org.springframework.amqp.rabbit.support.ListenerExecutionFailedException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @since 2.1.8
 *
 */
public class ContainerUtilsTests {

	@Test
	void testMustRequeue() {
		assertThat(ContainerUtils.shouldRequeue(false,
				new ListenerExecutionFailedException("", new ImmediateRequeueAmqpException("requeue")),
				mock(Log.class)))
			.isTrue();
	}

	@Test
	void testMustNotRequeue() {
		assertThat(ContainerUtils.shouldRequeue(true,
				new ListenerExecutionFailedException("", new AmqpRejectAndDontRequeueException("no requeue")),
				mock(Log.class)))
			.isFalse();
	}

}
