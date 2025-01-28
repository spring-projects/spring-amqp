/*
 * Copyright 2022-2025 the original author or authors.
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

package org.springframework.amqp.rabbit.config;

import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.listener.MessageListenerContainer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @since 2.4.8
 *
 */
public class CompositeContainerCustomizerTests {

	@SuppressWarnings("unchecked")
	@Test
	void allCalled() {
		ContainerCustomizer<MessageListenerContainer> mock1 = mock(ContainerCustomizer.class);
		ContainerCustomizer<MessageListenerContainer> mock2 = mock(ContainerCustomizer.class);
		CompositeContainerCustomizer<MessageListenerContainer> cust = new CompositeContainerCustomizer<>(
				List.of(mock1, mock2));
		MessageListenerContainer mlc = mock(MessageListenerContainer.class);
		cust.configure(mlc);
		verify(mock1).configure(mlc);
		verify(mock2).configure(mlc);
	}

}
