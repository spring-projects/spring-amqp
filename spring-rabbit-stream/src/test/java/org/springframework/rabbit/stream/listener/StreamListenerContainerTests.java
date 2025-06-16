/*
 * Copyright 2022-present the original author or authors.
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

package org.springframework.rabbit.stream.listener;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.MessageHandler.Context;
import org.aopalliance.intercept.MethodInterceptor;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @since 2.4.5
 *
 */
public class StreamListenerContainerTests {

	@Test
	void testAdviceChain() throws Exception {
		Environment env = mock(Environment.class);
		ConsumerBuilder builder = mock(ConsumerBuilder.class);
		given(env.consumerBuilder()).willReturn(builder);
		AtomicReference<MessageHandler> handler = new AtomicReference<>();
		willAnswer(inv -> {
			handler.set(inv.getArgument(0));
			return null;
		}
		).given(builder).messageHandler(any());
		AtomicBoolean advised = new AtomicBoolean();
		MethodInterceptor advice = (inv) -> {
			advised.set(true);
			return inv.proceed();
		};

		StreamListenerContainer container = new StreamListenerContainer(env);
		container.setAdviceChain(advice);
		AtomicBoolean called = new AtomicBoolean();
		MessageListener ml = mock(MessageListener.class);
		willAnswer(inv -> {
			called.set(true);
			return null;
		}).given(ml).onMessage(any());
		container.setupMessageListener(ml);
		Message message = mock(Message.class);
		given(message.getBodyAsBinary()).willReturn("foo".getBytes());
		Context context = mock(Context.class);
		handler.get().handle(context, message);
		assertThat(advised.get()).isTrue();
		assertThat(called.get()).isTrue();

		advised.set(false);
		called.set(false);
		ChannelAwareMessageListener cal = mock(ChannelAwareMessageListener.class);
		willAnswer(inv -> {
			called.set(true);
			return null;
		}).given(cal).onMessage(any(), isNull());
		container.setupMessageListener(cal);
		handler.get().handle(context, message);
		assertThat(advised.get()).isTrue();
		assertThat(called.get()).isTrue();

		called.set(false);
		StreamMessageListener sml = mock(StreamMessageListener.class);
		willAnswer(inv -> {
			called.set(true);
			return null;
		}).given(sml).onStreamMessage(message, context);
		container.setupMessageListener(sml);
		handler.get().handle(context, message);
		assertThat(advised.get()).isTrue();
		assertThat(called.get()).isTrue();
	}

}
