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

package org.springframework.rabbit.stream.producer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import com.rabbitmq.stream.ConfirmationHandler;
import com.rabbitmq.stream.ConfirmationStatus;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.ProducerBuilder;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.rabbit.stream.support.converter.StreamMessageConverter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @since 2.4.7
 *
 */
public class RabbitStreamTemplateTests {

	@Test
	void handleConfirm() throws InterruptedException, ExecutionException {
		Environment env = mock(Environment.class);
		ProducerBuilder pb = mock(ProducerBuilder.class);
		given(env.producerBuilder()).willReturn(pb);
		Producer producer = mock(Producer.class);
		given(pb.build()).willReturn(producer);
		AtomicInteger which = new AtomicInteger();
		willAnswer(inv -> {
			ConfirmationHandler handler = inv.getArgument(1);
			ConfirmationStatus status = null;
			switch (which.getAndIncrement()) {
			case 0:
				status = new ConfirmationStatus(inv.getArgument(0), true, (short) 0);
				break;
			case 1:
				status = new ConfirmationStatus(inv.getArgument(0), false, Constants.CODE_MESSAGE_ENQUEUEING_FAILED);
				break;
			case 2:
				status = new ConfirmationStatus(inv.getArgument(0), false, Constants.CODE_PRODUCER_CLOSED);
				break;
			case 3:
				status = new ConfirmationStatus(inv.getArgument(0), false, Constants.CODE_PRODUCER_NOT_AVAILABLE);
				break;
			case 4:
				status = new ConfirmationStatus(inv.getArgument(0), false, Constants.CODE_PUBLISH_CONFIRM_TIMEOUT);
				break;
			case 5:
				status = new ConfirmationStatus(inv.getArgument(0), false, (short) -1);
				break;
			}
			handler.handle(status);
			return null;
		}).given(producer).send(any(), any());
		try (RabbitStreamTemplate template = new RabbitStreamTemplate(env, "foo")) {
			SimpleMessageConverter messageConverter = new SimpleMessageConverter();
			template.setMessageConverter(messageConverter);
			assertThat(template.messageConverter()).isSameAs(messageConverter);
			StreamMessageConverter converter = mock(StreamMessageConverter.class);
			given(converter.fromMessage(any())).willReturn(mock(Message.class));
			template.setStreamConverter(converter);
			assertThat(template.streamMessageConverter()).isSameAs(converter);
			CompletableFuture<Boolean> future = template.convertAndSend("foo");
			assertThat(future.get()).isTrue();
			CompletableFuture<Boolean> future1 = template.convertAndSend("foo");
			assertThatExceptionOfType(ExecutionException.class).isThrownBy(() -> future1.get())
					.withCauseExactlyInstanceOf(StreamSendException.class)
					.withStackTraceContaining("Message Enqueueing Failed");
			CompletableFuture<Boolean> future2 = template.convertAndSend("foo");
			assertThatExceptionOfType(ExecutionException.class).isThrownBy(() -> future2.get())
					.withCauseExactlyInstanceOf(StreamSendException.class)
					.withStackTraceContaining("Producer Closed");
			CompletableFuture<Boolean> future3 = template.convertAndSend("foo");
			assertThatExceptionOfType(ExecutionException.class).isThrownBy(() -> future3.get())
					.withCauseExactlyInstanceOf(StreamSendException.class)
					.withStackTraceContaining("Producer Not Available");
			CompletableFuture<Boolean> future4 = template.convertAndSend("foo");
			assertThatExceptionOfType(ExecutionException.class).isThrownBy(() -> future4.get())
					.withCauseExactlyInstanceOf(StreamSendException.class)
					.withStackTraceContaining("Publish Confirm Timeout");
			CompletableFuture<Boolean> future5 = template.convertAndSend("foo");
			assertThatExceptionOfType(ExecutionException.class).isThrownBy(() -> future5.get())
					.withCauseExactlyInstanceOf(StreamSendException.class)
					.withStackTraceContaining("Unknown code: " + -1);
		}
	}

	@Test
	void superStream() {
		Environment env = mock(Environment.class);
		ProducerBuilder pb = mock(ProducerBuilder.class);
		given(pb.superStream(any())).willReturn(pb);
		given(env.producerBuilder()).willReturn(pb);
		Producer producer = mock(Producer.class);
		given(pb.build()).willReturn(producer);
		try (RabbitStreamTemplate template = new RabbitStreamTemplate(env, "foo")) {
			SimpleMessageConverter messageConverter = new SimpleMessageConverter();
			template.setMessageConverter(messageConverter);
			assertThat(template.messageConverter()).isSameAs(messageConverter);
			StreamMessageConverter converter = mock(StreamMessageConverter.class);
			given(converter.fromMessage(any())).willReturn(mock(Message.class));
			template.setStreamConverter(converter);
			template.setSuperStreamRouting(msg -> "bar");
			template.convertAndSend("x");
			verify(pb).superStream("foo");
			verify(pb).routing(any());
			verify(pb, never()).stream("foo");
		}
	}

}
