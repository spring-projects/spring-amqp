/*
 * Copyright 2018-2025 the original author or authors.
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

package org.springframework.amqp.rabbit.retry;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.rabbit.support.ListenerExecutionFailedException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextClosedEvent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.0.5
 *
 */
@RabbitAvailable(queues = RepublishMessageRecovererIntegrationTests.BIG_HEADER_QUEUE)
class RepublishMessageRecovererIntegrationTests {

	static final String BIG_HEADER_QUEUE = "big.header.queue";

	private static final String BIG_EXCEPTION_MESSAGE1 = new String(new byte[10_000]).replace("\u0000", "x");

	private static final String BIG_EXCEPTION_MESSAGE2 = new String(new byte[10_000]).replace("\u0000", "y");

	private int maxHeaderSize;

	@Test
	void testBigHeader() {
		CachingConnectionFactory ccf = new CachingConnectionFactory(
				RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		ApplicationContext applicationContext = mock();
		ccf.setApplicationContext(applicationContext);
		RabbitTemplate template = new RabbitTemplate(ccf);
		this.maxHeaderSize = RabbitUtils.getMaxFrame(template.getConnectionFactory())
				- RepublishMessageRecoverer.DEFAULT_FRAME_MAX_HEADROOM;
		assertThat(this.maxHeaderSize).isGreaterThan(0);
		RepublishMessageRecoverer recoverer = new RepublishMessageRecoverer(template, "", BIG_HEADER_QUEUE);
		recoverer.recover(new Message("foo".getBytes(), new MessageProperties()),
				new ListenerExecutionFailedException("Listener failed",
						bigCause(new RuntimeException(BIG_EXCEPTION_MESSAGE1))));
		Message received = template.receive(BIG_HEADER_QUEUE, 10_000);
		assertThat(received).isNotNull();
		String trace = received.getMessageProperties().getHeaders()
				.get(RepublishMessageRecoverer.X_EXCEPTION_STACKTRACE).toString();
		assertThat(trace.length()).isEqualTo(this.maxHeaderSize - 100);
		String truncatedMessage =
				"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx...";
		assertThat(trace).contains(truncatedMessage);
		assertThat((String) received.getMessageProperties().getHeader(RepublishMessageRecoverer.X_EXCEPTION_MESSAGE))
				.isEqualTo(truncatedMessage);
		ccf.onApplicationEvent(new ContextClosedEvent(applicationContext));
		ccf.destroy();
	}

	@Test
	void testSmallException() {
		CachingConnectionFactory ccf = new CachingConnectionFactory(
				RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		ApplicationContext applicationContext = mock();
		ccf.setApplicationContext(applicationContext);
		RabbitTemplate template = new RabbitTemplate(ccf);
		this.maxHeaderSize = RabbitUtils.getMaxFrame(template.getConnectionFactory())
				- RepublishMessageRecoverer.DEFAULT_FRAME_MAX_HEADROOM;
		assertThat(this.maxHeaderSize).isGreaterThan(0);
		RepublishMessageRecoverer recoverer = new RepublishMessageRecoverer(template, "", BIG_HEADER_QUEUE);
		ListenerExecutionFailedException cause = new ListenerExecutionFailedException("Listener failed",
				new RuntimeException(new String(new byte[200]).replace('\u0000', 'x')));
		recoverer.recover(new Message("foo".getBytes(), new MessageProperties()),
				cause);
		Message received = template.receive(BIG_HEADER_QUEUE, 10_000);
		assertThat(received).isNotNull();
		String trace = received.getMessageProperties().getHeaders()
				.get(RepublishMessageRecoverer.X_EXCEPTION_STACKTRACE).toString();
		assertThat(trace).isEqualTo(getStackTraceAsString(cause));
		ccf.onApplicationEvent(new ContextClosedEvent(applicationContext));
		ccf.destroy();
	}

	@Test
	void testBigMessageSmallTrace() {
		CachingConnectionFactory ccf = new CachingConnectionFactory(
				RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		RabbitTemplate template = new RabbitTemplate(ccf);
		ApplicationContext applicationContext = mock();
		ccf.setApplicationContext(applicationContext);
		this.maxHeaderSize = RabbitUtils.getMaxFrame(template.getConnectionFactory())
				- RepublishMessageRecoverer.DEFAULT_FRAME_MAX_HEADROOM;
		assertThat(this.maxHeaderSize).isGreaterThan(0);
		RepublishMessageRecoverer recoverer = new RepublishMessageRecoverer(template, "", BIG_HEADER_QUEUE);
		ListenerExecutionFailedException cause = new ListenerExecutionFailedException("Listener failed",
				new RuntimeException(new String(new byte[this.maxHeaderSize]).replace('\u0000', 'x'),
						new IllegalStateException("foo")));
		recoverer.recover(new Message("foo".getBytes(), new MessageProperties()),
				cause);
		Message received = template.receive(BIG_HEADER_QUEUE, 10_000);
		assertThat(received).isNotNull();
		String trace = received.getMessageProperties().getHeaders()
				.get(RepublishMessageRecoverer.X_EXCEPTION_STACKTRACE).toString();
		assertThat(trace).contains("Caused by: java.lang.IllegalStateException");
		String exceptionMessage = received.getMessageProperties()
				.getHeader(RepublishMessageRecoverer.X_EXCEPTION_MESSAGE).toString();
		assertThat(trace.length() + exceptionMessage.length()).isEqualTo(this.maxHeaderSize);
		assertThat(exceptionMessage).endsWith("...");
		ccf.onApplicationEvent(new ContextClosedEvent(applicationContext));
		ccf.destroy();
	}

	private Throwable bigCause(Throwable cause) {
		int length = getStackTraceAsString(cause).length();
		int wantThisSize = this.maxHeaderSize + RepublishMessageRecoverer.DEFAULT_FRAME_MAX_HEADROOM;
		if (length > wantThisSize) {
			return cause;
		}
		String msg = length + BIG_EXCEPTION_MESSAGE1.length() > wantThisSize
				? BIG_EXCEPTION_MESSAGE1
				: BIG_EXCEPTION_MESSAGE2;
		return bigCause(new RuntimeException(msg, cause));
	}

	private String getStackTraceAsString(Throwable cause) {
		StringWriter stringWriter = new StringWriter();
		PrintWriter printWriter = new PrintWriter(stringWriter, true);
		cause.printStackTrace(printWriter);
		return stringWriter.getBuffer().toString();
	}

}
