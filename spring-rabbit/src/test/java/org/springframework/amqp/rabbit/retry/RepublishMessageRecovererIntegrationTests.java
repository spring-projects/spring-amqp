/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.amqp.rabbit.retry;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

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

import com.rabbitmq.client.LongString;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.0.5
 *
 */
@RabbitAvailable(queues = RepublishMessageRecovererIntegrationTests.BIG_HEADER_QUEUE)
public class RepublishMessageRecovererIntegrationTests {

	public static final String BIG_HEADER_QUEUE = "big.header.queue";

	private static final String BIG_EXCEPTION_MESSAGE = new String(new byte[10_000]).replaceAll("\u0000", "x");

	private long maxHeaderSize;

	@Test
	public void testBigHeader() {
		RabbitTemplate template = new RabbitTemplate(
				new CachingConnectionFactory(RabbitAvailableCondition.getBrokerRunning().getConnectionFactory()));
		this.maxHeaderSize = RabbitUtils.getMaxFrame(template.getConnectionFactory()) - 20_000;
		assertThat(this.maxHeaderSize, greaterThan(0L));
		RepublishMessageRecoverer recoverer = new RepublishMessageRecoverer(template, "", BIG_HEADER_QUEUE);
		recoverer.recover(new Message("foo".getBytes(), new MessageProperties()),
				bigCause(new RuntimeException(BIG_EXCEPTION_MESSAGE)));
		Message received = template.receive(BIG_HEADER_QUEUE, 10_000);
		assertNotNull(received);
		assertThat(((LongString) received.getMessageProperties().getHeaders()
				.get(RepublishMessageRecoverer.X_EXCEPTION_STACKTRACE)).length(), equalTo(this.maxHeaderSize));
	}

	private Throwable bigCause(Throwable cause) {
		if (getStackTraceAsString(cause).length() > this.maxHeaderSize) {
			return cause;
		}
		return bigCause(new RuntimeException(BIG_EXCEPTION_MESSAGE, cause));
	}

	private String getStackTraceAsString(Throwable cause) {
		StringWriter stringWriter = new StringWriter();
		PrintWriter printWriter = new PrintWriter(stringWriter, true);
		cause.printStackTrace(printWriter);
		return stringWriter.getBuffer().toString();
	}

}
