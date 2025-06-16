/*
 * Copyright 2021-present the original author or authors.
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

package org.springframework.rabbit.stream.support.converter;

import com.rabbitmq.stream.MessageHandler.Context;
import com.rabbitmq.stream.Properties;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Message;
import org.springframework.rabbit.stream.support.StreamMessageProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @since 2.4
 *
 */
public class DefaultStreamMessageConverterTests {

	@Test
	void toAndFrom() {
		StreamMessageProperties smp = new StreamMessageProperties(mock(Context.class));
		smp.setMessageId("test");
		smp.setUserId("user");
		smp.setTo("to");
		smp.setSubject("subject");
		smp.setReplyTo("replyTo");
		smp.setCorrelationId("correlation");
		smp.setContentType("application/json");
		smp.setContentEncoding("UTF-8");
		smp.setExpiration("42");
		smp.setCreationTime(43L);
		smp.setGroupId("groupId");
		smp.setGroupSequence(44L);
		smp.setReplyToGroupId("replyGroupId");
		smp.setHeader("foo", "bar");
		DefaultStreamMessageConverter converter = new DefaultStreamMessageConverter();
		Message msg = new Message("foo".getBytes(), smp);
		com.rabbitmq.stream.Message streamMessage = converter.fromMessage(msg);
		Properties props = streamMessage.getProperties();
		assertThat(props.getMessageIdAsString()).isEqualTo("test");
		assertThat(props.getUserId()).isEqualTo("user".getBytes());
		assertThat(props.getTo()).isEqualTo("to");
		assertThat(props.getSubject()).isEqualTo("subject");
		assertThat(props.getReplyTo()).isEqualTo("replyTo");
		assertThat(props.getCorrelationIdAsString()).isEqualTo("correlation");
		assertThat(props.getContentType()).isEqualTo("application/json");
		assertThat(props.getContentEncoding()).isEqualTo("UTF-8");
		assertThat(props.getAbsoluteExpiryTime()).isEqualTo(42L);
		assertThat(props.getCreationTime()).isEqualTo(43L);
		assertThat(props.getGroupId()).isEqualTo("groupId");
		assertThat(props.getGroupSequence()).isEqualTo(44L);
		assertThat(props.getReplyToGroupId()).isEqualTo("replyGroupId");
		assertThat(streamMessage.getApplicationProperties().get("foo")).isEqualTo("bar");

		StreamMessageProperties smp2 = new StreamMessageProperties(mock(Context.class));
		msg = converter.toMessage(streamMessage, smp2);
		assertThat(msg.getMessageProperties()).isSameAs(smp2);
		assertThat(smp2).isEqualTo(smp);
	}

}
