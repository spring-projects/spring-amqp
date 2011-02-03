/*
 * Copyright 2002-2008 the original author or authors.
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

package org.springframework.amqp.rabbit.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
public final class RabbitNamespaceHandlerTests {

	@Autowired
	private Queue foo;

	private RabbitTemplate template;
	
	@Autowired
	public void setConnectionFactory(ConnectionFactory connectionFactory) {
		template = new RabbitTemplate(connectionFactory);
	}

	@Test
	public void testParse() throws Exception {
		assertNotNull(foo);
	}

	@Test
	public void testBindings() throws Exception {
		template.convertAndSend("direct-test", "foo", "Hello"); 
		assertEquals("Hello", template.receiveAndConvert("foo")); 
		template.convertAndSend("topic-test", "foo.bar", "Hello"); 
		assertEquals("Hello", template.receiveAndConvert("foo")); 
		template.convertAndSend("fanout-test", null, "Hello"); 
		assertEquals("Hello", template.receiveAndConvert("foo")); 
		template.convertAndSend("headers-test", null, "Hello", new MessagePostProcessor() {
			public Message postProcessMessage(Message message) throws AmqpException {
				message.getMessageProperties().setHeader("type", "foo");
				return message;
			}
		}); 
		assertEquals("Hello", template.receiveAndConvert("foo")); 
	}

}
