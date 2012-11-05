/*
 * Copyright 2002-2010 the original author or authors.
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
package org.springframework.amqp.core;

import static org.junit.Assert.*;

import org.junit.Test;


/**
 * @author Dave Syer
 *
 */
public class MessagePropertiesTests {
	
	@Test
	public void testReplyTo() throws Exception {
		MessageProperties properties = new MessageProperties();
		properties.setReplyTo("fanout://foo/bar");
		assertEquals("bar", properties.getReplyToAddress().getRoutingKey());
	}

	@Test
	public void testReplyToNullByDefault() throws Exception {
		MessageProperties properties = new MessageProperties();
		assertEquals(null, properties.getReplyTo());
		assertEquals(null, properties.getReplyToAddress());
	}

}
