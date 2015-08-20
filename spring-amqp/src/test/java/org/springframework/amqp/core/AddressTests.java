/*
 * Copyright 2002-2015 the original author or authors.
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

import static org.junit.Assert.assertEquals;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Artem Bilan
 * @author Gary Russell
 */
public class AddressTests {

	@Test
	public void toStringCheck() {
		Address address = new Address("my-exchange", "routing-key");
		String replyToUri = "my-exchange/routing-key";
		Assert.assertEquals(replyToUri, address.toString());
	}

	@Test
	public void parse() {
		String replyToUri = "direct://my-exchange/routing-key";
		Address address = new Address(replyToUri);
		assertEquals("my-exchange", address.getExchangeName());
		assertEquals("routing-key", address.getRoutingKey());
	}

	@Test
	public void parseUnstructuredWithRoutingKeyOnly() {
		Address address = new Address("my-routing-key");
		assertEquals("my-routing-key", address.getRoutingKey());
		assertEquals("/my-routing-key", address.toString());

		address = new Address("/foo");
		assertEquals("foo", address.getRoutingKey());
		assertEquals("/foo", address.toString());

		address = new Address("bar/baz");
		assertEquals("bar", address.getExchangeName());
		assertEquals("baz", address.getRoutingKey());
		assertEquals("bar/baz", address.toString());
	}

	@Test
	public void parseWithoutRoutingKey() {
		Address address = new Address("fanout://my-exchange");
		assertEquals("my-exchange", address.getExchangeName());
		assertEquals("", address.getRoutingKey());
		assertEquals("my-exchange/", address.toString());
	}

	@Test
	public void parseWithDefaultExchangeAndRoutingKey() {
		Address address = new Address("direct:///routing-key");
		assertEquals("", address.getExchangeName());
		assertEquals("routing-key", address.getRoutingKey());
		assertEquals("/routing-key", address.toString());
	}

	@Test
	public void testEmpty() {
		Address address = new Address("/");
		assertEquals("", address.getExchangeName());
		assertEquals("", address.getRoutingKey());
		assertEquals("/", address.toString());
	}

	@Test
	public void testDirectReplyTo() {
		String replyTo = Address.AMQ_RABBITMQ_REPLY_TO + ".ab/cd/ef";
		MessageProperties props = new MessageProperties();
		props.setReplyTo(replyTo);
		Message message = new Message("foo".getBytes(), props);
		Address address = message.getMessageProperties().getReplyToAddress();
		assertEquals("", address.getExchangeName());
		assertEquals(replyTo, address.getRoutingKey());
		address = props.getReplyToAddress();
		assertEquals("", address.getExchangeName());
		assertEquals(replyTo, address.getRoutingKey());
	}

	@Test
	public void testEquals() {
		assertEquals(new Address("foo/bar"), new Address("foo/bar"));
	}

}
