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

import static org.junit.Assert.assertEquals;
import junit.framework.Assert;

import org.junit.Test;

/**
 * @author Mark Pollack
 * @author Mark Fisher
 */
public class AddressTests {

	@Test
	public void toStringCheck() {
		Address address = new Address(ExchangeType.direct, "my-exchange", "routing-key");
		String replyToUri = "direct://my-exchange/routing-key";
		Assert.assertEquals(replyToUri, address.toString());
	}

	@Test
	public void parse() {
		String replyToUri = "direct://my-exchange/routing-key";
		Address address = new Address(replyToUri);
		assertEquals(address.getExchangeType(), ExchangeType.direct);
		assertEquals(address.getExchangeName(), "my-exchange");
		assertEquals(address.getRoutingKey(), "routing-key");
	}

	@Test
	public void parseUnstructuredWithRoutingKeyOnly() {
		Address address = new Address("my-routing-key");
		assertEquals("my-routing-key", address.getRoutingKey());
		assertEquals("direct:///my-routing-key", address.toString());
	}

	@Test
	public void parseWithoutRoutingKey() {
		Address address = new Address("fanout://my-exchange");
		assertEquals(ExchangeType.fanout, address.getExchangeType());
		assertEquals("my-exchange", address.getExchangeName());
		assertEquals("", address.getRoutingKey());
		assertEquals("fanout://my-exchange/", address.toString());
	}

	@Test
	public void parseWithDefaultExchangeAndRoutingKey() {
		Address address = new Address("direct:///routing-key");
		assertEquals(ExchangeType.direct, address.getExchangeType());
		assertEquals("", address.getExchangeName());
		assertEquals("routing-key", address.getRoutingKey());
		assertEquals("direct:///routing-key", address.toString());
	}

}
