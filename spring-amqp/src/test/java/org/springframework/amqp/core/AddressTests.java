/*
 * Copyright 2002-2025 the original author or authors.
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

package org.springframework.amqp.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Artem Bilan
 * @author Gary Russell
 * @author Ngoc Nhan
 */
public class AddressTests {

	@Test
	public void toStringCheck() {
		Address address = new Address("my-exchange", "routing-key");
		String replyToUri = "my-exchange/routing-key";
		assertThat(address.toString()).isEqualTo(replyToUri);
	}

	@Test
	public void parse() {
		String replyToUri = "direct://my-exchange/routing-key";
		Address address = new Address(replyToUri);
		assertThat(address.getExchangeName()).isEqualTo("my-exchange");
		assertThat(address.getRoutingKey()).isEqualTo("routing-key");
	}

	@Test
	public void parseUnstructuredWithRoutingKeyOnly() {
		Address address = new Address("my-routing-key");
		assertThat(address.getRoutingKey()).isEqualTo("my-routing-key");
		assertThat(address.toString()).isEqualTo("/my-routing-key");

		address = new Address("/foo");
		assertThat(address.getRoutingKey()).isEqualTo("foo");
		assertThat(address.toString()).isEqualTo("/foo");

		address = new Address("bar/baz");
		assertThat(address.getExchangeName()).isEqualTo("bar");
		assertThat(address.getRoutingKey()).isEqualTo("baz");
		assertThat(address.toString()).isEqualTo("bar/baz");
	}

	@Test
	public void parseWithoutRoutingKey() {
		Address address = new Address("fanout://my-exchange");
		assertThat(address.getExchangeName()).isEqualTo("my-exchange");
		assertThat(address.getRoutingKey()).isEqualTo("");
		assertThat(address.toString()).isEqualTo("my-exchange/");
	}

	@Test
	public void parseWithDefaultExchangeAndRoutingKey() {
		Address address = new Address("direct:///routing-key");
		assertThat(address.getExchangeName()).isEqualTo("");
		assertThat(address.getRoutingKey()).isEqualTo("routing-key");
		assertThat(address.toString()).isEqualTo("/routing-key");
	}

	@Test
	public void testEmpty() {
		Address address = new Address("/");
		assertThat(address.getExchangeName()).isEqualTo("");
		assertThat(address.getRoutingKey()).isEqualTo("");
		assertThat(address.toString()).isEqualTo("/");
	}

	@Test
	public void testDirectReplyTo() {
		String replyTo = Address.AMQ_RABBITMQ_REPLY_TO + ".ab/cd/ef";
		MessageProperties props = new MessageProperties();
		props.setReplyTo(replyTo);
		Message message = new Message("foo".getBytes(), props);
		Address address = message.getMessageProperties().getReplyToAddress();
		assertThat(address.getExchangeName()).isEqualTo("");
		assertThat(address.getRoutingKey()).isEqualTo(replyTo);
		address = props.getReplyToAddress();
		assertThat(address.getExchangeName()).isEqualTo("");
		assertThat(address.getRoutingKey()).isEqualTo(replyTo);
	}

	@Test
	public void testEquals() {
		assertThat(new Address("foo/bar")).isEqualTo(new Address("foo/bar"));
		assertThat(new Address("foo", null)).isEqualTo(new Address("foo", null));
		assertThat(new Address(null, "bar")).isEqualTo(new Address(null, "bar"));
		assertThat(new Address(null, null)).isEqualTo(new Address(null, null));
	}

}
