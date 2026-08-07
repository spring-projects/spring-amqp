/*
 * Copyright 2026-present the original author or authors.
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

package org.springframework.amqp.rabbitmq.client;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.rabbitmq.client.amqp.Connection;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.connection.SimpleResourceHolder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Robin Collard
 *
 * @since 4.2
 */
public class RoutingAmqpConnectionFactoryTests {

	@Test
	void routesToTargetByLookupKey() {
		AmqpConnectionFactory cf1 = mock();
		AmqpConnectionFactory cf2 = mock();
		Connection connection1 = mock();
		Connection connection2 = mock();
		given(cf1.getConnection()).willReturn(connection1);
		given(cf2.getConnection()).willReturn(connection2);

		Map<Object, AmqpConnectionFactory> factories = new HashMap<>();
		factories.put("one", cf1);
		factories.put("two", cf2);

		AtomicReference<@Nullable Object> lookupKey = new AtomicReference<>();
		AbstractRoutingAmqpConnectionFactory routingConnectionFactory =
				new AbstractRoutingAmqpConnectionFactory() {

					@Override
					protected @Nullable Object determineCurrentLookupKey() {
						return lookupKey.get();
					}

				};
		routingConnectionFactory.setTargetConnectionFactories(factories);

		lookupKey.set("one");
		assertThat(routingConnectionFactory.getConnection()).isSameAs(connection1);
		lookupKey.set("two");
		assertThat(routingConnectionFactory.getConnection()).isSameAs(connection2);

		assertThat(routingConnectionFactory.getTargetConnectionFactory("one")).isSameAs(cf1);
		verify(cf1).getConnection();
		verify(cf2).getConnection();
	}

	@Test
	void lenientFallbackToDefault() {
		AmqpConnectionFactory target = mock();
		AmqpConnectionFactory defaultCf = mock();
		Connection defaultConnection = mock();
		given(defaultCf.getConnection()).willReturn(defaultConnection);

		AbstractRoutingAmqpConnectionFactory routingConnectionFactory =
				new AbstractRoutingAmqpConnectionFactory() {

					@Override
					protected @Nullable Object determineCurrentLookupKey() {
						return "missing";
					}

				};
		routingConnectionFactory.setTargetConnectionFactories(Map.of("present", target));
		routingConnectionFactory.setDefaultTargetConnectionFactory(defaultCf);

		assertThat(routingConnectionFactory.getConnection()).isSameAs(defaultConnection);

		routingConnectionFactory.setLenientFallback(false);
		assertThatIllegalStateException()
				.isThrownBy(routingConnectionFactory::getConnection)
				.withMessageContaining("Cannot determine target AmqpConnectionFactory for lookup key [missing]");
	}

	@Test
	void afterPropertiesSetRequiresAtLeastOneFactory() {
		AbstractRoutingAmqpConnectionFactory routingConnectionFactory =
				new AbstractRoutingAmqpConnectionFactory() {

					@Override
					protected @Nullable Object determineCurrentLookupKey() {
						return null;
					}

				};

		assertThatIllegalArgumentException()
				.isThrownBy(routingConnectionFactory::afterPropertiesSet)
				.withMessageContaining("At least one target factory (or default) is required");

		routingConnectionFactory.setDefaultTargetConnectionFactory(mock());
		assertThatNoException().isThrownBy(routingConnectionFactory::afterPropertiesSet);
	}

	@Test
	void nullTargetValuesAreRejected() {
		AbstractRoutingAmqpConnectionFactory routingConnectionFactory =
				new AbstractRoutingAmqpConnectionFactory() {

					@Override
					protected @Nullable Object determineCurrentLookupKey() {
						return null;
					}

				};
		Map<Object, AmqpConnectionFactory> factories = new HashMap<>();
		factories.put("key", null);

		assertThatIllegalArgumentException()
				.isThrownBy(() -> routingConnectionFactory.setTargetConnectionFactories(factories))
				.withMessageContaining("cannot have null values");
	}

	@Test
	void addTargetConnectionFactoryRegistersTargetAtRuntime() {
		AmqpConnectionFactory cf = mock();
		AmqpConnectionFactory replacement = mock();
		Connection connection = mock();
		Connection replacementConnection = mock();
		given(cf.getConnection()).willReturn(connection);
		given(replacement.getConnection()).willReturn(replacementConnection);

		AbstractRoutingAmqpConnectionFactory routingConnectionFactory =
				new AbstractRoutingAmqpConnectionFactory() {

					@Override
					protected @Nullable Object determineCurrentLookupKey() {
						return "added";
					}

				};
		routingConnectionFactory.setLenientFallback(false);

		assertThat(routingConnectionFactory.getTargetConnectionFactory("added")).isNull();
		assertThatIllegalStateException()
				.isThrownBy(routingConnectionFactory::getConnection)
				.withMessageContaining("Cannot determine target AmqpConnectionFactory for lookup key [added]");

		routingConnectionFactory.addTargetConnectionFactory("added", cf);

		assertThat(routingConnectionFactory.getTargetConnectionFactory("added")).isSameAs(cf);
		assertThat(routingConnectionFactory.getConnection()).isSameAs(connection);

		routingConnectionFactory.addTargetConnectionFactory("added", replacement);

		assertThat(routingConnectionFactory.getTargetConnectionFactory("added")).isSameAs(replacement);
		assertThat(routingConnectionFactory.getConnection()).isSameAs(replacementConnection);
	}

	@Test
	void removeTargetConnectionFactoryUnregistersTarget() {
		AmqpConnectionFactory cf = mock();
		AmqpConnectionFactory defaultCf = mock();
		Connection connection = mock();
		Connection defaultConnection = mock();
		given(cf.getConnection()).willReturn(connection);
		given(defaultCf.getConnection()).willReturn(defaultConnection);

		AbstractRoutingAmqpConnectionFactory routingConnectionFactory =
				new AbstractRoutingAmqpConnectionFactory() {

					@Override
					protected @Nullable Object determineCurrentLookupKey() {
						return "removable";
					}

				};
		routingConnectionFactory.setTargetConnectionFactories(Map.of("removable", cf));
		routingConnectionFactory.setDefaultTargetConnectionFactory(defaultCf);

		assertThat(routingConnectionFactory.getConnection()).isSameAs(connection);

		assertThat(routingConnectionFactory.removeTargetConnectionFactory("removable")).isSameAs(cf);

		assertThat(routingConnectionFactory.getTargetConnectionFactory("removable")).isNull();
		assertThat(routingConnectionFactory.getConnection()).isSameAs(defaultConnection);

		assertThat(routingConnectionFactory.removeTargetConnectionFactory("removable")).isNull();
	}

	@Test
	void simpleRoutingResolvesViaResourceHolder() {
		AmqpConnectionFactory cf = mock();
		Connection connection = mock();
		given(cf.getConnection()).willReturn(connection);

		SimpleRoutingAmqpConnectionFactory routingConnectionFactory = new SimpleRoutingAmqpConnectionFactory();
		routingConnectionFactory.setTargetConnectionFactories(Map.of("lookup", cf));

		SimpleResourceHolder.bind(routingConnectionFactory, "lookup");
		try {
			assertThat(routingConnectionFactory.getConnection()).isSameAs(connection);
		}
		finally {
			SimpleResourceHolder.unbind(routingConnectionFactory);
		}
	}

}
