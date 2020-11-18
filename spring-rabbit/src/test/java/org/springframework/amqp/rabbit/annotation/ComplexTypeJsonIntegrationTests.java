/*
 * Copyright 2016-2020 the original author or authors.
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

package org.springframework.amqp.rabbit.annotation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.AsyncRabbitTemplate;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate.RabbitConverterFuture;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.LogLevels;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessagingMessageListenerAdapter;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Gary Russell
 * @since 2.0
 *
 */
@SpringJUnitConfig
@DirtiesContext
@RabbitAvailable(queues = { ComplexTypeJsonIntegrationTests.TEST_QUEUE, ComplexTypeJsonIntegrationTests.TEST_QUEUE2 })
@LogLevels(classes = { RabbitTemplate.class,
		MessagingMessageListenerAdapter.class,
		SimpleMessageListenerContainer.class })
public class ComplexTypeJsonIntegrationTests {

	public static final String TEST_QUEUE = "test.complex.send.and.receive";

	public static final String TEST_QUEUE2 = "test.complex.receive";

	@Autowired
	private RabbitTemplate rabbitTemplate;

	@Autowired
	private AsyncRabbitTemplate asyncTemplate;

	private static Foo<Bar<Baz, Qux>> makeAFoo() {
		Foo<Bar<Baz, Qux>> foo = new Foo<>();
		Bar<Baz, Qux> bar = new Bar<>();
		bar.setaField(new Baz("foo"));
		bar.setbField(new Qux(42));
		foo.setField(bar);
		return foo;
	}

	/*
	 * Covers all flavors of convertSendAndReceiveAsType
	 */
	@Test
	public void testSendAndReceive() throws Exception {
		verifyFooBarBazQux(this.rabbitTemplate.convertSendAndReceiveAsType("foo",
				new ParameterizedTypeReference<Foo<Bar<Baz, Qux>>>() { }));
		verifyFooBarBazQux(this.rabbitTemplate.convertSendAndReceiveAsType("foo",
				m -> m,
				new ParameterizedTypeReference<Foo<Bar<Baz, Qux>>>() { }));
		verifyFooBarBazQux(this.rabbitTemplate.convertSendAndReceiveAsType(TEST_QUEUE, "foo",
				new ParameterizedTypeReference<Foo<Bar<Baz, Qux>>>() { }));
		verifyFooBarBazQux(this.rabbitTemplate.convertSendAndReceiveAsType(TEST_QUEUE, "foo",
				m -> m,
				new ParameterizedTypeReference<Foo<Bar<Baz, Qux>>>() { }));
		verifyFooBarBazQux(this.rabbitTemplate.convertSendAndReceiveAsType("", TEST_QUEUE, "foo",
				new ParameterizedTypeReference<Foo<Bar<Baz, Qux>>>() { }));
		verifyFooBarBazQux(this.rabbitTemplate.convertSendAndReceiveAsType("", TEST_QUEUE, "foo",
				m -> m,
				new ParameterizedTypeReference<Foo<Bar<Baz, Qux>>>() { }));
	}

	@Test
	public void testReceive() {
		this.rabbitTemplate.convertAndSend(TEST_QUEUE2, makeAFoo(), m -> {
			m.getMessageProperties().getHeaders().remove("__TypeId__");
			return m;
		});
		verifyFooBarBazQux(
			this.rabbitTemplate.receiveAndConvert(10_000, new ParameterizedTypeReference<Foo<Bar<Baz, Qux>>>() { }));
	}

	@Test
	public void testReceiveNoWait() throws Exception {
		this.rabbitTemplate.convertAndSend(TEST_QUEUE2, makeAFoo(), m -> {
			m.getMessageProperties().getHeaders().remove("__TypeId__");
			return m;
		});
		Foo<Bar<Baz, Qux>> foo = await().until(
				() -> this.rabbitTemplate.receiveAndConvert(new ParameterizedTypeReference<Foo<Bar<Baz, Qux>>>() { }),
				msg -> msg != null);
		verifyFooBarBazQux(foo);
	}

	@Test
	public void testAsyncSendAndReceive() throws Exception {
		verifyFooBarBazQux(this.asyncTemplate.convertSendAndReceiveAsType("foo",
				new ParameterizedTypeReference<Foo<Bar<Baz, Qux>>>() { }));
		verifyFooBarBazQux(this.asyncTemplate.convertSendAndReceiveAsType("foo",
				m -> m,
				new ParameterizedTypeReference<Foo<Bar<Baz, Qux>>>() { }));
		verifyFooBarBazQux(this.asyncTemplate.convertSendAndReceiveAsType(TEST_QUEUE, "foo",
				new ParameterizedTypeReference<Foo<Bar<Baz, Qux>>>() { }));
		verifyFooBarBazQux(this.asyncTemplate.convertSendAndReceiveAsType(TEST_QUEUE, "foo",
				m -> m,
				new ParameterizedTypeReference<Foo<Bar<Baz, Qux>>>() { }));
		verifyFooBarBazQux(this.asyncTemplate.convertSendAndReceiveAsType("", TEST_QUEUE, "foo",
				new ParameterizedTypeReference<Foo<Bar<Baz, Qux>>>() { }));
		verifyFooBarBazQux(this.asyncTemplate.convertSendAndReceiveAsType("", TEST_QUEUE, "foo",
				m -> m,
				new ParameterizedTypeReference<Foo<Bar<Baz, Qux>>>() { }));
	}

	private void verifyFooBarBazQux(RabbitConverterFuture<Foo<Bar<Baz, Qux>>> future) throws Exception {
		verifyFooBarBazQux(future.get(10, TimeUnit.SECONDS));
	}

	private void verifyFooBarBazQux(Foo<?> foo) {
		assertThat(foo).isNotNull();
		Bar<?, ?> bar;
		assertThat(foo.getField()).isInstanceOf(Bar.class);
		bar = (Bar<?, ?>) foo.getField();
		assertThat(bar.getaField()).isInstanceOf(Baz.class);
		assertThat(bar.getbField()).isInstanceOf(Qux.class);
	}

	@Configuration
	@EnableRabbit
	public static class ContextConfig {

		@Bean
		public ConnectionFactory cf() {
			return new CachingConnectionFactory("localhost");
		}

		@Bean
		public RabbitTemplate template() {
			RabbitTemplate rabbitTemplate = new RabbitTemplate(cf());
			rabbitTemplate.setRoutingKey(TEST_QUEUE);
			rabbitTemplate.setDefaultReceiveQueue(TEST_QUEUE2);
			rabbitTemplate.setMessageConverter(jsonMessageConverter());
			return rabbitTemplate;
		}

		@Bean
		public AsyncRabbitTemplate asyncTemplate() {
			return new AsyncRabbitTemplate(template());
		}

		@Bean
		public MessageConverter jsonMessageConverter() {
			return new Jackson2JsonMessageConverter();
		}

		@Bean
		public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(cf());
			factory.setMessageConverter(jsonMessageConverter());
			return factory;
		}

		@Bean
		public Listener listener() {
			return new Listener();
		}

	}

	public static class Listener {

		@RabbitListener(queues = TEST_QUEUE)
		public Foo<Bar<Baz, Qux>> listen(String in) {
			return makeAFoo();
		}

	}

	public static class Foo<T> {

		private T field;

		public T getField() {
			return this.field;
		}

		public void setField(T field) {
			this.field = field;
		}

		@Override
		public String toString() {
			return "Foo [field=" + this.field + "]";
		}

	}

	public static class Bar<A, B>  {

		private A aField;

		private B bField;

		public A getaField() {
			return this.aField;
		}

		public void setaField(A aField) {
			this.aField = aField;
		}

		public B getbField() {
			return this.bField;
		}

		public void setbField(B bField) {
			this.bField = bField;
		}

		@Override
		public String toString() {
			return "Bar [aField=" + this.aField + ", bField=" + this.bField + "]";
		}

	}

	public static class Baz {

		private String baz;

		Baz() {
		}

		public Baz(String string) {
			this.baz = string;
		}

		public String getBaz() {
			return this.baz;
		}

		public void setBaz(String baz) {
			this.baz = baz;
		}

		@Override
		public String toString() {
			return "Baz [baz=" + this.baz + "]";
		}

	}

	public static class Qux {

		private Integer qux;

		Qux() {
		}

		public Qux(int i) {
			this.qux = i;
		}

		public Integer getQux() {
			return this.qux;
		}

		public void setQux(Integer qux) {
			this.qux = qux;
		}

		@Override
		public String toString() {
			return "Qux [qux=" + this.qux + "]";
		}

	}

}
