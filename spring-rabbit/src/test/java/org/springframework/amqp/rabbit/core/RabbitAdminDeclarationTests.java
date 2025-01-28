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

package org.springframework.amqp.rabbit.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.impl.AMQImpl;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Binding.DestinationType;
import org.springframework.amqp.core.DeclarableCustomizer;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionListener;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @since 1.2
 *
 */
public class RabbitAdminDeclarationTests {

	@Test
	public void testUnconditional() throws Exception {
		ConnectionFactory cf = mock(ConnectionFactory.class);
		Connection conn = mock(Connection.class);
		Channel channel = mock(Channel.class);
		given(cf.createConnection()).willReturn(conn);
		given(conn.createChannel(false)).willReturn(channel);
		given(channel.queueDeclare("foo", true, false, false, new HashMap<>()))
				.willReturn(new AMQImpl.Queue.DeclareOk("foo", 0, 0));
		AtomicReference<ConnectionListener> listener = new AtomicReference<>();
		willAnswer(invocation -> {
			listener.set((ConnectionListener) invocation.getArguments()[0]);
			return null;
		}).given(cf).addConnectionListener(any(ConnectionListener.class));
		RabbitAdmin admin = new RabbitAdmin(cf);
		GenericApplicationContext context = new GenericApplicationContext();
		Queue queue = new Queue("foo");
		context.getBeanFactory().registerSingleton("foo", queue);
		DirectExchange exchange = new DirectExchange("bar");
		context.getBeanFactory().registerSingleton("bar", exchange);
		Binding binding = new Binding("foo", DestinationType.QUEUE, "bar", "foo", null);
		context.getBeanFactory().registerSingleton("baz", binding);
		context.refresh();
		admin.setApplicationContext(context);
		admin.afterPropertiesSet();
		assertThat(listener.get()).isNotNull();
		listener.get().onCreate(conn);

		verify(channel).queueDeclare("foo", true, false, false, new HashMap<>());
		verify(channel).exchangeDeclare("bar", "direct", true, false, false, new HashMap<>());
		verify(channel).queueBind("foo", "bar", "foo", new HashMap<>());
	}

	@Test
	public void testUnconditionalWithExplicitFactory() throws Exception {
		ConnectionFactory cf = mock(ConnectionFactory.class);
		Connection conn = mock(Connection.class);
		Channel channel = mock(Channel.class);
		given(cf.createConnection()).willReturn(conn);
		given(conn.createChannel(false)).willReturn(channel);
		given(channel.queueDeclare("foo", true, false, false, new HashMap<>()))
				.willReturn(new AMQImpl.Queue.DeclareOk("foo", 0, 0));
		AtomicReference<ConnectionListener> listener = new AtomicReference<>();
		willAnswer(invocation -> {
			listener.set(invocation.getArgument(0));
			return null;
		}).given(cf).addConnectionListener(any(ConnectionListener.class));
		RabbitAdmin admin = new RabbitAdmin(cf);
		GenericApplicationContext context = new GenericApplicationContext();
		Queue queue = new Queue("foo");
		queue.setAdminsThatShouldDeclare(admin);
		context.getBeanFactory().registerSingleton("foo", queue);
		DirectExchange exchange = new DirectExchange("bar");
		exchange.setAdminsThatShouldDeclare(admin);
		context.getBeanFactory().registerSingleton("bar", exchange);
		Binding binding = new Binding("foo", DestinationType.QUEUE, "bar", "foo", null);
		binding.setAdminsThatShouldDeclare(admin);
		context.getBeanFactory().registerSingleton("baz", binding);
		context.refresh();
		admin.setApplicationContext(context);
		admin.afterPropertiesSet();
		assertThat(listener.get()).isNotNull();
		listener.get().onCreate(conn);

		verify(channel).queueDeclare("foo", true, false, false, new HashMap<>());
		verify(channel).exchangeDeclare("bar", "direct", true, false, false, new HashMap<>());
		verify(channel).queueBind("foo", "bar", "foo", new HashMap<>());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testSkipBecauseDifferentFactory() throws Exception {
		ConnectionFactory cf = mock(ConnectionFactory.class);
		Connection conn = mock(Connection.class);
		Channel channel = mock(Channel.class);
		given(cf.createConnection()).willReturn(conn);
		given(conn.createChannel(false)).willReturn(channel);
		given(channel.queueDeclare("foo", true, false, false, null))
				.willReturn(new AMQImpl.Queue.DeclareOk("foo", 0, 0));
		AtomicReference<ConnectionListener> listener = new AtomicReference<>();
		willAnswer(invocation -> {
			listener.set(invocation.getArgument(0));
			return null;
		}).given(cf).addConnectionListener(any(ConnectionListener.class));
		RabbitAdmin admin = new RabbitAdmin(cf);
		RabbitAdmin other = new RabbitAdmin(cf);
		GenericApplicationContext context = new GenericApplicationContext();
		Queue queue = new Queue("foo");
		queue.setAdminsThatShouldDeclare(other);
		context.getBeanFactory().registerSingleton("foo", queue);
		DirectExchange exchange = new DirectExchange("bar");
		exchange.setAdminsThatShouldDeclare(other);
		context.getBeanFactory().registerSingleton("bar", exchange);
		Binding binding = new Binding("foo", DestinationType.QUEUE, "bar", "foo", null);
		binding.setAdminsThatShouldDeclare(other);
		context.getBeanFactory().registerSingleton("baz", binding);
		context.refresh();
		admin.setApplicationContext(context);
		admin.afterPropertiesSet();
		assertThat(listener.get()).isNotNull();
		listener.get().onCreate(conn);

		verify(channel, never()).queueDeclare(eq("foo"), anyBoolean(), anyBoolean(), anyBoolean(), any(Map.class));
		verify(channel, never())
				.exchangeDeclare(eq("bar"), eq("direct"), anyBoolean(), anyBoolean(), anyBoolean(), any(Map.class));
		verify(channel, never()).queueBind(eq("foo"), eq("bar"), eq("foo"), any(Map.class));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testSkipBecauseShouldNotDeclare() throws Exception {
		ConnectionFactory cf = mock(ConnectionFactory.class);
		Connection conn = mock(Connection.class);
		Channel channel = mock(Channel.class);
		given(cf.createConnection()).willReturn(conn);
		given(conn.createChannel(false)).willReturn(channel);
		given(channel.queueDeclare("foo", true, false, false, null))
				.willReturn(new AMQImpl.Queue.DeclareOk("foo", 0, 0));
		AtomicReference<ConnectionListener> listener = new AtomicReference<>();
		willAnswer(invocation -> {
			listener.set(invocation.getArgument(0));
			return null;
		}).given(cf).addConnectionListener(any(ConnectionListener.class));
		RabbitAdmin admin = new RabbitAdmin(cf);
		GenericApplicationContext context = new GenericApplicationContext();
		Queue queue = new Queue("foo");
		queue.setShouldDeclare(false);
		context.getBeanFactory().registerSingleton("foo", queue);
		DirectExchange exchange = new DirectExchange("bar");
		exchange.setShouldDeclare(false);
		context.getBeanFactory().registerSingleton("bar", exchange);
		Binding binding = new Binding("foo", DestinationType.QUEUE, "bar", "foo", null);
		binding.setShouldDeclare(false);
		context.getBeanFactory().registerSingleton("baz", binding);
		context.refresh();
		admin.setApplicationContext(context);
		admin.afterPropertiesSet();
		assertThat(listener.get()).isNotNull();
		listener.get().onCreate(conn);

		verify(channel, never()).queueDeclare(eq("foo"), anyBoolean(), anyBoolean(), anyBoolean(), any(Map.class));
		verify(channel, never())
				.exchangeDeclare(eq("bar"), eq("direct"), anyBoolean(), anyBoolean(), anyBoolean(), any(Map.class));
		verify(channel, never()).queueBind(eq("foo"), eq("bar"), eq("foo"), any(Map.class));
	}

	@Test
	public void testJavaConfig() throws Exception {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Config.class);
		Config.listener1.onCreate(Config.conn1);
		verify(Config.channel1).queueDeclare("foo", true, false, false, new HashMap<>());
		verify(Config.channel1, never()).queueDeclare("baz", true, false, false, new HashMap<>());
		verify(Config.channel1).queueDeclare("qux", true, false, false, new HashMap<>());
		verify(Config.channel1).exchangeDeclare("bar", "direct", true, false, true, new HashMap<>());
		verify(Config.channel1).queueBind("foo", "bar", "foo", new HashMap<>());
		Config.listener2.onCreate(Config.conn2);
		verify(Config.channel2, never())
				.queueDeclare(eq("foo"), anyBoolean(), anyBoolean(), anyBoolean(), isNull());
		verify(Config.channel1, never()).queueDeclare("baz", true, false, false, new HashMap<>());
		verify(Config.channel2).queueDeclare("qux", true, false, false, new HashMap<>());
		verify(Config.channel2, never())
				.exchangeDeclare(eq("bar"), eq("direct"), anyBoolean(), anyBoolean(),
						anyBoolean(), anyMap());
		verify(Config.channel2, never()).queueBind(eq("foo"), eq("bar"), eq("foo"), anyMap());
		Config.listener3.onCreate(Config.conn3);
		verify(Config.channel3, never())
				.queueDeclare(eq("foo"), anyBoolean(), anyBoolean(), anyBoolean(), isNull());
		Map<String, Object> args = new HashMap<>();
		args.put("added.by.customizer.1", true);
		args.put("added.by.customizer.2", true);
		verify(Config.channel3).queueDeclare("baz", true, false, false, args);
		verify(Config.channel3, never()).queueDeclare("qux", true, false, false, new HashMap<>());
		verify(Config.channel3, never())
				.exchangeDeclare(eq("bar"), eq("direct"), anyBoolean(), anyBoolean(),
						anyBoolean(), anyMap());
		verify(Config.channel3, never()).queueBind(eq("foo"), eq("bar"), eq("foo"), anyMap());

		context.close();
	}

	@Test
	public void testAddRemove() {
		Queue queue = new Queue("foo");
		ConnectionFactory cf = mock(ConnectionFactory.class);
		RabbitAdmin admin1 = new RabbitAdmin(cf);
		RabbitAdmin admin2 = new RabbitAdmin(cf);
		queue.setAdminsThatShouldDeclare(admin1, admin2);
		assertThat(queue.getDeclaringAdmins()).hasSize(2);
		queue.setAdminsThatShouldDeclare(admin1);
		assertThat(queue.getDeclaringAdmins()).hasSize(1);
		queue.setAdminsThatShouldDeclare(new Object[] {null});
		assertThat(queue.getDeclaringAdmins()).hasSize(0);
		queue.setAdminsThatShouldDeclare(admin1, admin2);
		assertThat(queue.getDeclaringAdmins()).hasSize(2);
		queue.setAdminsThatShouldDeclare();
		assertThat(queue.getDeclaringAdmins()).hasSize(0);
		queue.setAdminsThatShouldDeclare(admin1, admin2);
		assertThat(queue.getDeclaringAdmins()).hasSize(2);
		queue.setAdminsThatShouldDeclare((AmqpAdmin) null);
		assertThat(queue.getDeclaringAdmins()).hasSize(0);
		queue.setAdminsThatShouldDeclare(admin1, admin2);
		assertThat(queue.getDeclaringAdmins()).hasSize(2);
		queue.setAdminsThatShouldDeclare((Object[]) null);
		assertThat(queue.getDeclaringAdmins()).hasSize(0);
		assertThatIllegalArgumentException()
				.isThrownBy(() -> queue.setAdminsThatShouldDeclare(null, admin1))
				.withMessageContaining("'admins' cannot contain null elements");
	}

	@Test
	public void testNoOpWhenNothingToDeclare() throws Exception {
		com.rabbitmq.client.ConnectionFactory cf = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection connection = mock(com.rabbitmq.client.Connection.class);
		Channel channel = mock(Channel.class, "channel1");
		given(channel.isOpen()).willReturn(true);
		willReturn(connection).given(cf).newConnection(any(ExecutorService.class), anyString());
		given(connection.isOpen()).willReturn(true);
		given(connection.createChannel()).willReturn(channel);
		CachingConnectionFactory ccf = new CachingConnectionFactory(cf);
		ccf.setExecutor(mock(ExecutorService.class));
		RabbitTemplate rabbitTemplate = new RabbitTemplate(ccf);
		RabbitAdmin admin = new RabbitAdmin(rabbitTemplate);
		ApplicationContext ac = mock(ApplicationContext.class);
		admin.setApplicationContext(ac);
		admin.afterPropertiesSet();
		ccf.createConnection();
		verify(connection, never()).createChannel();
	}

	@Configuration
	public static class Config {

		private static final Connection conn1 = mock();

		private static final Connection conn2 = mock();

		private static final Connection conn3 = mock();

		private static final Channel channel1 = mock();

		private static final Channel channel2 = mock();

		private static final Channel channel3 = mock();

		private static ConnectionListener listener1;

		private static ConnectionListener listener2;

		private static ConnectionListener listener3;

		@Bean
		public ConnectionFactory cf1() throws IOException {
			ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
			given(connectionFactory.createConnection()).willReturn(conn1);
			given(conn1.createChannel(false)).willReturn(channel1);
			willAnswer(inv -> new AMQImpl.Queue.DeclareOk(inv.getArgument(0), 0, 0))
					.given(channel1)
					.queueDeclare(anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any());
			willAnswer(invocation -> {
				listener1 = invocation.getArgument(0);
				return null;
			}).given(connectionFactory).addConnectionListener(any(ConnectionListener.class));
			return connectionFactory;
		}

		@Bean
		public ConnectionFactory cf2() throws IOException {
			ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
			given(connectionFactory.createConnection()).willReturn(conn2);
			given(conn2.createChannel(false)).willReturn(channel2);
			willAnswer(inv -> new AMQImpl.Queue.DeclareOk(inv.getArgument(0), 0, 0))
					.given(channel2)
					.queueDeclare(anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any());
			willAnswer(invocation -> {
				listener2 = invocation.getArgument(0);
				return null;
			}).given(connectionFactory).addConnectionListener(any(ConnectionListener.class));
			return connectionFactory;
		}

		@Bean
		public ConnectionFactory cf3() throws IOException {
			ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
			given(connectionFactory.createConnection()).willReturn(conn3);
			given(conn3.createChannel(false)).willReturn(channel3);
			willAnswer(inv -> new AMQImpl.Queue.DeclareOk(inv.getArgument(0), 0, 0))
					.given(channel3)
					.queueDeclare(anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any());
			willAnswer(invocation -> {
				listener3 = invocation.getArgument(0);
				return null;
			}).given(connectionFactory).addConnectionListener(any(ConnectionListener.class));
			return connectionFactory;
		}

		@Bean
		public RabbitAdmin admin1() throws IOException {
			return new RabbitAdmin(cf1());
		}

		@Bean
		public RabbitAdmin admin2() throws IOException {
			return new RabbitAdmin(cf2());
		}

		@Bean
		public RabbitAdmin admin3() throws IOException {
			RabbitAdmin rabbitAdmin = new RabbitAdmin(cf3());
			rabbitAdmin.setExplicitDeclarationsOnly(true);
			return rabbitAdmin;
		}

		@Bean
		public Queue queueFoo() throws IOException {
			Queue queue = new Queue("foo");
			queue.setAdminsThatShouldDeclare(admin1());
			return queue;
		}

		@Bean
		public Queue queueBaz() throws IOException {
			Queue queue = new Queue("baz");
			queue.setAdminsThatShouldDeclare(admin3());
			return queue;
		}

		@Bean
		public Queue queueQux() {
			return new Queue("qux");
		}

		@Bean
		public Exchange exchange() throws IOException {
			DirectExchange exchange = new DirectExchange("bar");
			exchange.setAdminsThatShouldDeclare(admin1());
			exchange.setInternal(true);
			return exchange;
		}

		@Bean
		public Binding binding() throws IOException {
			Binding binding = new Binding("foo", DestinationType.QUEUE, exchange().getName(), "foo", null);
			binding.setAdminsThatShouldDeclare(admin1());
			return binding;
		}

		@Bean
		public DeclarableCustomizer customizer1() {
			return dec -> {
				if (dec instanceof Queue && ((Queue) dec).getName().equals("baz")) {
					dec.addArgument("added.by.customizer.1", true);
				}
				return dec;
			};
		}

		@Bean
		public DeclarableCustomizer customizer2() {
			return dec -> {
				if (dec instanceof Queue && ((Queue) dec).getName().equals("baz")) {
					dec.addArgument("added.by.customizer.2", true);
				}
				return dec;
			};
		}

	}

}
