/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.config;

import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.env.StandardEnvironment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;

/**
 * @author Gary Russell
 * @author Gunnar Hillert
 * @author Ngoc Nhan
 * @since 1.2
 *
 */
@RabbitAvailable
public class MismatchedQueueDeclarationTests {

	private SingleConnectionFactory connectionFactory;

	private RabbitAdmin admin;

	@BeforeEach
	public void setup() throws Exception {
		connectionFactory = new SingleConnectionFactory();
		connectionFactory.setHost("localhost");
		this.admin = new RabbitAdmin(this.connectionFactory);
		deleteQueues();
	}

	@AfterEach
	public void deleteQueues() throws Exception {
		this.admin.deleteQueue("mismatch.foo");
		this.admin.deleteQueue("mismatch.bar");

		((DisposableBean) connectionFactory).destroy();
	}

	@Test
	@Disabled
	public void testAdminFailsWithMismatchedQueue() throws Exception {
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext();
		context.setConfigLocation("org/springframework/amqp/rabbit/config/MismatchedQueueDeclarationTests-context.xml");
		StandardEnvironment env = new StandardEnvironment();
		env.addActiveProfile("basicAdmin");
		env.addActiveProfile("basic");
		context.setEnvironment(env);
		context.refresh();
		context.getBean(CachingConnectionFactory.class).createConnection();
		context.close();
		Channel channel = this.connectionFactory.createConnection().createChannel(false);
		channel.queueDeclarePassive("mismatch.bar");
		this.admin.deleteQueue("mismatch.bar");
		assertThat(this.admin.getQueueProperties("mismatch.foo")).isNotNull();
		assertThat(this.admin.getQueueProperties("mismatch.bar")).isNull();

		env = new StandardEnvironment();
		env.addActiveProfile("basicAdmin");
		env.addActiveProfile("ttl");
		context.setEnvironment(env);
		context.refresh();
		this.connectionFactory.createConnection().createChannel(false);
		assertThatException().isThrownBy(context.getBean(CachingConnectionFactory.class)::createConnection)
				.satisfies(ex -> {
					assertThat(ex.getCause().getCause().getMessage()).contains("inequivalent arg 'x-message-ttl'");
				});
		assertThat(this.admin.getQueueProperties("mismatch.foo")).isNotNull();
		assertThat(this.admin.getQueueProperties("mismatch.bar")).isNull();
		context.close();
	}

	@Test
	public void testAdminSkipsMismatchedQueue() throws Exception {
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext();
		context.setConfigLocation("org/springframework/amqp/rabbit/config/MismatchedQueueDeclarationTests-context.xml");
		StandardEnvironment env = new StandardEnvironment();
		env.addActiveProfile("advancedAdmin");
		env.addActiveProfile("basic");
		context.setEnvironment(env);
		context.refresh();
		context.getBean(CachingConnectionFactory.class).createConnection();
		context.close();
		Channel channel = this.connectionFactory.createConnection().createChannel(false);
		channel.queueDeclarePassive("mismatch.bar");
		this.admin.deleteQueue("mismatch.bar");
		assertThat(this.admin.getQueueProperties("mismatch.foo")).isNotNull();
		assertThat(this.admin.getQueueProperties("mismatch.bar")).isNull();

		context = new ClassPathXmlApplicationContext();
		context.setConfigLocation("org/springframework/amqp/rabbit/config/MismatchedQueueDeclarationTests-context.xml");
		env = new StandardEnvironment();
		env.addActiveProfile("advancedAdmin");
		env.addActiveProfile("ttl");
		context.setEnvironment(env);
		context.refresh();
		channel = this.connectionFactory.createConnection().createChannel(false);
		context.getBean(CachingConnectionFactory.class).createConnection();
		assertThat(this.admin.getQueueProperties("mismatch.foo")).isNotNull();
		assertThat(this.admin.getQueueProperties("mismatch.bar")).isNotNull();
		context.close();
	}

}
