/*
 * Copyright 2020-present the original author or authors.
 */

package org.springframework.amqp.rabbit.test.context;

import org.springframework.amqp.rabbit.annotation.RabbitBootstrapConfiguration;
import org.springframework.amqp.rabbit.config.AbstractRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.config.DirectRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerRunningSupport;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.rabbit.test.context.SpringRabbitTest.ContainerType;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.test.context.ContextCustomizer;
import org.springframework.test.context.MergedContextConfiguration;
import org.springframework.util.Assert;

/**
 * Adds infrastructure beans to the Spring test context.
 *
 * @author Gary Russell
 * @since 2.3
 *
 */
class SpringRabbitContextCustomizer implements ContextCustomizer {

	private final SpringRabbitTest springRabbitTest;

	SpringRabbitContextCustomizer(SpringRabbitTest test) {
		this.springRabbitTest = test;
	}

	@Override
	public void customizeContext(ConfigurableApplicationContext context, MergedContextConfiguration mergedConfig) {
		Assert.isInstanceOf(GenericApplicationContext.class, context);
		GenericApplicationContext applicationContext = (GenericApplicationContext) context;
		BrokerRunningSupport brokerRunning = RabbitAvailableCondition.getBrokerRunning();
		CachingConnectionFactory cf;
		if (brokerRunning != null) {
			cf = new CachingConnectionFactory(brokerRunning.getConnectionFactory());
		}
		else {
			cf = new CachingConnectionFactory(this.springRabbitTest.host(), this.springRabbitTest.port());
			cf.setUsername(this.springRabbitTest.user());
			cf.setPassword(this.springRabbitTest.password());
		}
		applicationContext.registerBean("autoConnectionFactory", CachingConnectionFactory.class, () -> cf);
		applicationContext.registerBean("autoRabbitTemplate", RabbitTemplate.class, () -> new RabbitTemplate(cf));
		applicationContext.registerBean("autoRabbitAdmin", RabbitAdmin.class, () -> new RabbitAdmin(cf));
		AbstractRabbitListenerContainerFactory<?> factory;
		if (this.springRabbitTest.containerType().equals(ContainerType.simple)) {
			factory = new SimpleRabbitListenerContainerFactory();
		}
		else {
			factory = new DirectRabbitListenerContainerFactory();
		}
		factory.setConnectionFactory(cf);
		applicationContext.registerBean("autoContainerFactory", AbstractRabbitListenerContainerFactory.class,
				() -> factory);
		new RabbitBootstrapConfiguration().registerBeanDefinitions(null,
				(BeanDefinitionRegistry) applicationContext.getBeanFactory());
	}

}
