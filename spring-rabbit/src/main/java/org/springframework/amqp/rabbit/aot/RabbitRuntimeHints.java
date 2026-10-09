/*
 * Copyright 2022-present the original author or authors.
 */

package org.springframework.amqp.rabbit.aot;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.rabbit.connection.ChannelProxy;
import org.springframework.amqp.rabbit.connection.PublisherCallbackChannel;
import org.springframework.aop.SpringProxy;
import org.springframework.aop.framework.Advised;
import org.springframework.aot.hint.ProxyHints;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.aot.hint.TypeReference;
import org.springframework.core.DecoratingProxy;

/**
 * {@link RuntimeHintsRegistrar} for spring-rabbit.
 *
 * @author Gary Russell
 * @since 3.0
 *
 */
public class RabbitRuntimeHints implements RuntimeHintsRegistrar {

	@Override
	public void registerHints(RuntimeHints hints, @Nullable ClassLoader classLoader) {
		ProxyHints proxyHints = hints.proxies();
		proxyHints.registerJdkProxy(ChannelProxy.class);
		proxyHints.registerJdkProxy(ChannelProxy.class, PublisherCallbackChannel.class);
		proxyHints.registerJdkProxy(builder ->
				builder.proxiedInterfaces(TypeReference.of(
						"org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer$ContainerDelegate"))
						.proxiedInterfaces(SpringProxy.class, Advised.class, DecoratingProxy.class));
	}

}
