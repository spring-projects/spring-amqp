/*
 * Copyright 2022 the original author or authors.
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

package org.springframework.amqp.rabbit.aot;

import org.springframework.amqp.rabbit.connection.ChannelProxy;
import org.springframework.amqp.rabbit.connection.PublisherCallbackChannel;
import org.springframework.aop.SpringProxy;
import org.springframework.aop.framework.Advised;
import org.springframework.aot.hint.ProxyHints;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.aot.hint.TypeReference;
import org.springframework.core.DecoratingProxy;
import org.springframework.lang.Nullable;

/**
 * {@link RuntimeHintsRegistrar} for spring-rabbit.
 *
 * @author Gary Russell
 * @since 3.0
 *
 */
public class RabbitRuntimeHints implements RuntimeHintsRegistrar {

	@Override
	public void registerHints(RuntimeHints hints, @Nullable	ClassLoader classLoader) {
		ProxyHints proxyHints = hints.proxies();
		proxyHints.registerJdkProxy(ChannelProxy.class);
		proxyHints.registerJdkProxy(ChannelProxy.class, PublisherCallbackChannel.class);
		proxyHints.registerJdkProxy(builder ->
				builder.proxiedInterfaces(TypeReference.of(
						"org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer$ContainerDelegate"))
						.proxiedInterfaces(SpringProxy.class, Advised.class, DecoratingProxy.class));
	}

}
