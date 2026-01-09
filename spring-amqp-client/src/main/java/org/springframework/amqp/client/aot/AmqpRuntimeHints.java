/*
 * Copyright 2022-present the original author or authors.
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

package org.springframework.amqp.client.aot;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.stream.Stream;

import org.apache.qpid.protonj2.client.impl.ClientReceiver;
import org.apache.qpid.protonj2.engine.impl.ProtonReceiver;
import org.apache.qpid.protonj2.engine.impl.ProtonSessionIncomingWindow;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.MessageListener;
import org.springframework.aot.hint.ExecutableMode;
import org.springframework.aot.hint.ProxyHints;
import org.springframework.aot.hint.ReflectionHints;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.util.ReflectionUtils;

/**
 * The {@link RuntimeHintsRegistrar} for {@code spring-amqp-client}.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 */
public class AmqpRuntimeHints implements RuntimeHintsRegistrar {

	@Override
	public void registerHints(RuntimeHints hints, @Nullable ClassLoader classLoader) {
		ProxyHints proxyHints = hints.proxies();
		proxyHints.registerJdkProxy(MessageListener.class);

		ReflectionHints reflection = hints.reflection();

		Method protonLink = ReflectionUtils.findMethod(ClientReceiver.class, "protonLink");
		Method getCreditState = ReflectionUtils.findMethod(ProtonReceiver.class, "getCreditState");
		Method writeFlow =
				ReflectionUtils.findMethod(ProtonSessionIncomingWindow.class, "writeFlow", ProtonReceiver.class);

		Stream.of(protonLink, getCreditState, writeFlow)
				.forEach(method -> reflection.registerMethod(method, ExecutableMode.INVOKE));

		Field sessionWindow = ReflectionUtils.findField(ProtonReceiver.class, "sessionWindow");
		if (sessionWindow != null) {
			reflection.registerField(sessionWindow);
		}
	}

}
