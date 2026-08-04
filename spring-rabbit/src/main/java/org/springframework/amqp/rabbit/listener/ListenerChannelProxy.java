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

package org.springframework.amqp.rabbit.listener;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.function.Supplier;

import com.rabbitmq.client.Channel;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.rabbit.connection.ChannelProxy;

/**
 * A {@link ChannelProxy} for a listener container to expose to listener code - a
 * {@link org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener}, or a
 * {@code @RabbitListener} method with a {@link Channel} argument - instead of the
 * consumer's channel directly.
 * <p>
 * Every call is delegated to the consumer's channel. In addition, the deliveries the
 * listener settles itself, via {@code basicAck}, {@code basicNack}, {@code basicReject} or
 * {@code basicRecover}, are reported to a {@link SettledTagCallback} once the broker has
 * accepted the call. A container has to know about those: re-settling an already-settled
 * delivery is an AMQP protocol violation rather than a per-call error, so the broker
 * closes the whole channel with {@code PRECONDITION_FAILED - unknown delivery tag},
 * failing everything else in progress on it.
 *
 * @author Artem Bilan
 *
 * @since 4.2
 */
final class ListenerChannelProxy implements InvocationHandler {

	private final Supplier<Channel> channelSupplier;

	private final SettledTagCallback settledTagCallback;

	private ListenerChannelProxy(Supplier<Channel> channelSupplier, SettledTagCallback settledTagCallback) {
		this.channelSupplier = channelSupplier;
		this.settledTagCallback = settledTagCallback;
	}

	/**
	 * Create a {@link Channel} proxy to hand to listener code.
	 * @param channelSupplier supplies the channel to delegate to; it is called on every
	 * invocation, hence the proxy may be created before the consumer has a channel.
	 * @param settledTagCallback notified about the deliveries settled by the listener.
	 * @return the proxy.
	 */
	static Channel create(Supplier<Channel> channelSupplier, SettledTagCallback settledTagCallback) {
		return (Channel) Proxy.newProxyInstance(ChannelProxy.class.getClassLoader(),
				new Class<?>[] {ChannelProxy.class},
				new ListenerChannelProxy(channelSupplier, settledTagCallback));
	}

	@Override
	public @Nullable Object invoke(Object proxy, Method method, Object[] args) throws Throwable { // NOSONAR
		Channel channel = this.channelSupplier.get();
		String methodName = method.getName();
		switch (methodName) {
			case "equals" -> {
				return proxy == args[0]; // NOSONAR
			}
			case "hashCode" -> {
				return System.identityHashCode(proxy);
			}
			case "toString" -> {
				return "Listener channel proxy: " + channel;
			}
			case "getTargetChannel" -> {
				return channel;
			}
			case "isTransactional" -> {
				return channel instanceof ChannelProxy channelProxy && channelProxy.isTransactional();
			}
			case "isConfirmSelected" -> {
				return channel instanceof ChannelProxy channelProxy && channelProxy.isConfirmSelected();
			}
			case "isPublisherConfirms" -> {
				return channel instanceof ChannelProxy channelProxy && channelProxy.isPublisherConfirms();
			}
			default -> {
			}
		}
		Object result;
		try {
			result = method.invoke(channel, args); // NOSONAR
		}
		catch (InvocationTargetException ex) {
			Throwable cause = ex.getCause();
			throw cause != null ? cause : ex;
		}
		// The broker has accepted the settlement, hence the container must not repeat it.
		switch (methodName) {
			case "basicAck", "basicNack" -> this.settledTagCallback.settled((Long) args[0], (Boolean) args[1]);
			case "basicReject" -> this.settledTagCallback.settled((Long) args[0], false);
			// Requeues every unsettled delivery on the channel.
			case "basicRecover" -> this.settledTagCallback.settled(Long.MAX_VALUE, true);
			default -> {
			}
		}
		return result;
	}

	/**
	 * Notified when listener code has settled deliveries through a
	 * {@link ListenerChannelProxy}.
	 */
	@FunctionalInterface
	interface SettledTagCallback {

		/**
		 * A delivery, or a range of them, has been settled by the listener.
		 * @param deliveryTag the settled delivery tag.
		 * @param multiple whether every unsettled delivery up to and including
		 * {@code deliveryTag} is settled, the way the broker treats this flag.
		 */
		void settled(long deliveryTag, boolean multiple);

	}

}
