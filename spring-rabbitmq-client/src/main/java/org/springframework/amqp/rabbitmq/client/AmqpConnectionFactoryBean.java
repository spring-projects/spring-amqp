/*
 * Copyright 2025 the original author or authors.
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

import java.time.Duration;
import java.util.function.Consumer;

import javax.net.ssl.SSLContext;

import com.rabbitmq.client.amqp.AddressSelector;
import com.rabbitmq.client.amqp.BackOffDelayPolicy;
import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.ConnectionBuilder;
import com.rabbitmq.client.amqp.ConnectionSettings;
import com.rabbitmq.client.amqp.CredentialsProvider;
import com.rabbitmq.client.amqp.Environment;
import com.rabbitmq.client.amqp.OAuth2Settings;
import com.rabbitmq.client.amqp.Resource;
import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.config.AbstractFactoryBean;

/**
 * The {@link AbstractFactoryBean} for RabbitMQ AMQP 1.0 {@link Connection}.
 * A Spring-friendly wrapper around {@link Environment#connectionBuilder()};
 *
 * @author Artem Bilan
 *
 * @since 4.0
 */
public class AmqpConnectionFactoryBean extends AbstractFactoryBean<Connection> {

	private final ConnectionBuilder connectionBuilder;

	public AmqpConnectionFactoryBean(Environment amqpEnvironment) {
		this.connectionBuilder = amqpEnvironment.connectionBuilder();
	}

	public AmqpConnectionFactoryBean setHost(String host) {
		this.connectionBuilder.host(host);
		return this;
	}

	public AmqpConnectionFactoryBean setPort(int port) {
		this.connectionBuilder.port(port);
		return this;
	}

	public AmqpConnectionFactoryBean setUsername(String username) {
		this.connectionBuilder.username(username);
		return this;
	}

	public AmqpConnectionFactoryBean setPassword(String password) {
		this.connectionBuilder.password(password);
		return this;
	}

	public AmqpConnectionFactoryBean setVirtualHost(String virtualHost) {
		this.connectionBuilder.virtualHost(virtualHost);
		return this;
	}

	public AmqpConnectionFactoryBean setUri(String uri) {
		this.connectionBuilder.uri(uri);
		return this;
	}

	public AmqpConnectionFactoryBean setUris(String... uris) {
		this.connectionBuilder.uris(uris);
		return this;
	}

	public AmqpConnectionFactoryBean setIdleTimeout(Duration idleTimeout) {
		this.connectionBuilder.idleTimeout(idleTimeout);
		return this;
	}

	public AmqpConnectionFactoryBean setAddressSelector(AddressSelector addressSelector) {
		this.connectionBuilder.addressSelector(addressSelector);
		return this;
	}

	public AmqpConnectionFactoryBean setCredentialsProvider(CredentialsProvider credentialsProvider) {
		this.connectionBuilder.credentialsProvider(credentialsProvider);
		return this;
	}

	public AmqpConnectionFactoryBean setSaslMechanism(SaslMechanism saslMechanism) {
		this.connectionBuilder.saslMechanism(saslMechanism.name());
		return this;
	}

	public AmqpConnectionFactoryBean setTls(Consumer<Tls> tlsCustomizer) {
		tlsCustomizer.accept(new Tls(this.connectionBuilder.tls()));
		return this;
	}

	public AmqpConnectionFactoryBean setAffinity(Consumer<Affinity> affinityCustomizer) {
		affinityCustomizer.accept(new Affinity(this.connectionBuilder.affinity()));
		return this;
	}

	public AmqpConnectionFactoryBean setOAuth2(Consumer<OAuth2> oauth2Customizer) {
		oauth2Customizer.accept(new OAuth2(this.connectionBuilder.oauth2()));
		return this;
	}

	public AmqpConnectionFactoryBean setRecovery(Consumer<Recovery> recoveryCustomizer) {
		recoveryCustomizer.accept(new Recovery(this.connectionBuilder.recovery()));
		return this;
	}

	public AmqpConnectionFactoryBean setListeners(Resource.StateListener... listeners) {
		this.connectionBuilder.listeners(listeners);
		return this;
	}

	@Override
	public @Nullable Class<?> getObjectType() {
		return Connection.class;
	}

	@Override
	protected Connection createInstance() {
		return this.connectionBuilder.build();
	}

	@Override
	protected void destroyInstance(@Nullable Connection instance) {
		if (instance != null) {
			instance.close();
		}
	}

	public enum SaslMechanism {

		PLAIN, ANONYMOUS, EXTERNAL

	}

	public static final class Tls {

		private final ConnectionSettings.TlsSettings<? extends ConnectionBuilder> tls;

		private Tls(ConnectionSettings.TlsSettings<? extends ConnectionBuilder> tls) {
			this.tls = tls;
		}

		public Tls hostnameVerification() {
			this.tls.hostnameVerification();
			return this;
		}

		public Tls hostnameVerification(boolean hostnameVerification) {
			this.tls.hostnameVerification(hostnameVerification);
			return this;
		}

		public Tls sslContext(SSLContext sslContext) {
			this.tls.sslContext(sslContext);
			return this;
		}

		public Tls trustEverything() {
			this.tls.trustEverything();
			return this;
		}

	}

	public static final class Affinity {

		private final ConnectionSettings.Affinity<? extends ConnectionBuilder> affinity;

		private Affinity(ConnectionSettings.Affinity<? extends ConnectionBuilder> affinity) {
			this.affinity = affinity;
		}

		public Affinity queue(String queue) {
			this.affinity.queue(queue);
			return this;
		}

		public Affinity operation(ConnectionSettings.Affinity.Operation operation) {
			this.affinity.operation(operation);
			return this;
		}

		public Affinity reuse(boolean reuse) {
			this.affinity.reuse(reuse);
			return this;
		}

		public Affinity strategy(ConnectionSettings.AffinityStrategy strategy) {
			this.affinity.strategy(strategy);
			return this;
		}

	}

	public static final class OAuth2 {

		private final OAuth2Settings<? extends ConnectionBuilder> oAuth2Settings;

		private OAuth2(OAuth2Settings<? extends ConnectionBuilder> oAuth2Settings) {
			this.oAuth2Settings = oAuth2Settings;
		}

		public OAuth2 tokenEndpointUri(String uri) {
			this.oAuth2Settings.tokenEndpointUri(uri);
			return this;
		}

		public OAuth2 clientId(String clientId) {
			this.oAuth2Settings.clientId(clientId);
			return this;
		}

		public OAuth2 clientSecret(String clientSecret) {
			this.oAuth2Settings.clientSecret(clientSecret);
			return this;
		}

		public OAuth2 grantType(String grantType) {
			this.oAuth2Settings.grantType(grantType);
			return this;
		}

		public OAuth2 parameter(String name, String value) {
			this.oAuth2Settings.parameter(name, value);
			return this;
		}

		public OAuth2 shared(boolean shared) {
			this.oAuth2Settings.shared(shared);
			return this;
		}

		public OAuth2 sslContext(SSLContext sslContext) {
			this.oAuth2Settings.tls().sslContext(sslContext);
			return this;
		}

	}

	public static final class Recovery {

		private final ConnectionBuilder.RecoveryConfiguration recoveryConfiguration;

		private Recovery(ConnectionBuilder.RecoveryConfiguration recoveryConfiguration) {
			this.recoveryConfiguration = recoveryConfiguration;
		}

		public Recovery activated(boolean activated) {
			this.recoveryConfiguration.activated(activated);
			return this;
		}

		public Recovery backOffDelayPolicy(BackOffDelayPolicy backOffDelayPolicy) {
			this.recoveryConfiguration.backOffDelayPolicy(backOffDelayPolicy);
			return this;
		}

		public Recovery topology(boolean activated) {
			this.recoveryConfiguration.topology(activated);
			return this;
		}

	}

}
