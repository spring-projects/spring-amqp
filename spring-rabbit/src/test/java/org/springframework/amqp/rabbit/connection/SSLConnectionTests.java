/*
 * Copyright 2014-2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.amqp.rabbit.connection;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.junit.Ignore;
import org.junit.Test;

import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.core.io.ClassPathResource;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * @author Gary Russell
 * @since 1.4.4
 *
 */
public class SSLConnectionTests {

	@Test @Ignore
	public void test() throws Exception {
		RabbitConnectionFactoryBean fb = new RabbitConnectionFactoryBean();
		fb.setUseSSL(true);
		fb.setSslPropertiesLocation(new ClassPathResource("ssl.properties"));
		fb.afterPropertiesSet();
		ConnectionFactory cf = fb.getObject();
		Connection conn = cf.newConnection();
		Channel chan = conn.createChannel();
		chan.close();
		conn.close();
	}

	@Test
	public void testAlgNoProps() throws Exception {
		RabbitConnectionFactoryBean fb = new RabbitConnectionFactoryBean();
		ConnectionFactory rabbitCf = spy(TestUtils.getPropertyValue(fb, "connectionFactory", ConnectionFactory.class));
		new DirectFieldAccessor(fb).setPropertyValue("connectionFactory", rabbitCf);
		fb.setUseSSL(true);
		fb.setSslAlgorithm("TLSv1.2");
		fb.afterPropertiesSet();
		fb.getObject();
		verify(rabbitCf).useSslProtocol("TLSv1.2");
	}

	@Test
	public void testNoAlgNoProps() throws Exception {
		RabbitConnectionFactoryBean fb = new RabbitConnectionFactoryBean();
		ConnectionFactory rabbitCf = spy(TestUtils.getPropertyValue(fb, "connectionFactory", ConnectionFactory.class));
		new DirectFieldAccessor(fb).setPropertyValue("connectionFactory", rabbitCf);
		fb.setUseSSL(true);
		fb.afterPropertiesSet();
		fb.getObject();
		verify(rabbitCf).useSslProtocol();
	}

}
