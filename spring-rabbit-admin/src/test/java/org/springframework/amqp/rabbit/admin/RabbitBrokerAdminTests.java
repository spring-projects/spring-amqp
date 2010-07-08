/*
 * Copyright 2002-2010 the original author or authors.
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

package org.springframework.amqp.rabbit.admin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.otp.erlang.OtpIOException;

/**
 * @author Mark Pollack
 */
public class RabbitBrokerAdminTests {

	private static RabbitBrokerAdmin adminTemplate;

	@BeforeClass
	public static void setUp() {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setUsername("guest");
		connectionFactory.setPassword("guest");
		connectionFactory.setChannelCacheSize(10);
		adminTemplate = new RabbitBrokerAdmin(connectionFactory);
	}

	@Test
	@Ignore
	public void integrationTestsUserCrud() {
		List<String> users = adminTemplate.listUsers();
		if (users.contains("joe")) {
			adminTemplate.deleteUser("joe");
		}
		adminTemplate.addUser("joe", "trader");
		adminTemplate.changeUserPassword("joe", "sales");
		users = adminTemplate.listUsers();
		if (users.contains("joe")) {
			adminTemplate.deleteUser("joe");
		}
	}

	public void integrationTestListUsers() {
		// OtpErlangObject result =
		// adminTemplate.getErlangTemplate().executeRpc("rabbit_amqqueue",
		// "info_all", "/".getBytes());
		// System.out.println(result);
		List<String> users = adminTemplate.listUsers();
		System.out.println(users);
	}

	public void integrationTestDeleteUser() {
		// OtpErlangObject result =
		// adminTemplate.getErlangTemplate().executeRpc("rabbit_access_control",
		// "delete_user", "joe".getBytes());
		adminTemplate.deleteUser("joe");
		// System.out.println(result.getClass());
		// System.out.println(result);
	}
	
	@Test
	@Ignore
	public void testStatusAndBrokerLifecycle() {
		RabbitStatus status = adminTemplate.getStatus();
		assertBrokerAppRunning(status);		
		
		adminTemplate.stopBrokerApplication();
		status = adminTemplate.getStatus();
		assertEquals(0, status.getRunningNodes().size());
		
		adminTemplate.startBrokerApplication();
		status = adminTemplate.getStatus();
		assertBrokerAppRunning(status);				
	}
	
	@Test
	@Ignore("NEEDS RABBITMQ_HOME to be set.")
	public void testStartNode() {
		try {
			adminTemplate.stopNode();
		} catch (OtpIOException e) {
			//assume it is not running.
		}
		adminTemplate.startNode();
		assertEquals(1,1);
		adminTemplate.stopNode();
	}
		

	private void assertBrokerAppRunning(RabbitStatus status) {
		assertEquals(1, status.getRunningNodes().size());
		assertTrue(status.getRunningNodes().get(0).getName().contains("rabbit"));
	}
}
