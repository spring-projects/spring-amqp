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
import org.junit.Test;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.erlang.OtpIOException;

/**
 * @author Mark Pollack
 */
public class RabbitBrokerAdminIntegrationTests {

	private static RabbitBrokerAdmin brokerAdmin;

	private static SingleConnectionFactory connectionFactory;

	@BeforeClass
	public static void setUp() {
		connectionFactory = new SingleConnectionFactory();
		connectionFactory.setUsername("guest");
		connectionFactory.setPassword("guest");
		brokerAdmin = new RabbitBrokerAdmin(connectionFactory);
	}

	@Test
	//@Ignore
	public void integrationTestsUserCrud() {
		List<String> users = brokerAdmin.listUsers();
		if (users.contains("joe")) {
			brokerAdmin.deleteUser("joe");
		}
		brokerAdmin.addUser("joe", "trader");
		brokerAdmin.changeUserPassword("joe", "sales");
		users = brokerAdmin.listUsers();
		if (users.contains("joe")) {
			brokerAdmin.deleteUser("joe");
		}
	}

	
	public void integrationTestListUsers() {
		// OtpErlangObject result =
		// adminTemplate.getErlangTemplate().executeRpc("rabbit_amqqueue",
		// "info_all", "/".getBytes());
		// System.out.println(result);
		List<String> users = brokerAdmin.listUsers();
		System.out.println(users);
	}

	public void integrationTestDeleteUser() {
		// OtpErlangObject result =
		// adminTemplate.getErlangTemplate().executeRpc("rabbit_access_control",
		// "delete_user", "joe".getBytes());
		brokerAdmin.deleteUser("joe");
		// System.out.println(result.getClass());
		// System.out.println(result);
	}
	
	@Test
	public void repeatLifecycle() throws Exception {
		for (int i = 1; i< 100; i++) {
			testStatusAndBrokerLifecycle();
			System.out.println("i = " + i);
			//Thread.sleep(1000);
		}
	}
	
	//@Test	
	public void testStatusAndBrokerLifecycle() throws Exception {
				
		RabbitStatus status = brokerAdmin.getStatus();
		
		brokerAdmin.stopBrokerApplication();
		status = brokerAdmin.getStatus();
		assertEquals(0, status.getRunningNodes().size());
		
		brokerAdmin.startBrokerApplication();
		status = brokerAdmin.getStatus();
		assertBrokerAppRunning(status);				
	}
	
	
	
	@Test
	//@Ignore("NEEDS RABBITMQ_HOME to be set.")
	public void testStartNode() {
		try {
			brokerAdmin.stopNode();
		} catch (OtpIOException e) {
			//assume it is not running.
		}
		brokerAdmin.startNode();
		assertEquals(1,1);
		brokerAdmin.stopNode();
	}
	
	@Test
	public void testGetQueues() {
		brokerAdmin.declareQueue(new Queue("test.queue"));
		assertEquals("/", connectionFactory.getVirtualHost());
		List<QueueInfo> queues = brokerAdmin.getQueues();
		assertEquals("test.queue", queues.get(0).getName());
	}

	private void assertBrokerAppRunning(RabbitStatus status) {
		assertEquals(1, status.getRunningNodes().size());
		assertTrue(status.getRunningNodes().get(0).getName().contains("rabbit"));
	}

}
