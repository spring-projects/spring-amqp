/*
 * Copyright 2002-2015 the original author or authors.
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

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.amqp.rabbit.test.BrokerTestUtils;
import org.springframework.amqp.rabbit.test.EnvironmentAvailable;
import org.springframework.erlang.connection.SingleConnectionFactory;
import org.springframework.erlang.core.ErlangTemplate;

import com.ericsson.otp.erlang.OtpConnection;
import com.ericsson.otp.erlang.OtpErlangBinary;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpPeer;
import com.ericsson.otp.erlang.OtpSelf;

/**
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Chris Beams
 * @author Dave Syer
 * @author Gary Russell
 * @author Artem Bilan
 */
public class JInterfaceIntegrationTests {

	private static Log logger = LogFactory.getLog(JInterfaceIntegrationTests.class);

	private static int counter;

	private static final String NODE_NAME = "spring@localhost";

	private OtpConnection connection = null;

	private RabbitBrokerAdmin brokerAdmin;

	@ClassRule
	public static EnvironmentAvailable environment = new EnvironmentAvailable("BROKER_INTEGRATION_TEST");

	@Before
	public void init() {
		brokerAdmin = BrokerTestUtils.getRabbitBrokerAdmin(NODE_NAME);
		RabbitStatus status = brokerAdmin.getStatus();
		if (!status.isRunning()) {
			brokerAdmin.startBrokerApplication();
		}
	}

	@After
	public void close() {
		if (connection != null) {
			connection.close();
		}
		if (brokerAdmin != null) {
			brokerAdmin.stopNode();
		}
	}

	@Test
	public void testRawApi() throws Exception {

		OtpSelf self = new OtpSelf("rabbit-monitor");

		String hostName = NODE_NAME;
		OtpPeer peer = new OtpPeer(hostName);
		connection = self.connect(peer);

		OtpErlangObject[] objectArray = { new OtpErlangBinary("/".getBytes()) };

		connection.sendRPC("rabbit_amqqueue", "info_all", new OtpErlangList(objectArray));

		OtpErlangObject received = connection.receiveRPC();
		System.out.println(received);
		System.out.println(received.getClass());

	}

	@Test
	public void otpTemplate() throws UnknownHostException {

		String selfNodeName = "rabbit-monitor";
		String peerNodeName = NODE_NAME;

		SingleConnectionFactory cf = new SingleConnectionFactory(selfNodeName, peerNodeName);

		cf.afterPropertiesSet();
		ErlangTemplate template = new ErlangTemplate(cf);
		template.afterPropertiesSet();

		long number = (Long) template.executeAndConvertRpc("erlang", "abs", -161803399);
		Assert.assertEquals(161803399, number);

		cf.destroy();

	}

	@Test
	public void testRawOtpConnect() throws Exception {
		createConnection();
	}

	@Test
	public void stressTest() throws Exception {
		String cookie = readCookie();
		logger.info("Cookie: " + cookie);
		OtpConnection con = createConnection();
		boolean recycleConnection = false;
		for (int i = 0; i < 100; i++) {
			executeRpc(con, recycleConnection, "rabbit", "status");
			executeRpc(con, recycleConnection, "rabbit", "stop");
			executeRpc(con, recycleConnection, "rabbit", "status");
			executeRpc(con, recycleConnection, "rabbit", "start");
			executeRpc(con, recycleConnection, "rabbit", "status");
			if (i % 10 == 0) {
				logger.debug("i = " + i);
			}
		}
	}

	@Test
	public void testConcurrency() throws Exception {
		SingleConnectionFactory cf = new SingleConnectionFactory("rabbit-monitor", NODE_NAME);
		cf.afterPropertiesSet();
		final ErlangTemplate template = new ErlangTemplate(cf);
		template.afterPropertiesSet();

		ExecutorService executorService = Executors.newFixedThreadPool(10);

		for (int i = 0; i < 10; i++) {
			final int j = i;
			executorService.execute(new Runnable() {

				@Override
				public void run() {
					Assert.assertEquals((long) j, template.executeAndConvertRpc("erlang", "abs", -j));
				}

			});
		}

		executorService.shutdown();

		assertTrue(executorService.awaitTermination(10, TimeUnit.SECONDS));

		cf.destroy();
	}

	public OtpConnection createConnection() throws Exception {
		OtpSelf self = new OtpSelf("rabbit-monitor-" + counter++);
		OtpPeer peer = new OtpPeer(NODE_NAME);
		return self.connect(peer);
	}

	private void executeRpc(OtpConnection con, boolean recycleConnection, String module, String function)
			throws Exception, UnknownHostException {
		con.sendRPC(module, function, new OtpErlangList());
		OtpErlangObject response = con.receiveRPC();
		logger.debug(module + " response received = " + response.toString());
		if (recycleConnection) {
			con.close();
			con = createConnection();
		}
	}

	private String readCookie() throws Exception {
		String cookie = null;
		final String dotCookieFilename = System.getProperty("user.home") + File.separator + ".erlang.cookie";
		BufferedReader br = null;

		try {
			final File dotCookieFile = new File(dotCookieFilename);

			br = new BufferedReader(new FileReader(dotCookieFile));
			cookie = br.readLine().trim();
			return cookie;
		} finally {
			try {
				if (br != null) {
					br.close();
				}
			} catch (final IOException e) {
			}
		}
	}

}
