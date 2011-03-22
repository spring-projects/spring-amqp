package org.springframework.amqp.rabbit.admin;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.amqp.rabbit.test.EnvironmentAvailable;
import org.springframework.erlang.connection.SingleConnectionFactory;
import org.springframework.erlang.core.ErlangTemplate;
import org.springframework.util.exec.Os;

import com.ericsson.otp.erlang.OtpConnection;
import com.ericsson.otp.erlang.OtpErlangBinary;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpPeer;
import com.ericsson.otp.erlang.OtpSelf;

public class JInterfaceIntegrationTests {

	private static Log logger = LogFactory.getLog(JInterfaceIntegrationTests.class);

	private static int counter;

	private OtpConnection connection = null;

	@Rule
	public static EnvironmentAvailable environment = new EnvironmentAvailable("BROKER_INTEGRATION_TEST");

	@After
	public void close() {
		if (connection != null) {
			connection.close();
		}
	}

	@Test
	public void testRawApi() throws Exception {

		OtpSelf self = new OtpSelf("rabbit-monitor");

		String hostName = "rabbit@" + getHostName();
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
		String peerNodeName = "rabbit@" + getHostName();

		SingleConnectionFactory cf = new SingleConnectionFactory(selfNodeName, peerNodeName);

		cf.afterPropertiesSet();
		ErlangTemplate template = new ErlangTemplate(cf);
		template.afterPropertiesSet();

		long number = (Long) template.executeAndConvertRpc("erlang", "abs", -161803399);
		Assert.assertEquals(161803399, number);

		cf.destroy();

	}

	private String getHostName() throws UnknownHostException {
		String hostName = InetAddress.getLocalHost().getHostName();
		if (Os.isFamily("windows")) {
			hostName = hostName.toUpperCase();
		}
		return hostName;
	}

	@Test
	public void testRawOtpConnect() throws Exception {
		createConnection();
	}

	@Test
	public void stressTest() throws Exception {
		String cookie = readCookie();
		logger.info("Cookie: "+cookie);
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

	public OtpConnection createConnection() throws Exception {
		OtpSelf self = new OtpSelf("rabbit-monitor-" + counter++);
		OtpPeer peer = new OtpPeer("rabbit@" + getHostName());
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
