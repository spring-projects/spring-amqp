package org.springframework.amqp.rabbit.admin;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.Ignore;
import org.junit.Test;

import org.springframework.erlang.connection.SimpleConnectionFactory;
import org.springframework.erlang.core.ErlangTemplate;

import com.ericsson.otp.erlang.OtpAuthException;
import com.ericsson.otp.erlang.OtpConnection;
import com.ericsson.otp.erlang.OtpErlangBinary;
import com.ericsson.otp.erlang.OtpErlangExit;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpPeer;
import com.ericsson.otp.erlang.OtpSelf;

@Ignore("manual integration test only.")
public class JInterfaceIntegrationTests {

	@Test
	public void rawApi() {
		OtpConnection connection = null;
		try {
			OtpSelf self = new OtpSelf("rabbit-monitor");

			String hostName = "rabbit@"
					+ InetAddress.getLocalHost().getHostName();
			OtpPeer peer = new OtpPeer(hostName);
			connection = self.connect(peer);
			// connection.sendRPC("erlang","date", new OtpErlangList());
			// connection.sendRPC("rabbit_access_control", "list_vhosts", new
			// OtpErlangList());
			OtpErlangObject[] objectArray = { new OtpErlangBinary("/"
					.getBytes()) };

			connection.sendRPC("rabbit_amqqueue", "info_all",
					new OtpErlangList(objectArray));

			// connection.sendRPC("rabbit_amqqueue", "stat_all", new
			// OtpErlangList());

			OtpErlangObject received = connection.receiveRPC();
			System.out.println(received);
			System.out.println(received.getClass());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (OtpAuthException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (OtpErlangExit e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (connection != null) {
				connection.close();
			}
		}

	}

	@Test
	public void otpTemplate() throws UnknownHostException {		
		String selfNodeName = "rabbit-monitor";
		String peerNodeName = "rabbit@"
				+ InetAddress.getLocalHost().getHostName();

		String home = System.getProperty("user.home");
		System.out.println("home = " + home);
		System.out.println("peerNodeName = " + peerNodeName);

		SimpleConnectionFactory cf = new SimpleConnectionFactory(selfNodeName,
				peerNodeName);

		cf.afterPropertiesSet();
		ErlangTemplate template = new ErlangTemplate(cf);
		template.afterPropertiesSet();

		OtpErlangObject result = template.executeRpc("rabbit_amqqueue",
				"info_all", "/".getBytes());
		System.out.println(result);
		System.out.println(result.getClass());

	}
}
