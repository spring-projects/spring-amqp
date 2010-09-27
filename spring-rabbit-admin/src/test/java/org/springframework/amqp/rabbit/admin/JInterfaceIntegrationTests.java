package org.springframework.amqp.rabbit.admin;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import junit.framework.Assert;

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

//@Ignore("manual integration test only.")
public class JInterfaceIntegrationTests {

	@Test
	public void rawApi() {
		OtpConnection connection = null;
		try {
			OtpSelf self = new OtpSelf("rabbit-monitor");

			String hostName = "rabbit@vmc-ssrc-rh82";
					//+ InetAddress.getLocalHost().getHostName();
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
		String peerNodeName = "rabbit@" + InetAddress.getLocalHost().getHostName();

		//String home = System.getProperty("user.home");
		//System.out.println("home = " + home);
		//System.out.println("peerNodeName = " + peerNodeName);

		SimpleConnectionFactory cf = new SimpleConnectionFactory(selfNodeName,
				peerNodeName);

		cf.afterPropertiesSet();
		ErlangTemplate template = new ErlangTemplate(cf);
		template.afterPropertiesSet();

		//OtpErlangObject result = template.executeRpc("rabbit_amqqueue", "info_all", "/".getBytes());
		//System.out.println(result);
		//System.out.println(result.getClass());

		long number = (Long) template.executeAndConvertRpc("erlang", "abs",	-161803399);
		Assert.assertEquals(161803399, number);
	

	}

	
	@Test
	public void rawOtpConnect() throws Exception {
		createConnection();
	}
	
	
	@Test
	public void stressTest() throws Exception {
		//String cookie = readCookie();
		OtpConnection con = createConnection();
		boolean recycleConnection = false;
		for (int i=0; i< 100; i++) {
			executeRpc(con, recycleConnection, "rabbit", "status");
			executeRpc(con, recycleConnection, "rabbit", "stop");
			executeRpc(con, recycleConnection, "rabbit", "status");
			executeRpc(con, recycleConnection, "rabbit", "start");
			executeRpc(con, recycleConnection, "rabbit", "status");
			System.out.println("i = " + i);
		}	
	}
	
	public OtpConnection createConnection() throws Exception {
		OtpSelf self = new OtpSelf("rabbit-monitor");
		OtpPeer peer = new OtpPeer("rabbit");// + InetAddress.getLocalHost().getHostName());
		return self.connect(peer);		
	}

	private void executeRpc(OtpConnection con, boolean recycleConnection, String module, String function) throws Exception, UnknownHostException {		
		con.sendRPC(module,function, new OtpErlangList());
		OtpErlangObject response = con.receiveRPC();
		//System.out.println(module + " response received = " + response.toString());
		if (recycleConnection) {
			con.close();
			con = createConnection();
		}
	}


	private String readCookie() throws Exception {
		String cookie = null;
		final String dotCookieFilename = System.getProperty("user.home")
				+ File.separator + ".erlang.cookie";
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
