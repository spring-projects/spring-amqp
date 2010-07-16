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

package org.springframework.otp.erlang.connection;

import java.io.IOException;
import java.net.UnknownHostException;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.otp.erlang.OtpIOException;
import org.springframework.util.Assert;

import com.ericsson.otp.erlang.OtpAuthException;
import com.ericsson.otp.erlang.OtpConnection;
import com.ericsson.otp.erlang.OtpPeer;
import com.ericsson.otp.erlang.OtpSelf;

/**
 * @author Mark Pollack
 * @author Mark Fisher
 */
public class SimpleConnectionFactory implements ConnectionFactory, InitializingBean {

	private String selfNodeName;

	private String cookie;

	private String peerNodeName;

	private OtpSelf otpSelf;

	private OtpPeer otpPeer;


	public SimpleConnectionFactory(String selfNodeName, String cookie, String peerNodeName) {
		this.selfNodeName = selfNodeName;
		this.cookie = cookie;
		this.peerNodeName = peerNodeName;
	}

	public SimpleConnectionFactory(String selfNodeName, String peerNodeName) {
		this.selfNodeName = selfNodeName;
		this.peerNodeName = peerNodeName;
	}


	public OtpConnection createConnection() throws UnknownHostException, OtpAuthException, IOException {
		try {
			return otpSelf.connect(otpPeer);
		}
		catch (IOException ex) {
			throw new OtpIOException("failed to connect from '" + this.selfNodeName
					+ "' to peer node '" + this.peerNodeName + "'", ex);
		}
	}

	public void afterPropertiesSet() {
		Assert.isTrue(this.selfNodeName != null || this.peerNodeName != null,
				"'selfNodeName' or 'peerNodeName' is required");
		try {
			if (this.cookie == null) {
				this.otpSelf = new OtpSelf(this.selfNodeName);
			}
			else {
				this.otpSelf = new OtpSelf(this.selfNodeName, this.cookie);
			}
		}
		catch (IOException e) {
			throw new OtpIOException(e);
		}
		this.otpPeer = new OtpPeer(this.peerNodeName);
	}

}
