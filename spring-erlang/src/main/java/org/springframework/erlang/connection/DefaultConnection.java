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
package org.springframework.erlang.connection;

import java.io.IOException;

import com.ericsson.otp.erlang.OtpAuthException;
import com.ericsson.otp.erlang.OtpConnection;
import com.ericsson.otp.erlang.OtpErlangExit;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangObject;

/**
 * Basic implementation of {@link ConnectionProxy} that delegates to an underlying OtpConnection.
 * @author Mark Pollack
 *
 */
public class DefaultConnection implements ConnectionProxy {

	private OtpConnection otpConnection;
	
	
	public DefaultConnection(OtpConnection otpConnection) {
		this.otpConnection = otpConnection;
	}

	public void close() {
		otpConnection.close();		
	}

	public void sendRPC(String mod, String fun, OtpErlangList args)
			throws IOException {
		otpConnection.sendRPC(mod, fun, args);
		
	}

	public OtpErlangObject receiveRPC() throws IOException, OtpErlangExit,
			OtpAuthException {
		return otpConnection.receiveRPC();
	}

	public OtpConnection getTargetConnection() {
		return this.otpConnection;
	}

}
