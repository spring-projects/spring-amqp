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

import com.ericsson.otp.erlang.OtpPeer;
import com.ericsson.otp.erlang.OtpSelf;

/**
 * Encapsulate properties to create a OtpConnection
 * @author Mark Pollack
 *
 */
public class ConnectionParameters {

	private OtpSelf otpSelf;
	
	private OtpPeer otpPeer;
	
	public ConnectionParameters(OtpSelf otpSelf, OtpPeer otpPeer) {
		//TODO assert not null...
		this.otpSelf = otpSelf;
		this.otpPeer = otpPeer;
	}

	public OtpSelf getOtpSelf() {
		return otpSelf;
	}

	public OtpPeer getOtpPeer() {
		return otpPeer;
	}

}
