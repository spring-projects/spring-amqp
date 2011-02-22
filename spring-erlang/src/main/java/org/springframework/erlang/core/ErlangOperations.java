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


package org.springframework.erlang.core;

import org.springframework.erlang.OtpException;
import org.springframework.erlang.support.ControlAction;

import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangObject;

/**
 * Operations to perform against OTP/Erlang
 * @author Mark Pollack
 * @author Helena Edelson
 */
public interface ErlangOperations {

	<T> T execute(ConnectionCallback<T> action) throws OtpException;
	
	OtpErlangObject executeErlangRpc(String module, String function, OtpErlangList args) throws OtpException; 

	OtpErlangObject executeRpc(ControlAction action, Object... args) throws OtpException;

	Object executeAndConvertRpc(ControlAction action, Object... args) throws OtpException;

    Object executeAndConvertRpc(String module, String function, Object... args);
}
