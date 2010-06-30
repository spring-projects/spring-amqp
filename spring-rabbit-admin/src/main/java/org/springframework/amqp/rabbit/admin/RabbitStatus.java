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

import java.util.List;

import org.springframework.otp.erlang.core.Application;
import org.springframework.otp.erlang.core.Node;

/**
 * The status object returned from querying the broker via the
 *  
 * @author Mark Pollack
 *
 */
public class RabbitStatus {

	private List<Application> runningApplications;
	
	private List<Node> nodes;
	
	private List<Node> runningNodes;
	
	
	
	public RabbitStatus(List<Application> runningApplications,
			List<Node> nodes, List<Node> runningNodes) {
		super();
		this.runningApplications = runningApplications;
		this.nodes = nodes;
		this.runningNodes = runningNodes;
	}

	public List<Application> getRunningApplications() {
		return runningApplications;
	}
	
	public List<Node> getNodes() {
		return nodes;
	}
	
	public List<Node> getRunningNodes() { 
		return runningNodes;
	}

	@Override
	public String toString() {
		return "RabbitStatus [runningApplications=" + runningApplications
				+ ", runningNodes=" + runningNodes + ", nodes=" + nodes + "]";
	}


	
	
}
