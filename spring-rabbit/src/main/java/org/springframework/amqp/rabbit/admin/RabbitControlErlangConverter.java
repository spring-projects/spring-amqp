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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.erlang.core.Application;
import org.springframework.erlang.core.ControlAction;
import org.springframework.erlang.core.Node;
import org.springframework.erlang.support.converter.ErlangConversionException;
import org.springframework.erlang.support.converter.ErlangConverter;
import org.springframework.erlang.support.converter.SimpleErlangConverter;

import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangBinary;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangTuple;

/**
 * Converter that understands the responses from the rabbit control module and related functionality.
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Helena Edelson
 */
public class RabbitControlErlangConverter extends SimpleErlangConverter implements ErlangConverter {

    protected final Log logger = LogFactory.getLog(getClass());

    private Map<String, ControlAction> controlActionMap;

    private static ErlangConverter DEFAULT_CONVERTER = new SimpleErlangConverter();

    private ControlAction[] controlActions;

    /**
     * Creates an instance of RabbitControlErlangConverter and initializes
     * default ControlActions for running the latest version of the broker.
     */
    public RabbitControlErlangConverter() {
        initializeMappings(true);
    }

    public Object fromErlangRpc(String module, String function, OtpErlangObject erlangObject) throws ErlangConversionException {
        ErlangConverter converter = getConverter(function);
        return converter != null ? converter.fromErlang(erlangObject) : super.fromErlangRpc(module, function, erlangObject);
    }

    protected ErlangConverter getConverter(String function) {
        ControlAction action = controlActionMap.get(function);
        return action != null ? action.getConverter() : this;
    }

    public ControlAction[] getControlActions() {
        return this.controlActions;
    }

    /**
     * Initializes a map of ControlActions by function.
     */
    protected void initializeMappings(boolean isLatestVersion) {

        this.controlActions = new ControlAction[]{
                AddUser.create(isLatestVersion), DeleteUser.create(isLatestVersion), ChangePassword.create(isLatestVersion),
                ListUsers.create(isLatestVersion), ListStatus.create(), ListQueues.create(), StartBrokerApplication.create(),
                StopBrokerApplication.create(), StopNode.create(), ResetNode.create(), ForceResetNode.create()
        };
        initializeControlActionMap();
    }

    /**
     * Initializes a map of ControlActions by function.
     * Tests whether the broker is the latest version, for internal
     * API changes. Current changes are as of Rabbit 2.3.0.
     * @param version The broker version
     * @return ControlAction array
     */
    public ControlAction[] refreshMappings(String version) {
        boolean isLatestVersion = version.startsWith("2.3");
        initializeMappings(isLatestVersion);

        return getControlActions();
    }

    private void initializeControlActionMap() {
        this.controlActionMap = new HashMap<String, ControlAction>();

        for (ControlAction action : getControlActions()) {
            controlActionMap.put(action.getFunction(), action);
        }
    }

    public static class ListUsers extends RabbitControlAction {
        public static ControlAction create(boolean isLatestVersion) {
            return isLatestVersion ? new RabbitControlAction(ListUsers.class, "rabbit_auth_backend_internal", "list_users", new ListUsersConverter())
                    : new RabbitControlAction(ListUsers.class, "rabbit_access_control", "list_users", new ListUsersConverter());
        }
    }

    public static class AddUser extends RabbitControlAction {
        public static ControlAction create(boolean isLatestVersion) {
            return isLatestVersion ? new RabbitControlAction(AddUser.class, "rabbit_auth_backend_internal", "add_user", DEFAULT_CONVERTER)
                    : new RabbitControlAction(AddUser.class, "rabbit_access_control", "add_user", DEFAULT_CONVERTER);
        }
    }

    public static class DeleteUser extends RabbitControlAction {
        public static ControlAction create(boolean isLatestVersion) {
            return isLatestVersion ? new RabbitControlAction(DeleteUser.class, "rabbit_auth_backend_internal", "delete_user", DEFAULT_CONVERTER)
                    : new RabbitControlAction(DeleteUser.class, "rabbit_access_control", "delete_user", DEFAULT_CONVERTER);
        }
    }

    public static class ChangePassword extends RabbitControlAction {
        public static ControlAction create(boolean isLatestVersion) {
            return isLatestVersion ? new RabbitControlAction(ChangePassword.class, "rabbit_auth_backend_internal", "change_password", DEFAULT_CONVERTER)
                    : new RabbitControlAction(ChangePassword.class, "rabbit_access_control", "change_password", DEFAULT_CONVERTER);
        }
    }

    public static class StartBrokerApplication extends RabbitControlAction {
        public static ControlAction create() {
            return new RabbitControlAction(StartBrokerApplication.class, "rabbit", "start", DEFAULT_CONVERTER);
        }
    }

    public static class StopBrokerApplication extends RabbitControlAction {
        public static ControlAction create() {
            return new RabbitControlAction(StopBrokerApplication.class, "rabbit", "stop", DEFAULT_CONVERTER);
        }
    }

    public static class StopNode extends RabbitControlAction {
        public static ControlAction create() {
            return new RabbitControlAction(StopNode.class, "rabbit", "stop_and_halt", DEFAULT_CONVERTER);
        }
    }

    public static class ResetNode extends RabbitControlAction {
        public static ControlAction create() {
            return new RabbitControlAction(ResetNode.class, "rabbit_mnesia", "reset", DEFAULT_CONVERTER);
        }
    }

    public static class ForceResetNode extends RabbitControlAction {
        public static ControlAction create() {
            return new RabbitControlAction(ForceResetNode.class, "rabbit_mnesia", "force_reset", DEFAULT_CONVERTER);
        }
    }

    public static class ListStatus extends RabbitControlAction {
        public static ControlAction create() {
            return new RabbitControlAction(ListStatus.class, "rabbit", "status", new StatusConverter());
        }
    }

    public static class ListQueues extends RabbitControlAction {
        public static ControlAction create() {
            return new RabbitControlAction(ListQueues.class, "rabbit_amqqueue", "info_all", new QueueInfoAllConverter());
        }
    }

    /* TODO converter */

    public static class ListExchanges extends RabbitControlAction {
        public static ControlAction create() {
            return new RabbitControlAction("rabbit_exchange", "list", null);
        }
    }

    /* TODO */

    public static class ListBindings extends RabbitControlAction {
        public static ControlAction create() {
            return null;
        }
    }

    public static class BrokerVersion extends RabbitControlAction {
        public static ControlAction create() {
            return new RabbitControlAction(BrokerVersion.class, "rabbit", "status", new VersionConverter());
        }
    }

    public static class VersionConverter extends SimpleErlangConverter {

        public Object fromErlang(OtpErlangObject erlangObject) throws ErlangConversionException {
            String version = null;
            long items = ((OtpErlangList) erlangObject).elements().length;
            if (items > 0) {
                if (erlangObject instanceof OtpErlangList) {
                    for (OtpErlangObject outerList : ((OtpErlangList) erlangObject).elements()) {
                        if (outerList instanceof OtpErlangTuple) {
                            OtpErlangTuple entry = (OtpErlangTuple) outerList;
                            String key = entry.elementAt(0).toString();
                            if (key.equals("running_applications") && entry.elementAt(1) instanceof OtpErlangList) {
                                OtpErlangList value = (OtpErlangList) entry.elementAt(1);

                                Pattern p = Pattern.compile("\"(\\d+\\.\\d+(?:\\.\\d+)?)\"}$");
                                Matcher m = p.matcher(value.elementAt(0).toString());
                                version = m.find() ? m.group(1) : null;
                            }
                        }
                    }
                }
            }

            return version;
        }
    }

    public static class ListUsersConverter extends SimpleErlangConverter {

        public Object fromErlang(OtpErlangObject erlangObject) throws ErlangConversionException {

            List<String> users = new ArrayList<String>();
            if (erlangObject instanceof OtpErlangList) {
                OtpErlangList erlangList = (OtpErlangList) erlangObject;
                for (OtpErlangObject obj : erlangList) {
                    String value = extractString(obj);
                    if (value != null) {
                        users.add(value);
                    }
                }
            }
            return users;
        }

        private String extractString(OtpErlangObject obj) {

            if (obj instanceof OtpErlangBinary) {
                OtpErlangBinary binary = (OtpErlangBinary) obj;
                return new String(binary.binaryValue());
            } else if (obj instanceof OtpErlangTuple) {
                OtpErlangTuple tuple = (OtpErlangTuple) obj;
                return extractString(tuple.elementAt(0));
            }
            return null;
        }
    }

    public static class StatusConverter extends SimpleErlangConverter {

        public Object fromErlang(OtpErlangObject erlangObject) throws ErlangConversionException {

            List<Application> applications = new ArrayList<Application>();
            List<Node> nodes = new ArrayList<Node>();
            List<Node> runningNodes = new ArrayList<Node>();
            if (erlangObject instanceof OtpErlangList) {
                OtpErlangList erlangList = (OtpErlangList) erlangObject;

                OtpErlangTuple runningAppTuple = (OtpErlangTuple) erlangList.elementAt(0);
                OtpErlangList appList = (OtpErlangList) runningAppTuple.elementAt(1);
                extractApplications(applications, appList);

                OtpErlangTuple nodesTuple = (OtpErlangTuple) erlangList.elementAt(1);
                OtpErlangList nodesList = (OtpErlangList) nodesTuple.elementAt(1);
                extractNodes(nodes, nodesList);

                OtpErlangTuple runningNodesTuple = (OtpErlangTuple) erlangList.elementAt(2);
                nodesList = (OtpErlangList) runningNodesTuple.elementAt(1);
                extractNodes(runningNodes, nodesList);

                /*
                     * for (OtpErlangObject obj : erlangList) { if (obj instanceof OtpErlangBinary) { OtpErlangBinary binary
                     * = (OtpErlangBinary) obj; users.add(new String(binary.binaryValue())); } }
                     */
            }

            return new RabbitStatus(applications, nodes, runningNodes);
        }

        private void extractNodes(List<Node> nodes, OtpErlangList nodesList) {
            for (OtpErlangObject erlangNodeName : nodesList) {
                String nodeName = erlangNodeName.toString();
                nodes.add(new Node(nodeName));
            }
        }

        private void extractApplications(List<Application> applications, OtpErlangList appList) {
            for (OtpErlangObject appDescription : appList) {
                OtpErlangTuple appDescriptionTuple = (OtpErlangTuple) appDescription;
                String name = appDescriptionTuple.elementAt(0).toString();
                String description = appDescriptionTuple.elementAt(1).toString();
                String version = appDescriptionTuple.elementAt(2).toString();
                applications.add(new Application(name, description, version));
            }
        }
    }

    public enum QueueInfoField {
        transactions, acks_uncommitted, consumers, pid, durable, messages, memory, auto_delete, messages_ready, arguments, name, messages_unacknowledged, messages_uncommitted, NOVALUE;

        public static QueueInfoField toQueueInfoField(String str) {
            try {
                return valueOf(str);
            } catch (Exception ex) {
                return NOVALUE;
            }
        }
    }

    public static class QueueInfoAllConverter extends SimpleErlangConverter {

        @Override
        public Object fromErlang(OtpErlangObject erlangObject) throws ErlangConversionException {
            List<QueueInfo> queueInfoList = new ArrayList<QueueInfo>();
            if (erlangObject instanceof OtpErlangList) {
                OtpErlangList erlangList = (OtpErlangList) erlangObject;
                for (OtpErlangObject element : erlangList.elements()) {
                    QueueInfo queueInfo = new QueueInfo();
                    OtpErlangList itemList = (OtpErlangList) element;
                    for (OtpErlangObject item : itemList.elements()) {
                        OtpErlangTuple tuple = (OtpErlangTuple) item;
                        if (tuple.arity() == 2) {
                            String key = tuple.elementAt(0).toString();
                            OtpErlangObject value = tuple.elementAt(1);
                            switch (QueueInfoField.toQueueInfoField(key)) {
                                case name:
                                    queueInfo.setName(extractNameValueFromTuple((OtpErlangTuple) value));
                                    break;
                                case transactions:
                                    queueInfo.setTransactions(extractLong(value));
                                    break;
                                case acks_uncommitted:
                                    queueInfo.setAcksUncommitted(extractLong(value));
                                    break;
                                case consumers:
                                    queueInfo.setConsumers(extractLong(value));
                                    break;
                                case pid:
                                    queueInfo.setPid(extractPid(value));
                                    break;
                                case durable:
                                    queueInfo.setDurable(extractAtomBoolean(value));
                                    break;
                                case messages:
                                    queueInfo.setMessages(extractLong(value));
                                    break;
                                case memory:
                                    queueInfo.setMemory(extractLong(value));
                                    break;
                                case auto_delete:
                                    queueInfo.setAutoDelete(extractAtomBoolean(value));
                                    break;
                                case messages_ready:
                                    queueInfo.setMessagesReady(extractLong(value));
                                    break;
                                case arguments:
                                    OtpErlangList list = (OtpErlangList) value;
                                    if (list != null) {
                                        String[] args = new String[list.arity()];
                                        for (int i = 0; i < list.arity(); i++) {
                                            OtpErlangObject obj = list.elementAt(i);
                                            args[i] = obj.toString();
                                        }
                                        queueInfo.setArguments(args);
                                    }
                                    break;
                                case messages_unacknowledged:
                                    queueInfo.setMessagesUnacknowledged(extractLong(value));
                                    break;
                                case messages_uncommitted:
                                    queueInfo.setMessageUncommitted(extractLong(value));
                                    break;
                                default:
                                    break;
                            }
                        }
                    }
                    queueInfoList.add(queueInfo);
                }
            }
            return queueInfoList;
        }

        private boolean extractAtomBoolean(OtpErlangObject value) {
            return ((OtpErlangAtom) value).booleanValue();
        }

        private String extractNameValueFromTuple(OtpErlangTuple value) {
            Object nameElement = value.elementAt(3);
            return new String(((OtpErlangBinary) nameElement).binaryValue());
        }
    }

}
