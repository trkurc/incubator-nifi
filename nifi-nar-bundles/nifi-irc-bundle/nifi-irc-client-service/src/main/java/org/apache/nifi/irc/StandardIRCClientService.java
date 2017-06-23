/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.irc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;
import org.kitteh.irc.client.library.Client;
import org.kitteh.irc.client.library.event.channel.ChannelMessageEvent;
import org.kitteh.irc.client.library.event.channel.RequestedChannelJoinCompleteEvent;
import org.kitteh.irc.client.library.event.client.ClientConnectedEvent;
import org.kitteh.irc.client.library.event.client.ClientConnectionClosedEvent;

import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Invoke;

@Tags({ "irc"})
@CapabilityDescription("IRC client controller.")
public class StandardIRCClientService extends AbstractControllerService implements IRCClientService {

    public static final PropertyDescriptor IRC_SERVER = new PropertyDescriptor
            .Builder().name("IRC_SERVER")
            .displayName("IRC Server")
            .description("The IRC server you want to connect to")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor IRC_SERVER_PORT = new PropertyDescriptor
            .Builder().name("IRC_SERVER_PORT")
            .displayName("IRC Server Port")
            .description("The IRC server port you want to connect to")
            .required(true)
            .defaultValue("6667")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();
    public static final PropertyDescriptor IRC_TIMEOUT = new PropertyDescriptor
            .Builder().name("IRC_TIMEOUT")
            .displayName("IRC Timeout")
            .description("The amount of time to wait for certain actions to complete before timing-out")
            .required(true)
            .defaultValue("5 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();
    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL_CONTEXT_SERVICE")
            .displayName("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context. If this property is set, "
                    + "IRC connection will be established over a secure connection.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();
    public static final PropertyDescriptor IRC_NICK = new PropertyDescriptor
            .Builder().name("IRC_NICK")
            .displayName("Nickname")
            .description("The Nickname to use when connecting to the IRC server")
            .required(true)
            .defaultValue("NiFi")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor IRC_SERVER_PASSWORD = new PropertyDescriptor
            .Builder().name("IRC_SERVER_PASSWORD")
            .displayName("Password")
            .description("The password to be user for authentication")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES;
    private Client ircClient;
    private ComponentLog logger;
    protected AtomicBoolean connectionStatus = new AtomicBoolean(false);

    protected String clientIdentification;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(IRC_SERVER);
        props.add(IRC_SERVER_PORT);
        props.add(IRC_TIMEOUT);
        props.add(IRC_NICK);
        props.add(IRC_SERVER_PASSWORD);
        props.add(SSL_CONTEXT_SERVICE);
        PROPERTIES = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    private final Map<String, Map<String, IrcMessageHandler>> channelHandlers = 
            new ConcurrentHashMap<String, Map<String, IrcMessageHandler>>();
    private final ReentrantLock channelLock = new ReentrantLock();
    private String server;

    /**
     * @param context
     *            the configuration context
     * @throws InitializationException
     *             if unable to create an IRC connection
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
         clientIdentification = this.getIdentifier();

        this.logger = this.getLogger();

        Client.Builder clientSkeleton = Client
                .builder()
                .serverHost(context.getProperty(IRC_SERVER).getValue())
                .serverPort(context.getProperty(IRC_SERVER_PORT).asInteger())
                .nick(context.getProperty(IRC_NICK).getValue())
                .user("nifi")
                .realName(String.join(" - ", new String[] { this.getClass().getSimpleName(), this.clientIdentification }));

        // Setup Security
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);

        if (sslContextService == null) {
            // Disabled...
            clientSkeleton.secure(false);
        } else {
            // Enabled
            clientSkeleton.secure(true);
            SSLContext sslContext;
            TrustManagerFactory tmf;

            // Is key configured? If yes, populate and let it go...
            if (sslContextService.isKeyStoreConfigured()) {
                final String keyPassword = sslContextService.getKeyPassword();
                final String keyFile = sslContextService.getKeyStoreFile();

                if (!StringUtils.isEmpty(keyPassword) && !StringUtils.isEmpty(keyFile) ) {
                    clientSkeleton.secureKeyPassword(keyPassword);
                    clientSkeleton.secureKey(new File(keyFile));
                    sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.REQUIRED);
                } else {
                    sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.NONE);
                }

                try {
                    final KeyStore trustStore = KeyStoreUtils.getTrustStore(sslContextService.getTrustStoreType());
                    try (final InputStream trustStoreStream = new FileInputStream(new File(sslContextService.getTrustStoreFile()))) {
                        trustStore.load(trustStoreStream, sslContextService.getTrustStorePassword().toCharArray());
                    }

                    tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                    tmf.init(trustStore);

                    clientSkeleton.secureTrustManagerFactory(tmf);
                } catch (NoSuchAlgorithmException | CertificateException | KeyStoreException | IOException e) {
                    logger.error("Failed to initialize secure IRC service due to {}", new Object[]{e.getMessage()}, e);
                }

            }
        }
        try {
        this.ircClient = clientSkeleton.build();
        ircClient.getEventManager().registerEventListener(this);
        this.server = ircClient.getServerInfo().getAddress().get();
        // Setup the Server Handlers
        
        }
        catch (Throwable t) {
            t.printStackTrace();
        }
    }

    @OnDisabled
    public void shutdown() {
        Long timeOut = getConfigurationContext().getProperty(IRC_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);
        final StopWatch stopWatch = new StopWatch();

        stopWatch.start();

        this.ircClient.shutdown(clientIdentification + " - is going to rest a bit...");
        while (this.connectionStatus.get() || ( stopWatch.getElapsed(TimeUnit.MILLISECONDS) <= timeOut ) ) {
            // Wait for the disconnection
        }
        stopWatch.stop();
        logger.info("Disconnected from server after {} milliseconds", new Object[]{stopWatch.getDuration(TimeUnit.MILLISECONDS)});
    }

    static private final IrcMessageHandler NO_HANDLER = new IrcMessageHandler() {
        @Override
        public void handleMessage(IrcMessage message) {
            // Do Nothing
            
        }
    };

    @Override
    public void joinChannel(String handlerId, String channel, IrcMessageHandler handler) throws ProcessException {
        boolean doJoin = false;
        try {
            channelLock.lock();
            Map<String, IrcMessageHandler> handlers = channelHandlers.get(channel);
            if(handlers == null) {
                handlers = new ConcurrentHashMap<String, IrcMessageHandler>();
                channelHandlers.put(channel, handlers);
                doJoin = true;
            }
            else if(handlers.containsKey(handlerId)) {
                // shouldn't happen. Log
                logger.warn("Attempted to join {} after already being joined", new Object[]{channel});
            }
            if(handler != null) {
                handlers.put(handlerId, handler);
            } else {
                handlers.put(handlerId, NO_HANDLER);
            }
        } finally {
            channelLock.unlock();
        }
        if(doJoin) {
            ircClient.addChannel(channel);
        }
    }

    public void leaveChannel(String handlerId,String channel){
        boolean doLeave = false;
        try {
            channelLock.lock();
            Map<String, IrcMessageHandler> handlers = channelHandlers.get(channel);
            if(handlers == null || !handlers.containsKey(handlerId)) {
                logger.warn("Attempted to leave channel {} not already joined", new Object[]{channel});
                return;
            }
            handlers.remove(handlerId);
            if(handlers.isEmpty()) {
                channelHandlers.remove(channel);
                doLeave = true;
            }
        } finally {
            channelLock.unlock();
        }
        if(doLeave) {
            ircClient.removeChannel(channel);
        }
    }

//    @Handler(delivery = Invoke.Asynchronously)
//    protected void onPrivateMessageReceived(PrivateMessageEvent event) {
//        logger.info("Received private message '{}' from {} while waiting for messages on {} ",
//                new Object[] {event.getMessage(), event.getActor().getName(), context.getProperty(ConsumeIRC.IRC_CHANNEL).getValue()});
//        if (context.getProperty(ConsumeIRC.IRC_PROCESS_PRIV_MESSAGES).asBoolean()) {
//            turnEventIntoFlowFile(event);
//        } else {
//            event.sendReply(String.format("Hi %s. Thank you for your message but I am not looking to chat with strangers.",
//                    String.valueOf(event.getActor().getNick())));
//        }
//    }

    @Handler(delivery = Invoke.Asynchronously)
    protected void onChannelMessageReceived(ChannelMessageEvent event) {
        event.getChannel();
        try {
            channelLock.lock();
            final Map<String, IrcMessageHandler> handlers = channelHandlers.get(event.getChannel().getName());
            if(handlers == null) {
                logger.warn("message received for channel {} with no handlers", new Object[]{});
                return;
            }
            IrcMessage message = new WrappedMessageEvent(event);
            for (IrcMessageHandler handler : handlers.values()) {
                if(handler != NO_HANDLER) {
                    handler.handleMessage(message);
                }
            }
        }
        finally {
            channelLock.unlock();
        }
    }
    @Handler
    protected void onConnect(ClientConnectedEvent event) {
        connectionStatus.set(true);
        logger.info("Successfully connected to: " + event.getServerInfo().getAddress().get());
        // If not inside all the desired channel, try to join
//        if (!event.getClient().getChannels().contains(requestedChannels) || event.getClient().getChannels().isEmpty()) {
//            // chop the already joined channels
//            Set<String> pendingChannels = new HashSet<>(requestedChannels);
//            pendingChannels.removeAll(event.getClient().getChannels());
//            pendingChannels.forEach(pendingChannel -> event.getClient().addChannel(pendingChannel));
//        }
    }

    @Handler
    protected void onDisconnect(ClientConnectionClosedEvent event) {
        connectionStatus.set(false);
        // isReconnecting is used to KICL to state if re-connection is being attempted
        // e.g. connection dropped but client is trying to reconnect
        if (event.isReconnecting()) {
            logger.warn("Connection to IRC server dropped! Attempting to reconnect");
        } else {
            // This in theory should only be invoked during shutdown
            logger.info("Successfully disconnected from: " + event.getClient().getServerInfo().getAddress().get());
        }
    }

    @Handler(delivery = Invoke.Asynchronously)
    protected void onJoinComplete(RequestedChannelJoinCompleteEvent event) {
        logger.info("Joined channel {} ", new Object[] {event.getAffectedChannel().get().getName()});
    }

    @Override
    public void sendMessage(String channel, String message) {
        ircClient.sendMessage(channel, message);
    }

    @Override
    public String getServer() {
        return this.server;
    }
}
