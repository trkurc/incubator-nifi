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
package org.apache.nifi.processors.irc.handlers;

import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Invoke;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.irc.IrcMessage;
import org.apache.nifi.irc.IrcMessageHandler;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processors.irc.ConsumeIRC;
import org.apache.nifi.util.StopWatch;
import org.kitteh.irc.client.library.event.channel.ChannelMessageEvent;
import org.kitteh.irc.client.library.event.helper.MessageEvent;
import org.kitteh.irc.client.library.event.user.PrivateMessageEvent;
import org.kitteh.irc.client.library.util.Format;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class ConsumerEventHandler implements IrcMessageHandler {
    final ProcessContext context;
    final ProcessSessionFactory sessionFactory;
    final ComponentLog logger;

    public ConsumerEventHandler(ProcessContext context, ProcessSessionFactory sessionFactory, ComponentLog logger) {
        this.context = context;
        this.sessionFactory = sessionFactory;
        this.logger = logger;
    }

    @Override
    public void handleMessage(IrcMessage message) {
        final ProcessSession processSession = sessionFactory.createSession();
        final StopWatch watch = new StopWatch();
        watch.start();
        try {
            FlowFile flowFile = processSession.create();


            flowFile = processSession.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    if (context.getProperty(ConsumeIRC.IRC_STRIP_FORMATTING).asBoolean()) {
                        out.write(Format.stripAll(message.getMessage()).getBytes());
                    } else {
                        out.write(message.getMessage().getBytes());
                    }
                }
            });

            final Map<String, String> attributes = new HashMap<>();
            if (message.hasChannel()) {
                attributes.put("irc.channel", message.getChannel());
            }
            attributes.put("irc.sender", message.getSender());


            // But all come from servers
            attributes.put("irc.server", message.getServer());
            flowFile = processSession.putAllAttributes(flowFile, attributes);

            watch.stop();
            processSession.getProvenanceReporter()
                    .receive(flowFile, "irc://"
                            .concat(message.getServer())
                            .concat("/")
                            // Device if append channel to URI or not
                            .concat((message.hasChannel()) ? message.getChannel().concat("/") : ""),
                            watch.getDuration(TimeUnit.MILLISECONDS)
                            );

            processSession.transfer(flowFile, ConsumeIRC.REL_SUCCESS);
            processSession.commit();
        } catch (FlowFileAccessException | IllegalStateException ex) {
            logger.error("Unable to fully process input due to " + ex.getMessage(), ex);
            throw ex;
        } finally {
            processSession.rollback();
        }
    }



}

