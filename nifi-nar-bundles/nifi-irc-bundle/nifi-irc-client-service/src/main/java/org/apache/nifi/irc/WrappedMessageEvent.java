package org.apache.nifi.irc;

import org.kitteh.irc.client.library.event.channel.ChannelMessageEvent;
import org.kitteh.irc.client.library.event.user.PrivateMessageEvent;

// NOTE: keeps references
public class WrappedMessageEvent implements IrcMessage {
    
    private final String message;
    private final String sender;
    private final String channel;
    private final String server;
    public WrappedMessageEvent(PrivateMessageEvent p){
        channel = null;
        sender = p.getActor().getName();
        server = p.getClient().getServerInfo().getAddress().get();
        message = p.getMessage();
    }
    public WrappedMessageEvent(ChannelMessageEvent c){
        channel = c.getChannel().getName();
        sender = c.getActor().getName();
        server = c.getClient().getServerInfo().getAddress().get();
        message = c.getMessage();
    }
    @Override
    public String getMessage() {
        return message;
    }
    @Override
    public String getSender() {
        return sender;
    }
    @Override
    public String getChannel() {
        return channel;
    }
    @Override
    public boolean hasChannel() {
        return channel == null ? false : true;
    }
    @Override
    public boolean isPrivate() {
        return channel == null ? true : false;
    }
    @Override
    public String getServer() {
        return server;
    }
    
}
