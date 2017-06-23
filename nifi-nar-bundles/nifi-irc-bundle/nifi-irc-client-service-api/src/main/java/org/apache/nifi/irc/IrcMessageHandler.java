package org.apache.nifi.irc;

public interface IrcMessageHandler {
    public void handleMessage(IrcMessage message);
}
