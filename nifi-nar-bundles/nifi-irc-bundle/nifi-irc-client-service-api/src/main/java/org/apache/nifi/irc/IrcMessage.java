package org.apache.nifi.irc;

public interface IrcMessage {

    public String getMessage();
    public String getSender();
    public String getChannel();
    public boolean hasChannel(); 
    public boolean isPrivate();
    public String getServer();

}
