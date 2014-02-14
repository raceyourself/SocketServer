package com.glassfitgames.glassfitserver;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class User {
    private final int id;
    private final Set<Integer> groups = new HashSet<Integer>();
    private final Set<SocketChannel> connections = new HashSet<SocketChannel>();
    
    public User(int id) {
        this.id = id;
    }
    
    public synchronized void addGroup(int groupId) {
        groups.add(groupId);
    }
    
    public synchronized void removeGroup(int groupId) {
        groups.remove(groupId);
    }
    
    public int getId() {
        return id;
    }
    
    public Set<Integer> getGroups() {        
        return Collections.unmodifiableSet(groups);
    }
    
    public boolean in(int groupId) {
        return groups.contains(groupId);
    }
    
    public synchronized void connected(SocketChannel channel) {
        connections.add(channel);
    }
    
    public synchronized void disconnected(SocketChannel channel) {
        connections.remove(channel);
    }
    
    public Set<SocketChannel> getConnections() {
        pruneConnections();
        return Collections.unmodifiableSet(connections);
    }

    public void pruneConnections() {
        Iterator<SocketChannel> iterator = connections.iterator();
        while(iterator.hasNext()) {
            SocketChannel ch = iterator.next();
            if (!ch.isConnected()) {
                try {
                    System.out.println("User " + id + " pruned " + ch.getRemoteAddress().toString());
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                iterator.remove();
            }
        }
    }
}
