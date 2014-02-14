package com.glassfitgames.glassfitserver;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

public class GlassFitServerClient {
    private final Socket socket;
    private ConcurrentLinkedQueue<byte[]> msgQueue = new ConcurrentLinkedQueue<byte[]>();
    
    private boolean running = true;
    
    public GlassFitServerClient(byte[] token, String host) throws UnknownHostException, IOException {
        this(token, host, GlassFitServer.DEFAULT_PORT);
    }
    public GlassFitServerClient(byte[] token, String host, int port) throws UnknownHostException, IOException {
        System.out.println("** Connecting");
        socket = new Socket(host, port);
        socket.setTcpNoDelay(true);

        System.out.println("** Authenticating");
        PacketBuffer.write(socket.getOutputStream(), token);
        System.out.println("** Waiting for response");
        byte[] packet = PacketBuffer.read(socket.getInputStream());
        if (packet == null) {
            disconnect();
            shutdown();
            throw new IOException("Server did not respond with a valid packet");
        }
        handle(packet);
    }
    
    public void loop() throws IOException {
        InputStream is = socket.getInputStream();
        OutputStream os = socket.getOutputStream();
        while (running) {
            boolean busy = false;
            byte[] data = msgQueue.poll();
            if (data != null) {
                PacketBuffer.write(os, data);
                System.out.println("** Sent " + data.length);
                busy = true;
            }
            if (is.available() > 0) {
                byte[] packet = PacketBuffer.read(is);
                if (packet == null) {
                    disconnect();
                    shutdown();
                    throw new IOException("Server did not respond with a valid packet");
                }
                handle(packet);            
                busy = true;
            }
            if (!busy) {
                try {
                    synchronized(this) {
                        this.wait(100);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        if (!socket.isClosed()) disconnect();
    }
    
    private void handle(byte[] data) {
        System.out.println("** Received " + data.length);
        if (data.length == 0) {
            onPing();
            return;
        }                
        ByteBuffer packet = ByteBuffer.wrap(data);                
        byte command = packet.get();
        
        switch (command) {
        case 0x01: {
            // User->user message
            int fromUid = packet.getInt();
            onUserMessage(fromUid, packet);
            break;
        }
        case 0x02: {
            // User->group message
            int fromUid = packet.getInt();
            int fromGid = packet.getInt();
            onGroupMessage(fromUid, fromGid, packet);
            break;
        }
        case 0x10: {
            // Created a group
            int groupId = packet.getInt();
            onGroupCreated(groupId);
            break;
        }
        default: {
            System.err.println("Unknown command: " + String.format("%02x", command) + " from server");
            disconnect();
            shutdown();
            return;
        }
        }
    }
    
    protected void onUserMessage(int fromUid, ByteBuffer data) {
        String message = new String(data.array(), data.position(), data.remaining());
        System.out.println("<" + fromUid + ">: " + message);        
    }
    
    protected void onGroupMessage(int fromUid, int fromGid, ByteBuffer data) {
        String message = new String(data.array(), data.position(), data.remaining());
        System.out.println("#" + fromGid + " <" + fromUid + ">: " + message);        
    }
    
    protected void onGroupCreated(int groupId) {
        System.out.println("* Created group " + groupId);        
    }
    
    protected void onPing() {        
    }
    
    protected void send(byte[] data) {
        msgQueue.add(data);
        synchronized(this) {
            this.notify();
        }
    }
    
    public void createGroup() {
        ByteBuffer command = ByteBuffer.allocate(1);
        command.put((byte)0x10);
        send(command.array());
        System.out.println("* Creating group..");
    }
    
    public void joinGroup(int groupId) {
        ByteBuffer command = ByteBuffer.allocate(1 + 4);
        command.put((byte)0x11);
        command.putInt(groupId);
        send(command.array());
        System.out.println("* Joining group " + groupId);
    }
    
    public void leaveGroup(int groupId) {
        ByteBuffer command = ByteBuffer.allocate(1 + 4);
        command.put((byte)0x12);
        command.putInt(groupId);
        send(command.array());
        System.out.println("* Leaving group " + groupId);
    }
    
    public void messageUser(int userId, byte[] data) {
        ByteBuffer command = ByteBuffer.allocate(1 + 4 + data.length);
        command.put((byte)0x01);
        command.putInt(userId);
        command.put(data);
        send(command.array());
        System.out.println(userId + ", " + new String(data));
    }
    
    public void messageGroup(int groupId, byte[] data) {
        ByteBuffer command = ByteBuffer.allocate(1 + 4 + data.length);
        command.put((byte)0x02);
        command.putInt(groupId);
        command.put(data);
        send(command.array());        
        System.out.println("#" + groupId + ", " + new String(data));
    }
    
    public void shutdown() {
        running = false;
    }
    
    public boolean isRunning() {
        return running;
    }
    
    public void disconnect() {
        try {
            socket.close();
        } catch (Exception e) {}
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("USAGE: client <access token>");
            System.exit(1);
        }
        System.out.println("Starting client");
        final GlassFitServerClient client = new GlassFitServerClient(args[0].getBytes(), "socket.raceyourself.com");
        System.out.println("Started client");
        Thread looper = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    client.loop();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            
        });
        looper.start();
        System.out.println("Started looper");
        
        try {
            Random random = new Random();
            int me = 1 + random.nextInt() % 2;
            System.out.println("Sleeping");
            Thread.sleep(random.nextInt(5000));
            System.out.println("Slept");
            int you = 1 + random.nextInt() % 2;
            client.messageUser(you, "Hello, bub".getBytes());
            System.out.println("Messaged " + you);
            System.out.println("Sleeping");
            Thread.sleep(5000);
            System.out.println("Slept");
//            client.createGroup();
//            System.out.println("Created group");
//            client.joinGroup(1);
//            System.out.println("Joined group");
//            client.messageGroup(1, "boo!".getBytes());
//            System.out.println("Messaged group");
//            client.leaveGroup(1);
//            System.out.println("left group");
            client.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }    
}
