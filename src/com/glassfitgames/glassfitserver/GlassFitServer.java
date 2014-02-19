package com.glassfitgames.glassfitserver;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import javax.sql.DataSource;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class GlassFitServer {
    private final ConcurrentHashMap<SocketChannel, Queue<Timestamped<byte[]>>> messageQueues = new ConcurrentHashMap<SocketChannel, Queue<Timestamped<byte[]>>>();
    private final ConcurrentHashMap<SocketChannel, PacketBuffer> readBuffers = new ConcurrentHashMap<SocketChannel, PacketBuffer>();
    private final ConcurrentHashMap<SocketChannel, ByteBuffer> writeBuffers = new ConcurrentHashMap<SocketChannel, ByteBuffer>();
    private final ConcurrentHashMap<SocketChannel, Long> connections = new ConcurrentHashMap<SocketChannel, Long>();
    private final ConcurrentHashMap<SocketChannel, Ping> pings = new ConcurrentHashMap<SocketChannel, Ping>();
    
    private final Users users;
    private final ConcurrentHashMap<SocketChannel, Integer> connectedUsers = new ConcurrentHashMap<SocketChannel, Integer>();
    private final ConcurrentHashMap<Integer, Set<Integer>> usergroups = new ConcurrentHashMap<Integer, Set<Integer>>();
    
    private final Random random = new Random();
    
    public final static int DEFAULT_PORT = 9090;
    public final static long KEEPALIVE_INTERVAL = 30000;

    private boolean running = true;
    
    // Config
    private InetAddress hostAddress = null;
    private int port;
    private final String databaseDriver = "com.mysql.jdbc.Driver";
//    private final String databaseUrl = "jdbc:mysql://localhost/gf_core_development";
    private final String databaseUrl = "jdbc:mysql://localhost/gf_core";
    private final String databaseUser = "gf-server";
    private final String databasePassword = "securemewithclientcerts";
    
    private final Selector selector;    
    private final DataSource database;
    
    public GlassFitServer() throws IOException, SQLException, PropertyVetoException {
        this(DEFAULT_PORT);
    }

    public GlassFitServer(int port) throws IOException, SQLException, PropertyVetoException {
        this.port = port;
        selector = initSelector();
        
        ComboPooledDataSource database = new ComboPooledDataSource();
        database.setDriverClass(databaseDriver); // loads the jdbc driver
        database.setJdbcUrl(databaseUrl);
        database.setUser(databaseUser);
        database.setPassword(databasePassword);

        // pooling configuration
        database.setMinPoolSize(1);
        database.setAcquireIncrement(3);
        database.setMaxPoolSize(25);
        database.setMaxStatements(50);
        database.setMaxIdleTime(60);
        database.setTestConnectionOnCheckout(true);
        database.setPreferredTestQuery("SELECT 1");
        
        this.database = database;
        
        users = new Users(this.database);
    }
    
    public void messageUser(User user, byte[] data) throws IOException {
        Set<SocketChannel> connections = user.getConnections();
        if (connections.isEmpty()) System.out.println("No connections for user " + user.getId());
        for (SocketChannel socketChannel : connections) {
            messageSocket(socketChannel, data);
        }
    }
    
    private void messageSocket(SocketChannel socketChannel, byte[] data) throws IOException {
        Queue<Timestamped<byte[]>> deque = messageQueues.get(socketChannel);
        if (deque != null) {
            if (deque.isEmpty() && writeBuffers.get(socketChannel) == null) {
                // Attempt to write straight away if nothing queued
                if (writeBuffers.putIfAbsent(socketChannel, ByteBuffer.wrap(data)) == null) {
                    write(socketChannel);
                } else {
                    // Fall back to queue
                    deque.add(new Timestamped<byte[]>(data));                    
                }
            } else {
                // Add to queue
                deque.add(new Timestamped<byte[]>(data));
            }
        } else System.err.println("No queue for " + socketChannel.getRemoteAddress().toString());    
    }
    
    public void ping(SocketChannel socketChannel) throws IOException {
        Ping ping = new Ping(random.nextInt());
        pings.put(socketChannel, ping);
        ByteBuffer message = PacketBuffer.allocateMessageBuffer(1 + 4);
        message.put((byte)0x00);
        message.putInt(ping.getId());
        messageSocket(socketChannel, message.array());
    }

    public int createGroup() {
        int count = Integer.MAX_VALUE;
        int groupId;
        Set<Integer> userset = new HashSet<Integer>();
        do {
            // Generate a group id starting from 1
            groupId = 1 + random.nextInt(Integer.MAX_VALUE-1);
            if (count-- < 0) return -1; // No groups left in address space 
                                        // TODO: Create short circuit/create limit in case of possible DoS vector
        } while (usergroups.putIfAbsent(groupId, userset) != null);
        return groupId;
    }
    
    public boolean messageGroup(int fromId, int groupId, byte[] data) throws IOException {
        Set<Integer> userIds = usergroups.get(groupId);
        if (userIds == null) return false;
        
        for(Integer userId : userIds) {
            if (userId == fromId) continue; // Don't echo back
            User user = users.get(userId);
            if (user != null) messageUser(user, data);
            else System.err.println("Unknown user " + userId + " in group " + groupId);
        }
        
        return true;
    }

    public void loop() {
        try {
            while (running) {
                
                selector.select();
                long loop = System.currentTimeMillis();
                Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
                while (selectedKeys.hasNext()) {
                    SelectionKey key = selectedKeys.next();
                    selectedKeys.remove();

                    if (!key.isValid()) {
                        continue;
                    }

                    // Check what event is available and deal with it
                    try {
                        if (key.isAcceptable()) {
                            accept(key);
                        } else if (key.isReadable()) {
                            read(key);
                        } else if (key.isWritable()) {
                            write(key);
                        }
                    } catch (Throwable t) {
                        SocketChannel socketChannel = (SocketChannel)key.channel();
                        System.err.println(t.getMessage() + " for " + socketChannel.getRemoteAddress().toString());
                        t.printStackTrace();
                        cancel(key);
                    }
                }
                loop = System.currentTimeMillis() - loop;
                if (loop > 100) System.err.println("Cycle took " + loop + "ms");

            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void accept(SelectionKey key) throws IOException {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();

        SocketChannel socketChannel = serverSocketChannel.accept();
        socketChannel.configureBlocking(false);
        socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        socketChannel.register(selector, SelectionKey.OP_READ);

        readBuffers.putIfAbsent(socketChannel, new PacketBuffer());
        messageQueues.putIfAbsent(socketChannel, new LinkedBlockingQueue<Timestamped<byte[]>>());
        
        System.out.println("Client " + socketChannel.getRemoteAddress().toString() + " connected");
    }

    private void read(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();

        PacketBuffer buffer = readBuffers.get(socketChannel);
        ByteBuffer readBuffer = buffer.getBuffer();
       
        // Attempt to read off the channel
        int numRead;
        try {
            numRead = socketChannel.read(readBuffer);
        } catch (IOException e) {
            System.out.println("Forceful shutdown of " + socketChannel.getRemoteAddress().toString() + " due to: ");
            e.printStackTrace();
            cancel(key);
            return;
        }

        if (numRead == -1) {
            System.out.println("Graceful shutdown of " + socketChannel.getRemoteAddress().toString());
            cancel(key);
            return;
        }
        
        System.out.println("Received " + numRead + "B from " + socketChannel.getRemoteAddress().toString());
        
        if (buffer.isInvalid()) {
            System.out.println("Disconnected " + socketChannel.getRemoteAddress().toString() + " due to protocol error: " + buffer.getError());
            cancel(key);
            return;
        }
        
        byte[] packet = buffer.getPacket();
        if (packet == null) return;
        if (packet != null) System.out.println("Received a packet of length " + packet.length + "B from " + socketChannel.getRemoteAddress().toString());
        
        Integer userId = connectedUsers.get(socketChannel);
        if (userId == null) {
            synchronized (socketChannel) {
                userId = connectedUsers.get(socketChannel);
                if (userId == null) {
                    User user = authenticate(key, packet);
                    if (user != null) {
                        if (connectedUsers.putIfAbsent(socketChannel, user.getId()) != null) throw new RuntimeException("Concurrency error");
                        user.connected(socketChannel);
                        userId = user.getId();
                        System.out.println("Client " + socketChannel.getRemoteAddress().toString() + " authenticated as " + user.getId());
                        
                        // Acknowledge auth by pinging client
                        ping(socketChannel);
                        
                        packet = buffer.getPacket(); // Check for next packet
                    } else {
                        System.out.println("Bad authentication from " + socketChannel.getRemoteAddress().toString() + ", disconnecting");
                        cancel(key);
                        return;
                    }
                }
            }
        }
        User user = users.get(userId);
        
        while (packet != null) {
            // NOTE: Potential CPU fairness issue with large buffers
            handle(key, user, packet);
            packet = buffer.getPacket();
        }
    }
    
    private void handle(SelectionKey key, User user, byte[] data) throws IOException {
        if (data.length == 0) {
            // PING
            return;
        }        
        final SocketChannel socketChannel = (SocketChannel)key.channel();
        final ByteBuffer packet = ByteBuffer.wrap(data);                
        final byte command = packet.get();
        
        switch (command) {
        case (byte)0x00: {
            // PING->PONG
            int pingId = packet.getInt();
            ByteBuffer message = PacketBuffer.allocateMessageBuffer(1 + 4);
            message.put((byte)0xff);
            message.putInt(pingId);
            message.put(packet);
            messageSocket(socketChannel, message.array());
            break;
        }
        case (byte)0xff: {
            // PONG
            int pingId = packet.getInt();
            Ping ping = pings.get(socketChannel);
            if (ping == null || ping.getId() != pingId) {
                System.err.println("Invalid pong from " + user.getId() + "/" + ((SocketChannel)key.channel()).getRemoteAddress().toString());
                break;
            }
            System.out.println(ping.Rtt() + "ms round-trip time to " + user.getId() + "/" + ((SocketChannel)key.channel()).getRemoteAddress().toString());
            break;
        }
        case (byte)0x01: {
            // User->user message
            int userId = packet.getInt();
            User recipient = users.get(userId);
            if (recipient == null) {
                System.err.println("Could not send message to user " + userId + " from " + user.getId() + "/" + ((SocketChannel)key.channel()).getRemoteAddress().toString());  
                break;
            }
            ByteBuffer message = PacketBuffer.allocateMessageBuffer(1 + 4 + packet.remaining());
            message.put((byte)0x01);
            message.putInt(user.getId());
            message.put(packet);
            messageUser(recipient, message.array());
            break;
        }
        case (byte)0x02: {
            // User->group message
            int groupId = packet.getInt();
            ByteBuffer message = PacketBuffer.allocateMessageBuffer(1 + 4 + 4 + packet.remaining());
            message.put((byte)0x02);
            message.putInt(user.getId());
            message.putInt(groupId);
            message.put(packet);
            if (!messageGroup(user.getId(), groupId, message.array())) {
                System.err.println("Could not send message to group " + groupId + " from " + user.getId() + "/" + ((SocketChannel)key.channel()).getRemoteAddress().toString());
            }
            break;
        }
        case (byte)0x10: {
            // Create a messaging group
            int groupId = createGroup();
            if (groupId < 0) {
                System.err.println("Group address space exhausted by " + user.getId() + "/" + ((SocketChannel)key.channel()).getRemoteAddress().toString());
                cancel(key);
                return;
            }
            System.out.println("Group " + groupId + " created by " + user.getId() + "/" + ((SocketChannel)key.channel()).getRemoteAddress().toString());
            user.addGroup(groupId);
            usergroups.get(groupId).add(user.getId());
            ByteBuffer response = PacketBuffer.allocateMessageBuffer(5);
            response.put((byte)0x10);
            response.putInt(groupId);
            messageUser(user, response.array());
            break;
        }
        case (byte)0x11: {
            // Join a messaging group
            int groupId = packet.getInt();            
            Set<Integer> userset = usergroups.get(groupId);
            if (userset != null) {
                userset.add(user.getId());
                user.addGroup(groupId);
                System.out.println("Group " + groupId + " joined by " + user.getId() + "/" + ((SocketChannel)key.channel()).getRemoteAddress().toString());
            }
            break;
        }
        case (byte)0x12: {
            // Leave a messaging group
            int groupId = packet.getInt();            
            Set<Integer> userset = usergroups.get(groupId);
            if (userset != null) {
                userset.remove(user.getId());
                user.removeGroup(groupId);
                System.out.println("Group " + groupId + " left by " + user.getId() + "/" + ((SocketChannel)key.channel()).getRemoteAddress().toString());
                if (userset.isEmpty()) usergroups.remove(groupId, userset);
                System.out.println("Group " + groupId + " freed");
            }
            break;
        }
        default: {
            System.err.println("Unknown command: " + String.format("%02x", command) + " from " + user.getId() + "/" + ((SocketChannel)key.channel()).getRemoteAddress().toString());
            cancel(key);
            return;
        }
        }
    }

    private User authenticate(SelectionKey key, byte[] packet) throws IOException {
        if (packet == null) return null;

        String token = new String(packet, "UTF-8");        
        User user = users.fromToken(token);
        
        return user;
    }
    
    private void write(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        write(socketChannel);
    }
    private void write(SocketChannel socketChannel) throws IOException {
        // NOTE: Not thread-safe. Only one thread may ever write to the same channel at a time!
        // TODO: Timeout idle connections?
            
        // Continue partial write
        ByteBuffer packet = writeBuffers.get(socketChannel);
        if (packet != null) {
            int numWritten = socketChannel.write(packet);
            connections.put(socketChannel, System.currentTimeMillis());
            if (numWritten > 0) System.out.println("Sent " + numWritten + "B to " + socketChannel.getRemoteAddress().toString());
            if (packet.remaining() > 0) {                
                socketChannel.keyFor(selector).interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                System.err.println("Buffer still full for " + socketChannel.getRemoteAddress().toString());
                return;
            }
            writeBuffers.remove(socketChannel);
        }
        
        // Send queued user messages
        Queue<Timestamped<byte[]>> queue = messageQueues.get(socketChannel);
        if (queue != null) {
            Timestamped<byte[]> data;
            while ( (data = queue.poll()) != null) {
                packet = ByteBuffer.wrap(data.getValue());
                int numWritten = socketChannel.write(packet);
                connections.put(socketChannel, System.currentTimeMillis());
                if (numWritten > 0) System.out.println("Sent " + numWritten + "B to " + socketChannel.getRemoteAddress().toString() + " delay: " + (System.currentTimeMillis()-data.getTimestamp()) + "ms");
                if (packet.remaining() > 0) {
                    socketChannel.keyFor(selector).interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                    System.err.println("Buffer full for " + socketChannel.getRemoteAddress().toString());
                    if (writeBuffers.putIfAbsent(socketChannel, packet) != null) throw new RuntimeException("Concurrency error");
                    return;
                }
            }
        } // else not connected

        // Everything queued should now be written
        socketChannel.keyFor(selector).interestOps(SelectionKey.OP_READ);
    }
       
    private Selector initSelector() throws IOException {
        Selector socketSelector = SelectorProvider.provider().openSelector();

        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);

        InetSocketAddress isa = new InetSocketAddress(hostAddress, port);
        serverChannel.socket().bind(isa);
        serverChannel.register(socketSelector, SelectionKey.OP_ACCEPT);

        System.out.println("Server started on " + serverChannel.getLocalAddress().toString());
        return socketSelector;
    }

    private void cancel(SelectionKey key) {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        try {
            socketChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        key.cancel();
        readBuffers.remove(socketChannel);
        writeBuffers.remove(socketChannel);
        messageQueues.remove(socketChannel);
        connections.remove(socketChannel);
        pings.remove(socketChannel);
        Integer userId = connectedUsers.remove(socketChannel);
        if (userId != null) {
            User user = users.get(userId);
            if (user != null) {
                user.disconnected(socketChannel);
            }
        }
        // TODO: Disconnect hooks
    }
    
    public void shutdown() {
        running = false;
    }
    
    public static void main(String[] args) throws Throwable {
        System.out.println("Starting server..");
        final GlassFitServer server = new GlassFitServer();
        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            @Override
            public void run()
            {
                System.out.println("Shutting down server..");
                server.shutdown();
            }
        });
        server.loop();
    }
    
    private static class Timestamped<T> {
        private final long timestamp;
        private final T value;
        
        public Timestamped(T value) {
            timestamp = System.currentTimeMillis();
            this.value = value;
        }
        
        public long getTimestamp() {
            return timestamp;
        }
        public T getValue() {
            return value;
        }
    }    
}