package com.glassfitgames.glassfitserver;

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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class GlassFitServer {
    private final ConcurrentHashMap<SocketChannel, Queue<byte[]>> messageQueues = new ConcurrentHashMap<SocketChannel, Queue<byte[]>>();
    private final ConcurrentHashMap<SocketChannel, PacketBuffer> readBuffers = new ConcurrentHashMap<SocketChannel, PacketBuffer>();
    private final ConcurrentHashMap<SocketChannel, ByteBuffer> writeBuffers = new ConcurrentHashMap<SocketChannel, ByteBuffer>();
    
    private final ConcurrentHashMap<Integer, User> users = new ConcurrentHashMap<Integer, User>();
    private final ConcurrentHashMap<SocketChannel, Integer> connectedUsers = new ConcurrentHashMap<SocketChannel, Integer>();
    private final ConcurrentHashMap<Integer, Set<Integer>> usergroups = new ConcurrentHashMap<Integer, Set<Integer>>();
    
    private Random random = new Random();
    
    public final static int DEFAULT_PORT = 9090;

    private boolean running = true;
    
    private InetAddress hostAddress = null;
    private int port;
    private Selector selector;

    public GlassFitServer() throws IOException {
        this(DEFAULT_PORT);
    }

    public GlassFitServer(int port) throws IOException {
        this.port = port;
        selector = initSelector();
    }
    
    public void messageUser(User user, byte[] data) throws IOException {
        Set<SocketChannel> connections = user.getConnections();
        if (connections.isEmpty()) System.out.println("No connections for user " + user.getId());
        for (SocketChannel socketChannel : connections) {
            Queue<byte[]> deque = messageQueues.get(socketChannel);
            if (deque != null) {
                deque.add(data);
            } else System.err.println("No queue for " + socketChannel.getRemoteAddress().toString());
        }
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
    
    public boolean messageGroup(int groupId, byte[] data) {
        Set<Integer> users = usergroups.get(groupId);
        if (users == null) return false;
        
        for(Integer user : users) {
            
        }
        
        return true;
    }

    public void loop() {
        while (running) {
            try {
                
                selector.select();
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

            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
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
        messageQueues.putIfAbsent(socketChannel, new LinkedBlockingQueue<byte[]>());
        
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
                        users.put(user.getId(), user);
                        userId = user.getId();
                        System.out.println("Client " + socketChannel.getRemoteAddress().toString() + " authenticated as " + user.getId());
                        
                        // Acknowledge auth by pinging client
                        messageUser(user, PacketBuffer.PING);
                        
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
        ByteBuffer packet = ByteBuffer.wrap(data);                
        byte command = packet.get();
        
        switch (command) {
        case 0x01: {
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
        case 0x02: {
            // User->group message
            int groupId = packet.getInt();
            ByteBuffer message = PacketBuffer.allocateMessageBuffer(1 + 4 + 4 + packet.remaining());
            message.put((byte)0x02);
            message.putInt(user.getId());
            message.putInt(groupId);
            message.put(packet);
            if (!messageGroup(groupId, message.array())) {
                System.err.println("Could not send message to group " + groupId + " from " + user.getId() + "/" + ((SocketChannel)key.channel()).getRemoteAddress().toString());
            }
            break;
        }
        case 0x10: {
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
        case 0x11: {
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
        case 0x12: {
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

        User user;
        String token = new String(packet, "UTF-8");
        if (token != null) user = new User(1+random.nextInt(2)); // TODO
        else return null;
        
        // Enable read/write once something is read
        key.channel().register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        return user;
    }
    
    private void write(SelectionKey key) throws IOException {
        // NOTE: Not thread-safe. Only one thread may ever write to the same channel at a time!
        // TODO: Timeout idle connections?
        
        SocketChannel socketChannel = (SocketChannel) key.channel();
        
        // Continue partial write
        ByteBuffer packet = writeBuffers.get(socketChannel);
        if (packet != null) {
            int numWritten = socketChannel.write(packet);
            if (numWritten > 0) System.out.println("Sent " + numWritten + "B to " + socketChannel.getRemoteAddress().toString());
            if (packet.remaining() > 0) {
                System.err.print("Buffer still full for " + socketChannel.getRemoteAddress().toString());
                return;
            }
            writeBuffers.remove(key);
        }

        // Send queued user messages
        Queue<byte[]> queue = messageQueues.get(socketChannel);
        if (queue != null) {
            byte[] data;
            while ( (data = queue.poll()) != null) {
                packet = ByteBuffer.wrap(data);
                int numWritten = socketChannel.write(packet);
                if (numWritten > 0) System.out.println("Sent " + numWritten + "B to " + socketChannel.getRemoteAddress().toString());
                if (packet.remaining() > 0) {
                    System.err.print("Buffer full for " + socketChannel.getRemoteAddress().toString());
                    if (writeBuffers.putIfAbsent(socketChannel, packet) != null) throw new RuntimeException("Concurrency error");
                    return;
                }
            }
        } // else not connected
        
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
    
    public static void main(String[] args) throws IOException {
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
}