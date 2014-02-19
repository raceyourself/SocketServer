package com.glassfitgames.glassfitserver;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.commons.io.IOUtils;

public class PacketBuffer  {
    private static final int HEADER_LENGTH = 8;
    private static final int HEADER_MARKER = 0xd34db33f;    
    
    private ByteBuffer buffer = ByteBuffer.allocate(1024);
    private Integer length = null;
    private boolean invalid = false;
    private String error = null;
    
    public static final byte[] PING = new byte[] { (byte)0xd3, (byte)0x4d, (byte)0xb3, (byte)0x3f, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00 }; // 0-length packet
    
    public PacketBuffer() {
        // Network byte order
        buffer.order(ByteOrder.BIG_ENDIAN);
    }
    
    public ByteBuffer getBuffer() {
        return buffer;
    }
    
    public synchronized byte[] getPacket() {
        if (length == null && !parseHeader()) return null;
        if (isInvalid()) return null;        
        if (buffer.position() < HEADER_LENGTH + length) return null;
        
        byte[] packet = new byte[length];
        System.arraycopy(buffer.array(), HEADER_LENGTH, packet, 0, length);
        synchronized (this) {
            // NOTE: This will be slow if we use a big buffer and many small packets. Rewrite to get all packets before compacting in that case.
            buffer.flip();
            buffer.position(HEADER_LENGTH + length);
            buffer.compact();
            length = null; 
            invalid = false;
            error = null;
        }
        return packet;
    }
    
    public boolean parseHeader() {
        if (length != null) return true;        
        if (buffer.position() < HEADER_LENGTH) return false;         
        
        if (buffer.getInt(0) != HEADER_MARKER) {
            error = "invalid header marker";
            invalid = true;
            return false;
        }
        length = buffer.getInt(4);
        if (length < 0) {
            error = "negative payload length";
            invalid = true;
            return false;
        }
        // NOTE: A very large payload length may exhaust memory/trigger a DoS. Set a lower limit if necessary
        if (HEADER_LENGTH + length > buffer.capacity()) {
            synchronized (this) {
                if (HEADER_LENGTH + length > buffer.capacity()) {
                    ByteBuffer old = buffer;
                    buffer = ByteBuffer.allocate(HEADER_LENGTH + length);
                    // Network byte order
                    buffer.order(ByteOrder.BIG_ENDIAN);
                    old.flip();
                    buffer.put(old);
                }
            }
        }
        return true;
    }
    
    public boolean isInvalid() {
        if (!invalid) parseHeader();
        return invalid;
    }
    
    public String getError() {
        if (isInvalid() && error == null) error = "unknown error";
        return error;
    }
    
    public static ByteBuffer allocateMessageBuffer(int payloadLength) {
        ByteBuffer packet = ByteBuffer.allocate(HEADER_LENGTH + payloadLength);
        packet.order(ByteOrder.BIG_ENDIAN);
        packet.putInt(HEADER_MARKER);
        packet.putInt(payloadLength);
        return packet;
    }
    
    private static ByteBuffer header = ByteBuffer.allocate(8);
    public static synchronized void write(OutputStream os, byte[] data) throws IOException {
        /// Packetize using a simple header
        header.clear();
        header.order(ByteOrder.BIG_ENDIAN); // Network byte order
        // Marker
        header.putInt(0xd34db33f);
        // Length
        header.putInt(data.length);
        header.flip();
        os.write(header.array(), header.arrayOffset(), header.limit());
        os.write(data);
    }
    
    public static synchronized byte[] read(Socket socket) throws IOException {
        InputStream is = socket.getInputStream();
        // 250ms timeout for first byte so we don't block indefinitely
        socket.setSoTimeout(250);
        int b = is.read();
        socket.setSoTimeout(0);
        if (b != 0xd3 || is.read() != 0x4d || is.read() != 0xb3 || is.read() != 0x3f) return null; // Invalid header marker
        return readRest(is);
    }
    public static synchronized byte[] read(InputStream is) throws IOException {
        if (is.read() != 0xd3 || is.read() != 0x4d || is.read() != 0xb3 || is.read() != 0x3f) return null; // Invalid header marker
        return readRest(is);
    }
    
    private static ByteBuffer headerlength = ByteBuffer.allocate(4);
    private static synchronized byte[] readRest(InputStream is) throws IOException {
        headerlength.clear();
        headerlength.order(ByteOrder.BIG_ENDIAN); // Network byte order
        headerlength.put((byte)is.read());
        headerlength.put((byte)is.read());
        headerlength.put((byte)is.read());
        headerlength.put((byte)is.read());
        headerlength.flip();
        int length = headerlength.getInt();
        if (length < 0) return null;
        
        byte[] data = new byte[length];
        IOUtils.readFully(is, data);
        return data;
    }
}
