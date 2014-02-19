package com.glassfitgames.glassfitserver;

public class Ping {
    private final int pingId;
    private final long timestamp;
    
    public Ping(int pingId) {
        this.pingId = pingId;
        this.timestamp = System.currentTimeMillis();
    }
    
    public int getId() {
        return pingId;
    }
    
    public long Rtt() {
        return System.currentTimeMillis() - this.timestamp;
    }
}
