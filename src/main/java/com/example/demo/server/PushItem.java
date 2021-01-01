package com.example.demo.server;

public class PushItem {
    private final String token;
    private final long id;
    private final int type;
    private final String id1;
    private final long id2;

    public PushItem(String token, long id, int type, String id1) {
        this.token = token;
        this.id = id;
        this.type = type;
        this.id1 = id1;
        this.id2 = 0;
    }

    public PushItem(String token, long id, int type, String id1, long id2) {
        this.token = token;
        this.id = id;
        this.type = type;
        this.id1 = id1;
        this.id2 = id2;
    }

    public long getId() {
        return id;
    }

    public int getType() {
        return type;
    }

    public String getId1() {
        return id1;
    }

    public long getId2() {
        return id2;
    }
}
