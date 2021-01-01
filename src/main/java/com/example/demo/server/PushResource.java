package com.example.demo.server;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PushResource {
    private int num = 0;
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    public void get() {
        try {
            lock.lock();
            while (num == 0) {
                try {
                    condition.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            num--;
            condition.signal();
        } finally {
            lock.unlock();
        }

    }

    public void put() {
        try {
            lock.lock();
            while (num == Integer.MAX_VALUE) {
                try {
                    condition.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            num++;
            condition.signal();
        }finally {
            lock.unlock();
        }

    }
}
