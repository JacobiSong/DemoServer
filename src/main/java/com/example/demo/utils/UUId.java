package com.example.demo.utils;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class UUId {

    private static final UUId me = new UUId();
    private String hostAddr;
    private final Random random = new SecureRandom();
    private final UniqTimer timer = new UniqTimer();

    private UUId() {
        try {
            final InetAddress addr = InetAddress.getLocalHost();

            hostAddr = addr.getHostAddress();
        }
        catch (final IOException e) {
            System.err.println("[UniqID] Get HostAddr Error"+e);
            hostAddr = String.valueOf(System.currentTimeMillis());
        }

        if (null == hostAddr || hostAddr.trim().length() == 0 || "127.0.0.1".equals(hostAddr)) {
            hostAddr = String.valueOf(System.currentTimeMillis());

        }
        hostAddr = hostAddr.substring(hostAddr.length()-2).replace(".", "0");
    }


    /**
     * 获取UniqID实例
     *
     * @return UniqId
     */
    public static UUId getInstance() {
        return me;
    }

    /**
     * 获得不会重复的毫秒数
     *
     * @return 不会重复的时间
     */
    public String getUniqTime() {
        return timer.getCurrentTime();
    }

    /**
     * 获得UniqId
     *
     * @return uniqTime-randomNum-hostAddr-threadId
     */
    public String getUniqID() {
        final StringBuilder sb = new StringBuilder();
        final String t = getUniqTime();
        int randomNumber = random.nextInt(8999999) + 1000000;
        sb.append(t);
        sb.append(hostAddr);
        sb.append(getUniqThreadCode());
        sb.append(randomNumber);
        return sb.toString();
    }

    public String getUniqThreadCode(){
        String threadCode = StringUtils.left(String.valueOf(Thread.currentThread().hashCode()),9);
        return StringUtils.leftPad(threadCode, 9, "0");
    }

    /**
     * 实现不重复的时间
     */
    private class UniqTimer {
        private final AtomicLong lastTime = new AtomicLong(System.currentTimeMillis());

        public String getCurrentTime() {
            if(!timestamp2Date(this.lastTime.incrementAndGet()).equals(timestamp2Date(System.currentTimeMillis()))){
                lastTime.set(System.currentTimeMillis()+random.nextInt(10000));
            }
            return timestamp2Datetime(this.lastTime.incrementAndGet());
        }
    }

    /**
     * 规范化日期，规范成yyyy-MM-dd
     * @param timestamp 时间戳
     * @return 规范化后的日期
     */
    public static String timestamp2Date(long timestamp){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return dateFormat.format(new Date(timestamp * 1000));
    }

    private static String timestamp2Datetime(long timestamp){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        return dateFormat.format(new Date(timestamp));
    }

}