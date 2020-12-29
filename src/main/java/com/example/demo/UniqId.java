package com.example.demo;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @ClassName: UniqId
 * @Description: 生成32位全局唯一id
 */
public class UniqId {

    private static UniqId me = new UniqId();
    private String hostAddr;
    private final Random random = new SecureRandom();
    private final UniqTimer timer = new UniqTimer();

    private boolean isOutputInfo = false;

    private UniqId() {
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

        if(isOutputInfo){
            System.out.println("[UniqID]hostAddr is:" + hostAddr + "----length:"+hostAddr.length());
        }
    }


    /**
     * 获取UniqID实例
     *
     * @return UniqId
     */
    public static UniqId getInstance() {
        me.isOutputInfo = false;
        return me;
    }

    /**
     * 获取UniqID实例
     *
     * @return UniqId
     */
    public static UniqId getInstanceWithLog() {
        me.isOutputInfo = true;
        return me;
    }


    /**
     * 获得不会重复的毫秒数
     *
     * @return 不会重复的时间
     */
    public String getUniqTime() {
        String time = timer.getCurrentTime();
        if(isOutputInfo){
            System.out.println("[UniqID.getUniqTime]" + time +"----length:"+ time.length());
        }
        return time;
    }

    /**
     * 获得UniqId
     *
     * @return uniqTime-randomNum-hostAddr-threadId
     */
    public String getUniqID() {
        final StringBuffer sb = new StringBuffer();
        final String t = getUniqTime();
        int randomNumber = random.nextInt(8999999) + 1000000;
        sb.append(t);
        sb.append(hostAddr);
        sb.append(getUniqThreadCode());
        sb.append(randomNumber);
        if (isOutputInfo) {
            System.out.println("[UniqID.randomNumber]" + randomNumber+"----length:"+String.valueOf(randomNumber).length());
            System.out.println("[UniqID.getUniqID]" + sb.toString()+"----length:"+String.valueOf(sb).length());
        }
        return sb.toString();
    }

    public String getUniqThreadCode(){
        String threadCode = StringUtils.left(String.valueOf(Thread.currentThread().hashCode()),9);
        if (isOutputInfo) {
            System.out.println("[UniqID.getUniqThreadCode]" +threadCode+"----length:"+threadCode.length());
        }
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
            return timestamp2Datetimes(this.lastTime.incrementAndGet());
        }
    }

    /**
     * 规范化日期，规范成yyyy-MM-dd
     * @param timestamp
     * @return
     */
    public static String timestamp2Date(long timestamp){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return dateFormat.format(new Date(timestamp * 1000));
    }

    private static String timestamp2Datetimes(long timestamp){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        return dateFormat.format(new Date(timestamp));
    }

}