package com.hellolvs;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * IP计数POJO
 *
 * @author lvs
 * @date 2017/12/08.
 */
public class IP implements Comparable<IP> {

    private String ip;
    private int count;

    public IP() {
    }

    public IP(String ip, int count) {
        this.ip = ip;
        this.count = count;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public int compareTo(IP o) {
        return Integer.compare(this.count, o.count);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
