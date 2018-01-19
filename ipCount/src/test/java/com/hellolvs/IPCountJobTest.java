package com.hellolvs;

import org.junit.Test;

/**
 * @author lvs
 * @date 2017/12/11.
 */
public class IPCountJobTest {
    @Test
    public void test() throws Exception {
        IPCountJob ipCountJob = new IPCountJob();
        IPCountJob.generateLog(600000000);
        ipCountJob.execute();
    }
}