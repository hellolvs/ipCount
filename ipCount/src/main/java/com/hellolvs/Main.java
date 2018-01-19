package com.hellolvs;

import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

/**
 * 定时任务启动器
 * 
 * @author lvs
 * @date 2017/12/11.
 */
public class Main {
    public static void main(String[] args) throws Exception {
        // 生成模拟日志文件
        IPCountJob.generateLog(600000000);

        JobDetail job = JobBuilder.newJob(IPCountJob.class)
                .withIdentity("ipCountJob", "group1").build();

        Trigger trigger = TriggerBuilder
                .newTrigger()
                .withIdentity("ipCountTrigger", "group1")
                .withSchedule(
                        SimpleScheduleBuilder.simpleSchedule()
                                .withIntervalInMinutes(10).repeatForever())
                .build();

        Scheduler scheduler = new StdSchedulerFactory().getScheduler();
        scheduler.start();
        scheduler.scheduleJob(job, trigger);
    }
}
