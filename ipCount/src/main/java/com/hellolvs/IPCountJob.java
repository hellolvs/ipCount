package com.hellolvs;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 定时Job，每十分钟统计请求次数前k的ip
 *
 * @author lvs
 * @date 2017/12/08.
 */
public class IPCountJob implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(IPCountJob.class);

    private static final String LINE_SEPARATOR = StandardSystemProperty.LINE_SEPARATOR.value();
    private static final Charset UTF_8 = Charsets.UTF_8;

    private static final String INPUT_PATH = "/home/lvs/logs/ip.log";
    private static final String OUTPUT_PATH = "/home/lvs/logs/split/";

    private static final int SPLIT_NUM = 1024;
    private static final int TOP_K = 100;

    /**
     * 利用最小堆结构存储请求次数前k的IP
     */
    private List<IP> result = Lists.newArrayListWithExpectedSize(TOP_K);

    /**
     * 分割文件用，保存每个文件的写入流对象
     */
    private final Map<Integer, BufferedWriter> bufferMap = Maps.newHashMapWithExpectedSize(SPLIT_NUM);

    /**
     * 定时任务，每十分钟统计请求次数前k的IP
     */
    @Override
    public void execute(JobExecutionContext jobExecutionContext) {
        // 捕获异常，防止定时任务中断
        try {
            execute();
        } catch (Exception e) {
            LOG.error("定时任务出错：{}", e.getMessage(), e);
        }
    }

    /**
     * 统计大文件中请求次数前k的IP
     * 
     * @throws IOException I/O error
     */
    public void execute() throws IOException {
        // 这里应该每10分钟获取当前轮替日志文件路径，此处用常量路径模拟
        File ipLogFile = new File(INPUT_PATH);

        splitLog(ipLogFile, SPLIT_NUM);

        File logSplits = new File(OUTPUT_PATH);
        for (File logSplit : logSplits.listFiles()) {
            countTopK(logSplit, TOP_K);
        }

        MinHeap.sort(result);
        LOG.info("结果集:{}", result.size());
        for (int i = result.size() - 1; i >= 0; i--) {
            LOG.info("{}", result.get(i));
        }
    }

    /**
     * 生成模拟日志文件
     * 
     * @param logNum 生成日志条数
     * @throws IOException I/O error
     */
    public static void generateLog(long logNum) throws IOException {

        /* 创建文件 */
        File log = new File(INPUT_PATH);
        File parentDir = log.getParentFile();
        if (!parentDir.exists()) {
            parentDir.mkdirs();
        }
        log.createNewFile();

        /* 生成随机ip写入文件 */
        SecureRandom random = new SecureRandom();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(log))) {
            for (int i = 0; i < logNum; i++) {
                StringBuilder sb = new StringBuilder();
                sb.append("192.").append(random.nextInt(255)).append(".").append(random.nextInt(255)).append(".")
                        .append(random.nextInt(255)).append(LINE_SEPARATOR);
                bw.write(sb.toString());
            }
            bw.flush();
        }
    }

    /**
     * 分割日志文件
     *
     * @param logFile 待分割文件
     * @param fileNum 分割文件数量
     * @throws IOException I/O error
     */
    private void splitLog(File logFile, int fileNum) throws IOException {

        /* 为每个分割文件创建写入流对象 */
        for (int i = 0; i < fileNum; i++) {
            File file = new File(OUTPUT_PATH + i);
            File parentDir = file.getParentFile();
            if (!parentDir.exists()) {
                parentDir.mkdirs();
            }
            bufferMap.put(i, new BufferedWriter(new FileWriter(file)));
        }

        /* 根据ip的hashcode将数据分割到不同文件中 */
        LineIterator it = null;
        try {
            it = FileUtils.lineIterator(logFile, "UTF-8");
            while (it.hasNext()) {
                String ip = it.nextLine();
                int hashCode = Objects.hashCode(ip);
                hashCode = hashCode < 0 ? -hashCode : hashCode;
                BufferedWriter writer = bufferMap.get(hashCode % fileNum);
                writer.write(ip + LINE_SEPARATOR);
            }
        } finally {
            /* 释放资源 */
            LineIterator.closeQuietly(it);
            for (Map.Entry<Integer, BufferedWriter> buffer : bufferMap.entrySet()) {
                BufferedWriter writer = buffer.getValue();
                writer.flush();
                writer.close();
            }
            bufferMap.clear();
        }
    }

    /**
     * 统计请求次数前k的IP
     *
     * @param logSplit 当前分割文件
     * @param k top k
     * @throws IOException I/O error
     */
    private void countTopK(File logSplit, int k) throws IOException {

        /* 读取文件对ip计数 */
        HashMap<String, AtomicInteger> ipCountMap = Files.readLines(logSplit, UTF_8,
                new LineProcessor<HashMap<String, AtomicInteger>>() {
                    private HashMap<String, AtomicInteger> ipCountMap = Maps.newHashMap();

                    @Override
                    public boolean processLine(String line) throws IOException {
                        AtomicInteger ipCount = ipCountMap.get(line.trim());
                        if (ipCount != null) {
                            ipCount.getAndIncrement();
                        } else {
                            ipCountMap.put(line.trim(), new AtomicInteger(1));
                        }
                        return true;
                    }

                    @Override
                    public HashMap<String, AtomicInteger> getResult() {
                        return ipCountMap;
                    }
                });

        /* 前k条数据用来构建初始最小堆，之后的数据比堆顶大则替换堆顶并调堆 */
        for (Map.Entry<String, AtomicInteger> entry : ipCountMap.entrySet()) {
            IP ip = new IP(entry.getKey(), entry.getValue().get());
            if (result.size() != k) {
                result.add(ip);
                if (result.size() == k) {
                    MinHeap.initMinHeap(result);
                }
            } else {
                if (ip.compareTo(result.get(0)) > 0) {
                    result.set(0, ip);
                    MinHeap.adjust(result, 0, k);
                }
            }
        }
    }

    /**
     * 返回统计结果
     *
     * @return 结果集合
     */
    public List<IP> getResult() {
        return result;
    }
}
