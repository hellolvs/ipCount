package com.hellolvs;

import java.util.List;

/**
 * 最小堆
 *
 * @author lvs
 * @date 2017-12-12
 */
public class MinHeap {

    /**
     * 对最小堆排序
     * 
     * @param list 已经为最小堆结构的列表
     * @param <T> 元素须实现Comparable接口
     */
    public static <T extends Comparable<? super T>> void sort(List<T> list) {
        for (int i = list.size() - 1; i > 0; i--) {
            swap(list, 0, i);
            adjust(list, 0, i);
        }
    }

    /**
     * 初始化最小堆
     *
     * @param list 待初始化为最小堆的列表
     * @param <T> 元素须实现Comparable接口
     */
    public static <T extends Comparable<? super T>> void initMinHeap(List<T> list) {
        /* 从最后一个非叶节点开始至根节点依次调整 */
        for (int i = list.size() / 2 - 1; i >= 0; i--) {
            adjust(list, i, list.size());
        }
    }

    /**
     * 调堆
     *
     * @param list 当前堆
     * @param <T> 元素须实现Comparable接口
     * @param cur 待调整位置
     * @param length 当前堆大小
     */
    public static <T extends Comparable<? super T>> void adjust(List<T> list, int cur, int length) {
        T tmp = list.get(cur);
        for (int i = 2 * cur + 1; i < length; i = 2 * i + 1) {
            if (i + 1 < length && list.get(i).compareTo(list.get(i + 1)) > 0) {
                i++; // i指向孩子节点中最小的节点
            }
            if (tmp.compareTo(list.get(i)) > 0) {
                list.set(cur, list.get(i)); // 最小孩子节点调整到其父节点
                cur = i; // 当前节点置为最小孩子节点，继续调整
            } else {
                break; // 没有调整时退出循环
            }
        }
        list.set(cur, tmp); // 被调整节点最终存放位置
    }

    /**
     * 交换List中的元素
     * 
     * @param list 待交换列表
     * @param i 第一个元素位置
     * @param j 第二个元素位置
     */
    private static <T extends Comparable<? super T>> void swap(List<T> list, int i, int j) {
        T tmp = list.get(i);
        list.set(i, list.get(j));
        list.set(j, tmp);
    }
}
