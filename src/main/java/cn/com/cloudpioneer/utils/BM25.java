package cn.com.cloudpioneer.utils;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 搜索相关性评分算法
 *
 * @author hankcs
 */

public class BM25 {
    /**
     * 文档句子的个数
     */
    int D;

    /**
     * 文档句子的平均长度
     */
    private double avgdl;

    /**
     * 拆分为[句子[单词]]形式的文档
     */
    private List<List<String>> docs;

    /**
     * 文档中每个句子中的每个词与词频
     */
    private Map<String, Integer>[] f;

    /**
     * 文档中全部词和出现句子个数
     */
    private Map<String, Integer> df;

    /**
     * IDF
     */
    private Map<String, Double> idf;

    /**
     * 调节因子
     */
    final static float k1 = 1.5f;

    /**
     * 调节因子
     */
    final static float b = 0.75f;

    public BM25(List<List<String>> docs) {
        this.docs = docs;
        // 句子个数
        D = docs.size();
        for (List<String> sentence : docs) {
            avgdl += sentence.size();
        }
        avgdl /= D;
        // 初始化Map数组，句子数个Map
        f = new Map[D];
        // 词的句子数
        df = new TreeMap<String, Integer>();
        // 词的逆句子数
        idf = new TreeMap<String, Double>();
        // 计算f、df和idf的值
        init();
    }

    /**
     * 在构造时初始化自己的所有参数
     */
    private void init() {
        int index = 0;
        /*
        统计每个句子中各词的词频，和所有词的句子数
         */
        for (List<String> sentence : docs) {
            // 统计当前句子各词的词频
            Map<String, Integer> tf = new TreeMap<>();
            // sentence为每个句子的分词结果
            for (String word : sentence) {
                Integer freq = tf.get(word);
                freq = freq == null ? 1 : (freq + 1);
                tf.put(word, freq);
            }

            f[index] = tf;
            for (Map.Entry<String, Integer> entry : tf.entrySet()) {
                String word = entry.getKey();
                Integer freq = df.get(word);
                freq = freq == null ? 1 : (freq + 1);
                // 出现word的句子数freq
                df.put(word, freq);
            }
            index++;
        }

        /*
        统计完所有词的句子数后就可以计算逆句子数了
         */
        for (Map.Entry<String, Integer> entry : df.entrySet()) {
            String word = entry.getKey();
            Integer freq = entry.getValue();
            idf.put(word, Math.log(D - freq + 0.5) - Math.log(freq + 0.5));
//            idf.put(word, Math.log(D + 1) - Math.log(freq + 1));
        }
    }

    /**
     * 返回第index个句子和句子sentence的相似度
     *
     * @param sentence 句子sentence的分词结果
     * @param index    句子下标
     * @return 相似度
     */
    public double sim(List<String> sentence, int index) {
        double score = 0;
        for (String word : sentence) {
            // 若第index个句子的tfMap不包含词word
            if (!f[index].containsKey(word)) {
                continue;
            }
            // 第index个句子的词数
            int d = docs.get(index).size();
            // 第index个句子中词word的词频
            Integer wf = f[index].get(word);
            score += (
                    idf.get(word) * wf * (k1 + 1)
                            / (wf + k1 * (1 - b + b * d / avgdl))
            );
        }

        return score;
    }

    /**
     * 返回所有句子与句子sentence的相似度数组
     *
     * @param sentence 句子sentence分词结果
     * @return 相似度数组
     */
    public double[] simAll(List<String> sentence) {
        double[] scores = new double[D];
        for (int i = 0; i < D; ++i) {
            scores[i] = sim(sentence, i);
        }
        return scores;
    }
}
