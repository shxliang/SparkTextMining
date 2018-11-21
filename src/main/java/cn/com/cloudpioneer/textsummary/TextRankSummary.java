package cn.com.cloudpioneer.textsummary;


import cn.com.cloudpioneer.utils.BM25;
import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.utility.TextUtility;

import java.util.*;

/**
 * 使用TextRank算法提取摘要
 *
 * @author lsx
 */

public class TextRankSummary {

    /**
     * 阻尼系数
     */
    private double d = 0.85;

    /**
     * 最大迭代次数
     */
    private int maxIter = 200;

    /**
     * 收敛精度
     */
    private double minDiff = 0.001;

    /**
     * 文档句子的个数
     */
    private int D;

    /**
     * 拆分为[句子[单词]]形式的文档
     */
    private List<List<String>> docs;

    /**
     * 存储未排序的<句子下标，权重值>
     */
    private Map<Integer, Double> map;

    /**
     * 两两句子间相似度矩阵
     */
    private double[][] weight;

    /**
     * 该句子和其他句子相关度之和
     */
    private double[] weightSum;

    /**
     * 最终句子TextRank值
     */
    private double[] vertex;

    /**
     * BM25相似度
     */
    private BM25 bm25;

    /**
     * 构造方法
     *
     * @param docs 拆分为[句子[单词]]形式的文档
     */
    private TextRankSummary(List<List<String>> docs) {
        this.docs = docs;
        // 计算句子间两两的BM25值
        bm25 = new BM25(docs);
        D = docs.size();
        // 句子间相似度矩阵，D*D维
        weight = new double[D][D];
        weightSum = new double[D];
        vertex = new double[D];
        map = new HashMap<>();
    }

    /**
     * 迭代计算句子TextRank值
     *
     * @param sentenceList 句子列表
     */
    private void calTextRank(List<String> sentenceList) {
        /*
        计算句子相似度矩阵，初始化TextRank值
         */
        for (int i = 0; i < docs.size(); i++) {
            // 当前句子与所有句子的BM25相似度
            double[] scores = bm25.simAll(docs.get(i));
//            System.out.println(Arrays.toString(scores));

            weight[i] = scores;
            // 当前句子与其他所有句子的BM25相似的和，也就是计算公式中V_j所有链出顶点的相似度的和
            weightSum[i] = sum(scores) - scores[i];
            // 存储当前句子（顶点）TextRank值，初始值为1
            vertex[i] = 1.0;
        }

        /*
        迭代计算更新TextRank值
         */
        for (int k = 0; k < maxIter; ++k) {
            // 当前迭代步的TextRank值
            double[] m = new double[D];
            double curMaxDiff = 0;
            for (int i = 0; i < D; ++i) {
                m[i] = 1 - d;
                for (int j = 0; j < D; ++j) {
                    if (j == i || weightSum[j] == 0) {
                        continue;
                    }
                    m[i] += (d * weight[j][i] / weightSum[j] * vertex[j]);
                }
                double diff = Math.abs(m[i] - vertex[i]);
                if (diff > curMaxDiff) {
                    curMaxDiff = diff;
                }
            }
            // 更新顶点PageRank值
            vertex = m;
            // max{|diff_i|} < minDiff，达到收敛精度则停止迭代
            if (curMaxDiff <= minDiff) {
                break;
            }
        }

        /*
        根据一些人工规则调整收敛后的句子权重
         */
        for (int i = 0; i < D; ++i) {
            // 若句子含有指示性短语则加大权重
            if (sentenceList.get(i).contains("综上所述")) {
                map.put(i, vertex[i] * 2);
            }// 句子是否为最后一句
            else if (i == (D - 1))
            // Y:降低最后一句的权重
            {
                map.put(i, vertex[i] * 0.9);
            } else {
                map.put(i, vertex[i]);
            }
//            System.out.println(docs.get(i)+":"+vertex[i]);
        }
    }

    /**
     * 获取权重前几的关键句子
     *
     * @param size 要提取的Top句子数
     * @return 关键句子的下标及PageRank值
     */
    private TopSentence getTopSentence(int size) {
        // 按map的value排序，map=(句子下标,权重值)
        ValueComparator bvc = new ValueComparator(map);

        // 存储排序好的<句子下标，权重值>
        TreeMap<Integer, Double> top = new TreeMap<>(bvc);
        top.putAll(map);

        Collection<Integer> keySet = top.keySet();
        // size=min{size, 全文句子个数}
        size = Math.min(size, keySet.size());
        // 选取后的句子下标
        List<Integer> indexList = new LinkedList<>();
        // 选取后的句子权重
        List<Double> valueList = new LinkedList<>();
        // 按权重排序后句子下标
        Iterator<Integer> it = keySet.iterator();
        // 按权重排序后的权重
        Iterator<Double> itd = top.values().iterator();

        int i = 0;
        // 摘要句去重相似度阈值
        double threshold = 10;
        while (it.hasNext()) {
            int curIndex = it.next();
            double curValue = itd.next();
            // 是否重复的句子
            boolean dup = false;
            if (i != 0) {
                /*
                将当前句子与已选取所有句子比较相似度
                 */
                for (int j = 0; j < i; j++) {
                    int pickedIndex = indexList.get(j);
                    // 若当前句子与已选取的句子相似度过高，则不选取当前句子
                    if (weight[curIndex][pickedIndex] >= threshold) {
                        dup = true;
                        break;
                    }
                }
            }
            if (dup) {
                continue;
            }
            indexList.add(curIndex);
            valueList.add(curValue);
            i++;
            if (i >= (size - 1)) {
                break;
            }
        }

        int[] indexResult = new int[indexList.size()];
        double[] valueResult = new double[valueList.size()];

        for (int j = 0; j < indexList.size(); j++) {
            indexResult[j] = indexList.get(j);
            valueResult[j] = valueList.get(j);
        }
        return new TopSentence(indexResult, valueResult);
    }

    /**
     * 数组求和
     *
     * @param array 数组
     * @return 求和值
     */
    private static double sum(double[] array) {
        double total = 0;
        for (double v : array) {
            total += v;
        }
        return total;
    }

    /**
     * 按分句标点符号将文章分割为句子
     *
     * @param document 文章正文
     * @return 文章句子列表
     */
    private static List<String> spiltSentence(String document) {
        List<String> sentences = new LinkedList<>();
        // 分段，遍历段落分句
        for (String line : document.split("[\r\n]+")) {
            line = line.trim();
            if (line.length() == 0) {
                continue;
            }

            // 分句符号[，,。:：？?！!；;\t]
            for (String sent : line.split("[。？?！!；;\t]+")) {
                sent = sent.replaceAll("　+", " ");
                sent = sent.replaceAll(" +", " ");
                sent = sent.trim();
                // 过滤过短的句子
                if (sent.length() <= 5) {
                    continue;
                }
                sentences.add(sent);
            }
        }
        return sentences;
    }

    /**
     * 对每个句子进行分词，返回每个句子的分词结果
     *
     * @param sentenceList 文章句子列表
     * @return [[word11, word12, ...], [word21, word22, ...], ...]
     */
    private static List<List<String>> segmentSentenceList(List<String> sentenceList) {
        List<List<String>> docs = new LinkedList<>();
        for (String sentence : sentenceList) {
            List<Term> termList = HanLP.newSegment()
                    .enableNumberQuantifierRecognize(true)
                    .seg(sentence);
            CoreStopWordDictionary.apply(termList);
            List<String> wordList = new LinkedList<>();
            for (Term term : termList) {
                wordList.add(term.word);
            }
            docs.add(wordList);
        }
        return docs;
    }

    /**
     * @param document  目标文档
     * @param maxLength 提取的摘要长度
     * @param d         阻尼系数
     * @param maxIter   最大迭代次数
     * @param minDiff   收敛精度
     * @return 摘要文本
     */
    public static String getSummary(String document, int maxLength, double d, int maxIter, double minDiff) {
        // 清洗文本
        document = document.replaceAll("　+", " ")
                .replaceAll(" +", " ")
                .replaceAll("&nbsp;", "\n")
                .replaceAll("&nbsp", "\n")
                .replaceAll("\n+", "\n")
                .trim();

        int summaryLength = maxLength;
        // 若文章长度小于用户设定的提取摘要长度，则将摘要长度设置为文章长度的一半
        if (document.length() <= summaryLength) {
            summaryLength = document.length() / 2;
        }

        // 切分句子，全文句子列表
        List<String> sentenceList = spiltSentence(document);

        // 句子数
        int sentenceCount = sentenceList.size();
        // 文章长度
        int documentLength = document.length();

        // 若分句后句子数为0，则直接返回文章原文
        if (sentenceCount == 0) {
            return document;
        }

        // 平均句子长度 = 文章长度 / 句子数
        int sentenceLengthAvg = documentLength / sentenceCount;
        // 应提取的候选摘要句子数 = 所需摘要长度 / 平均句子长度
        int size = summaryLength / sentenceLengthAvg;

        // 对句子列表中的每个句子分词
        List<List<String>> docs = segmentSentenceList(sentenceList);

        TextRankSummary textRankSummary = new TextRankSummary(docs);
        textRankSummary.d = d;
        textRankSummary.maxIter = maxIter;
        textRankSummary.minDiff = minDiff;
        textRankSummary.calTextRank(sentenceList);

        // 返回Top-size个候选摘要句的下标（在句子列表中的下标）和权重值
        TopSentence topSentence = textRankSummary.getTopSentence(size);

        // 若返回提取的只有一个句子，则直接返回该句子
        if (topSentence.index.length == 1) {
            return sentenceList.get(topSentence.index[0]) + "。";
        }
        // 若返回提取的句子为空，则返回全文第一句
        if (topSentence.index.length == 0) {
            return sentenceList.get(0) + "。";
        }

        // 候选摘要句列表
        List<String> resultList = new LinkedList<>();
        List<Double> valueList = new LinkedList<>();

        for (int i = 0; i < topSentence.value.length; i++) {
            // 从全文句子列表中按下标取到候选摘要句，已按权重降序排序
            resultList.add(sentenceList.get(topSentence.index[i]).trim());
            // 候选摘要句的权重
            valueList.add(topSentence.value[i]);
        }
//        System.out.println(resultList);
//        System.out.println(valueList);

        // 将候选摘要句resultList按全文句sentenceList顺序排序
        SentenceResult result = permutation(resultList, valueList, sentenceList);
        // 对候选摘要句列表选取不超过最大长度的句子们，按句子顺序优先选取，本质上加大了文章靠前句子的权重
        result = pickSentences(result.resultList, result.valueList, summaryLength);
        // 将挑选后的摘要句resultList按全文句sentenceList顺序排序
        result = permutation(result.resultList, result.valueList, sentenceList);
        resultList = result.resultList;

        // 将挑选后的句子用句号拼接生成为摘要
        String summary = TextUtility.join("。", resultList).trim();
        // 若生成的摘要长度为0，则返回候选摘要句中权重最大的一句
        if (summary.length() == 0) {
            return sentenceList.get(topSentence.index[0]) + "。";
        }
        return summary;
    }

    /**
     * 将候选摘要句按全文句子顺序进行排序
     *
     * @param resultList   候选摘要句句子列表
     * @param valueList    候选摘要句权重列表
     * @param sentenceList 全文句子列表
     * @return 排序后的句子列表
     */
    private static SentenceResult permutation(List<String> resultList, List<Double> valueList, List<String> sentenceList) {
        List<String> paraResultList = new ArrayList<>();
        List<Double> paraValueList = new ArrayList<>();
        List<String> paraSentenceList = new ArrayList<>();
        paraResultList.addAll(resultList);
        paraValueList.addAll(valueList);
        paraSentenceList.addAll(sentenceList);

        // 句子x在全文句子列表中的下标
        int indexBufferX;
        int indexBufferY;
        // 句子x
        String senX;
        String senY;
        // 句子x的权重
        double valX;
        double valY;
        // 提取的摘要句子数
        int length = paraResultList.size();

        // bubble sort derivative
        for (int i = 0; i < length; i++) {
            for (int offset = 0; offset < length - i; offset++) {
                senX = paraResultList.get(i);
                senY = paraResultList.get(i + offset);
                valX = paraValueList.get(i);
                valY = paraValueList.get(i + offset);
                // 摘要句在全文句列表中的下标
                indexBufferX = paraSentenceList.indexOf(senX);
                indexBufferY = paraSentenceList.indexOf(senY);
                // 若摘要句在摘要句列表中的先后顺序与在全文句列表中的先后顺序不一致，则翻转在摘要句列表中的顺序
                if (indexBufferX > indexBufferY) {
                    paraResultList.set(i, senY);
                    paraResultList.set(i + offset, senX);
                    paraValueList.set(i, valY);
                    paraValueList.set(i + offset, valX);
                }
            }
        }
        return new SentenceResult(paraResultList, paraValueList);
    }

    /**
     * 在候选摘要句列表中选取不超过最大长度的句子们，按句子顺序优先提取
     *
     * @param resultList 候选句子列表
     * @param valueList  候选句子权重列表
     * @param maxLength  最大提取长度
     * @return
     */
    private static SentenceResult pickSentences(List<String> resultList, List<Double> valueList, int maxLength) {
        // 已提取的摘要长度
        int curLength = 0;
        // 加上当前句子后的摘要长度
        int afterBuffer;

        // 挑选后的句子列表
        List<String> resultBuffer = new LinkedList<>();
        // 挑选后的句子权重列表
        List<Double> valueBuffer = new LinkedList<>();

        List<String> sentenceList = new LinkedList<>();
        List<Double> rankValueList = new LinkedList<>();
        sentenceList.addAll(resultList);
        rankValueList.addAll(valueList);

        while (curLength < maxLength) {
            double max = -Double.MAX_VALUE;
            int maxIndex = -1;
            /*
            选取权重最大的句子
             */
            for (int i = 0; i < sentenceList.size(); i++) {
                if (rankValueList.get(i) > max) {
                    max = rankValueList.get(i);
                    maxIndex = i;
                }
            }
            if (maxIndex == -1) {
//                System.out.println(resultList+":"+valueList);
                break;
            }
            afterBuffer = curLength + sentenceList.get(maxIndex).length();
            // 若加上当前句子长度还没有超出最大长度，则挑选当前句子
            if (afterBuffer <= maxLength) {
                resultBuffer.add(sentenceList.get(maxIndex));
                valueBuffer.add(rankValueList.get(maxIndex));
                curLength += sentenceList.get(maxIndex).length();
//                System.out.println(sentenceList.get(maxIndex)+":"+rankValueList.get(maxIndex));
            }
            sentenceList.remove(maxIndex);
            rankValueList.remove(maxIndex);
            if (sentenceList.size() < 1) {
                break;
            }
        }
        return new SentenceResult(resultBuffer, valueBuffer);
    }

    /**
     * TreeMap Comparator
     */
    class ValueComparator implements Comparator<Integer> {
        Map<Integer, Double> base;

        /**
         * 这里需要将要比较的map集合传进来
         *
         * @param base
         */
        ValueComparator(Map<Integer, Double> base) {
            this.base = base;
        }

        /**
         * Note: this comparator imposes orderings that are inconsistent with equals.
         * 比较的时候，传入的两个参数应该是map的两个key，根据上面传入的要比较的集合base，可以获取到key对应的value，然后按照value进行比较
         *
         * @param a
         * @param b
         * @return
         */
        @Override
        public int compare(Integer a, Integer b) {
            if (base.get(a) >= base.get(b)) {
                return -1;
            } else {
                return 1;
            } // returning 0 would merge keys
        }
    }

    /**
     * 存储句子的下标数组及权重值数组
     */
    class TopSentence {
        int[] index;
        double[] value;

        TopSentence(int[] index, double[] value) {
            this.index = index;
            this.value = value;
        }
    }

    /**
     * 存储摘要提取结果，句子列表及权重列表
     */
    static class SentenceResult {
        List<String> resultList;
        List<Double> valueList;

        SentenceResult(List<String> resultList, List<Double> valueList) {
            this.resultList = resultList;
            this.valueList = valueList;
        }
    }
}
