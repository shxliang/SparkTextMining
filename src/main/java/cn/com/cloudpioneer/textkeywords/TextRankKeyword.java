package cn.com.cloudpioneer.textkeywords;

import com.hankcs.hanlp.algorithm.MaxHeap;
import com.hankcs.hanlp.corpus.tag.Nature;
import com.hankcs.hanlp.seg.common.Term;

import java.util.*;


/**
 * 使用TextRank算法提取文档关键词
 *
 * @author lsx
 */
public class TextRankKeyword extends KeywordFilter {

    /**
     * 窗宽
     */
    private int windowSize = 5;

    /**
     * 提取关键词个数
     */
    private int nKeyword = 10;

    /**
     * 阻尼系数
     */
    private double d = 0.85f;

    /**
     * 最大迭代次数
     */
    private int maxIter = 200;

    /**
     * 最小收敛误差
     */
    private double minDiff = 0.001f;

    /**
     * 提取关键词
     *
     * @param document 标题和正文的分词结果（含词性）拼接，形如"词1/词性1 词2/词性2"
     * @param size     提取关键词个数
     * @return 返回关键词和权重的列表，列表元素格式形如"关键词:权重"
     */
    private static List<String> getKeywordsList(String document, int size, int windowSize, double d, int maxIter, double minDiff) {
        TextRankKeyword textRankKeyword = new TextRankKeyword();
        textRankKeyword.nKeyword = size;
        textRankKeyword.windowSize = windowSize;
        textRankKeyword.d = d;
        textRankKeyword.maxIter = maxIter;
        textRankKeyword.minDiff = minDiff;

        return textRankKeyword.getKeywordsList(document);
    }

    /**
     * 提取关键词
     *
     * @param content 标题和正文的分词结果（含词性）拼接，形如"词1/词性1 词2/词性2"
     * @return 列表元素形如"词:TextRank值"
     */
    private List<String> getKeywordsList(String content) {
        Set<Map.Entry<String, Double>> entrySet = getWordAndRank(content, nKeyword).entrySet();

        /*
        将词和相应TextRank拼接为一个字符串存入列表返回
         */
        List<String> result = new ArrayList<String>(entrySet.size());
        for (Map.Entry<String, Double> entry : entrySet) {
            result.add(entry.getKey() + ":" + entry.getValue());
        }
        return result;
    }

    /**
     * 返回全部分词结果和对应的TextRank值
     *
     * @param content 标题和正文的分词结果（含词性）拼接，形如"词1/词性1 词2/词性2"
     * @return key为词，value为TextRank值
     */
    private Map<String, Double> getWordAndRank(String content) {
        assert content != null;

        /*
        将分词结果字符串转化为HanLP类型的分词结果列表
         */
        String[] parts = content.split(" ");
        List<Term> termList = new ArrayList<>();
        for (String s : parts) {
            // 切分词和词性
            String[] curParts = s.split("/");
            if (curParts.length > 1) {
                termList.add(new Term(curParts[0], Nature.fromString(curParts[1])));
            }
        }

        return calTextRank(termList);
    }

    /**
     * 返回分数最高的前size个分词结果和对应的rank
     *
     * @param content 标题和正文的分词结果（含词性）拼接，形如"词1/词性1 词2/词性2"
     * @param size    提取关键词个数
     * @return 将TextRank值降序筛选后的词和TextRank值
     */
    private Map<String, Double> getWordAndRank(String content, Integer size) {
        Map<String, Double> map = getWordAndRank(content);
        Map<String, Double> result = new LinkedHashMap<String, Double>();

        /*
        按map的value降序排序，并只保留前size个，存入result
         */
        for (Map.Entry<String, Double> entry : new MaxHeap<Map.Entry<String, Double>>(size, new Comparator<Map.Entry<String, Double>>() {
            @Override
            public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        }).addAll(map.entrySet()).toList()) {
            result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }

    /**
     * 迭代计算词的TextRank值
     *
     * @param termList 分词结果列表，类型HanLP分词的分词结果
     * @return key为词，value为TextRank值
     */
    private Map<String, Double> calTextRank(List<Term> termList) {
        List<String> wordList = new LinkedList<>();
        /*
        只保留指定词性的词用于TextRank的计算
         */
        for (Term t : termList) {
            if (shouldInclude(t)) {
                wordList.add(t.word);
            }
        }
//        System.out.println(wordList);

        // 存储和词(String)在给定窗宽下共现的那些词(Set<String>，词链出的那些词的集合)
        Map<String, Set<String>> words = new TreeMap<String, Set<String>>();
        // 存储给定窗宽内的词
        Queue<String> que = new LinkedList<String>();

        for (String w : wordList) {
            if (!words.containsKey(w)) {
                words.put(w, new TreeSet<String>());
            }
            // 插入当前到窗口
            que.offer(w);
            // 若窗口长度大于给定窗宽，则弹出窗口最前端的词
            if (que.size() > windowSize) {
                que.poll();
            }

            // 若窗口长度达到给定窗宽，则开始记录共现关系
            if (que.size() == windowSize) {
                /*
                遍历窗口中两两不相同的词
                 */
                for (String w1 : que) {
                    for (String w2 : que) {
                        if (w1.equals(w2)) {
                            continue;
                        }
                        words.get(w1).add(w2);
                        words.get(w2).add(w1);
                    }
                }
            }
        }
//        System.out.println(words);

        int totalWords = words.size();
        // 存储最终词的TextRank值
        Map<String, Double> score = new HashMap<String, Double>();
        for (int i = 0; i < maxIter; ++i) {
            // 当前迭代中词的TextRank值
            Map<String, Double> m = new HashMap<String, Double>();
            // 当前迭代中的最大误差
            double curDiff = 0;
            for (Map.Entry<String, Set<String>> entry : words.entrySet()) {
                String key = entry.getKey();
                // 词key链出的词的集合value
                Set<String> value = entry.getValue();
                m.put(key, 1 - d);
                for (String element : value) {
                    // 计算词element的出度
                    int size = words.get(element).size();
                    if (key.equals(element) || size == 0) {
                        continue;
                    }
                    m.put(key, m.get(key) + d / size * (score.get(element) == null ? (double) 1 / totalWords : score.get(element)));
                }
                curDiff = Math.max(curDiff, Math.abs(m.get(key) - (score.get(key) == null ? (double) 1 / totalWords : score.get(key))));
            }
            score = m;
            if (curDiff <= minDiff) {
                break;
            }
        }
        return score;
    }

    /**
     * 返回关键词以空格连接的字符串
     *
     * @param title      标题分词结果（含词性，不去停用词），形如"词1/词性1 词2/词性2"
     * @param content    正文分词结果（含词性，不去停用词），形如"词1/词性1 词2/词性2"
     * @param nKeywords  提取关键个数
     * @param windowSize 窗口词数
     * @param d          阻尼系数
     * @param maxIter    最大迭代次数
     * @param minDiff    最小误差
     * @param remove     过滤词列表
     * @return 关键词字符串
     */
    public static String getKeywords(String title, String content, int nKeywords, int windowSize, double d, int maxIter, double minDiff, List<String> remove) {
        // 拼接标题和正文的分词结果
        String words = title + " " + content;
        /*
        默认定义的候选关键词个数，若用户设置的提取个数大于候选个数，则候选个数就为提取个数
        候选个数一般大于提取个数，是由于需要在候选词集中进行关键词词组的合并
         */
        int candidateNum = 20;
        if (nKeywords > candidateNum) {
            candidateNum = nKeywords;
        }

        // 返回的关键词列表，列表元素形如"词:TextRank值"
        List<String> keywordsList = TextRankKeyword.getKeywordsList(words,
                candidateNum,
                windowSize,
                d,
                maxIter,
                minDiff);
//        System.out.println(keywordsList);

        // 标题词列表
        List<String> titleList = new LinkedList<>();
        // 正文词列表
        List<String> allWords = new LinkedList<>();
        String[] titleWords = title.split(" ");
        String[] contentWords = content.split(" ");
        for (String p : titleWords) {
            String[] cur = p.split("/");
            if (cur.length > 1) {
                titleList.add(cur[0]);
                allWords.add(cur[0]);
            }
        }
        for (String p : contentWords) {
            String[] cur = p.split("/");
            if (cur.length > 1) {
                allWords.add(cur[0]);
            }
        }

        // 返回的关键词
        List<String> key = new LinkedList<>();
        // 返回的关键词权重
        List<Double> value = new LinkedList<>();
        for (int i = 0; i < keywordsList.size(); i++) {
            // word:weight
            String[] parts = keywordsList.get(i).split(":");
            key.add(parts[0]);
            if (titleList.contains(parts[0])) {
                // 标题词的权重值调整为2倍
                value.add(Double.parseDouble(parts[1]) * 2);
            } else {
                value.add(Double.parseDouble(parts[1]));
            }
        }
//        System.out.println(value);

        // 已提取的关键词列表
        List<String> keywords = new LinkedList<>();
        // 已提取的关键词权重列表
        List<Double> keywordsValue = new LinkedList<>();
        // 已提取的关键词个数
        int num = 0;
        /*
        循环遍历关键词列表，每次从列表中选择TextRank值最大的词作为关键词
         */
        while (num < nKeywords && key.size() > 0) {
            double max = 0.0;
            int maxIndx = -1;
            /*
            从现有关键词列表中找出TextRank值最大的词
             */
            for (int i = 0; i < key.size(); i++) {
                if (value.get(i) > max) {
                    max = value.get(i);
                    maxIndx = i;
                }
            }
            if (maxIndx != -1) {
                // 是否将该词加入已提取的关键词列表
                boolean addTag = true;
                for (String w : keywords) {
                    /*
                    若将提取的关键词与已提取的关键词存在子串关系，则不提取
                    并且将TextRank值替换为其中最大的值
                     */
                    if (w.contains(key.get(maxIndx)) || key.get(maxIndx).contains(w)) {
                        addTag = false;
                        if (value.get(maxIndx) > keywordsValue.get(keywords.indexOf(w))) {
                            keywordsValue.set(keywords.indexOf(w), value.get(maxIndx));
                        }
                        key.remove(maxIndx);
                        value.remove(maxIndx);
                        break;
                    }
                }
                if (addTag) {
                    keywords.add(key.get(maxIndx));
                    keywordsValue.add(value.get(maxIndx));
                    key.remove(maxIndx);
                    value.remove(maxIndx);
                    num++;
                }
            }
        }


        // 所有标题和正文词个数
        int allWordsSize = allWords.size();

        /*
        顺序遍历标题和正文词，判断已提取的关键词是否在文档中相邻
        若相邻则合并为一个关键词，并重新计算该词权重
         */
        for (int i = 0; i < allWordsSize; i++) {
            String curWord = allWords.get(i);
            if (keywords.contains(curWord) && (i + 1) < allWordsSize) {
                String after = allWords.get(i + 1);
                if (keywords.contains(after) && !curWord.equalsIgnoreCase(after)) {
                    String combine = curWord + after;
                    int curWordIndex = keywords.indexOf(curWord);
                    int afterWordIndex = keywords.indexOf(after);

                    // 合并词后重新计算权重值
                    keywordsValue.add(Math.max(keywordsValue.get(curWordIndex), keywordsValue.get(afterWordIndex)));
//                    keywordsValue.add((keywordsValue.get(curWordIndex)+keywordsValue.get(afterWordIndex))/2);

                    keywordsValue.remove(Math.max(curWordIndex, afterWordIndex));
                    keywordsValue.remove(Math.min(curWordIndex, afterWordIndex));
                    keywords.remove(curWord);
                    keywords.remove(after);
                    keywords.add(combine);
                }
            }
        }

//        System.out.println(keywords);
//        System.out.println(keywordsValue);

        Map<String, Double> map = new HashMap<>();
        /*
        定义了一个广泛词过滤词表，过滤掉remove.txt中的词
         */
        for (int i = 0; i < keywords.size(); i++) {
            String s = keywords.get(i);
            if (!remove.contains(s) && !s.contains("某") && !s.contains("记者")) {
                map.put(s, keywordsValue.get(i));
            }
        }

        // 将map按value降序排序
        ValueComparator bvc = new ValueComparator(map);
        TreeMap<String, Double> resultWords = new TreeMap<String, Double>(bvc);
        resultWords.putAll(map);


//        ArrayList<String> combineList = new ArrayList<>();
//        for (int i=0;i<allWordsSize;)
//        {
//            String curWord = allWords.get(i);
//            int afterNum = 1;
//            if (keywords.contains(curWord))
//            {
//                boolean combineTag = false;
//                String combineIndx = i+" ";
//                while ((i+afterNum)<allWordsSize && keywords.contains(allWords.get(i+afterNum)))
//                {
//                    combineIndx = combineIndx+(i+afterNum)+" ";
//                    afterNum++;
//                    combineTag = true;
//                }
//                if (combineTag)
//                    combineList.add(combineIndx.trim());
//            }
//            i += afterNum;
//        }
//        ArrayList<String> resultWords = new ArrayList<>();
//        for (String indx : combineList)
//        {
//            String[] parts = indx.split(" ");
//            StringBuilder combined = new StringBuilder();
//            for (String w : parts)
//                combined.append(allWords.get(Integer.parseInt(w)));
//            resultWords.add(combined.toString());
//        }
//        resultWords.addAll(keywords);
//        keywords.clear();
//        for (String s : resultWords)
//            if (!remove.contains(s))
//                keywords.add(s);
//        resultWords.clear();
//        for (String s : keywords)
//            if (!resultWords.contains(s))
//                resultWords.add(s);


        /*
        将提取的关键词用空格拼接为字符串返回
         */
        StringBuilder result = new StringBuilder();
        for (String w : resultWords.keySet()) {
            result.append(w).append(" ");
        }

        return result.toString().trim();
    }

    private static class ValueComparator implements Comparator<String> {
        Map<String, Double> base;

        /**
         * 这里需要将要比较的map集合传进来
         *
         * @param base
         */
        ValueComparator(Map<String, Double> base) {
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
        public int compare(String a, String b) {
            if (base.get(a) >= base.get(b)) {
                return -1;
            } else {
                return 1;
            } // returning 0 would merge keys
        }
    }
}
