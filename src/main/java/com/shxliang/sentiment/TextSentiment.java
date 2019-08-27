package com.shxliang.sentiment;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.common.Term;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author lsx
 * @date 2016/11/15
 */

public class TextSentiment {
    public static ArrayList<Double> getSentiment(String[] sentence,Map<String,Double> senWords,Map<String,Double> advWords,ArrayList<String> negWords,String type)
    {
        ArrayList<Double> sentimentScore = new ArrayList<Double>();
        ArrayList<String> words = new ArrayList<String>();

        for (int i=0;i<sentence.length;i++)
        {
            List<Term> segResult = HanLP.segment(sentence[i].trim());
            for (int j=0;j<segResult.size();j++)
            {
                Term curTerm = segResult.get(j);
                if (curTerm.nature!=null) {
                    words.add(curTerm.word);
                }
            }

            //最后一个已处理情感词下标
            int lastLocator = 0;
            double score = 0.0;
            for (int j=0;j<words.size();j++)
            {
                String curWord = words.get(j).trim();
                if (senWords.containsKey(curWord))
                {
                    double W = 1;
                    if ((j-1)<=lastLocator)
                    {
                        score = score+W*senWords.get(curWord);
                    }
                    else
                    {
                        int negIndex = -1;
                        int advIndex = -1;
                        for (int k=j-1;k>lastLocator;k--)
                        {
                            String beforeWords = words.get(k).trim();
                            if (negWords.contains(beforeWords))
                            {
                                W = W*(-1);
                                if (negIndex == -1) {
                                    negIndex = k;
                                }
                            }
                            if (advWords.containsKey(beforeWords))
                            {
                                W = W*advWords.get(beforeWords);
                                if (advIndex == -1) {
                                    advIndex = k;
                                }
                            }
                        }
                        if (negIndex!=-1 && advIndex!=-1)
                        {
                            if (negIndex<advIndex) {
                                W = W * 2;
                            } else {
                                W = W * 0.5;
                            }
                        }
//                        System.out.println(W*senWords.get(curWord));
                        score = score+W*senWords.get(curWord);
                    }
                    lastLocator = j;
                }
            }

            //调整标题情感值为2倍
            if (i == 0) {
                score = score * 2;
            }

//            System.out.println(segResult.toString()+":"+score);
//            System.out.println(sentence[i]+":"+score);
            sentimentScore.add(score);
            words.clear();
        }

        double positive = 0.0;
        double negative = 0.0;
        double sum = 0.0;

        for (int i=0;i<sentimentScore.size();i++)
        {
            double curScore = sentimentScore.get(i);
            if (curScore>=0) {
                positive += curScore;
            } else {
                negative += -curScore;
            }
        }

        ArrayList<Double> result = new ArrayList<Double>();

        if (positive==0 && negative==0)
        {
            if ("percent".equalsIgnoreCase(type))
            {
                result.add((double)0.5);
                result.add((double)0.5);
            }
            else {
                result.add((double) 0);
            }
        }
        else
        {
            if ("percent".equalsIgnoreCase(type))
            {
                sum = positive + negative;
                positive = positive/sum;
                negative = negative/sum;
                BigDecimal bp = new BigDecimal(positive);
                BigDecimal bn = new BigDecimal(negative);
                double positivePercent = bp.setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue();
                double negativePercent = bn.setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue();
                result.add(positivePercent);
                result.add(negativePercent);
            }
            else
            {
                if (positive>=negative) {
                    result.add((double) 1);
                } else {
                    result.add((double) -1);
                }
            }
        }
        return result;
    }
}
