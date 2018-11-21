package cn.com.cloudpioneer.textkeywords;


import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary;
import com.hankcs.hanlp.seg.common.Term;


/**
 * 按词性过滤关键词
 *
 * @author lsx
 */
public class KeywordFilter {
    /**
     * 是否应当将这个term纳入计算，词性属于名词、动词、副词、形容词
     *
     * @param term Term对象，包含词和词性
     * @return 是否应当
     */
    public boolean shouldInclude(Term term) {

        if (term.nature == null) {
            return false;
        }
        String nature = term.nature.toString();
        char firstChar = nature.charAt(0);
        switch (firstChar) {
            case 'd':
            case 'f':
            case 't':
            case 'm':
            case 'b':
            case 'c':
            case 'e':
            case 'o':
            case 'p':
            case 'q':
            case 'u':
            case 'y':
            case 'z':
            case 'r':
            case 'w': {
                return false;
            }
            default: {
//                if (nature.equalsIgnoreCase("ns"))
//                    return false;
                if (term.word.trim().length() > 1 && !CoreStopWordDictionary.contains(term.word)) {
                    return true;
                }
            }
            break;
        }

        return false;
    }
}
