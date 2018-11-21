package cn.com.cloudpioneer.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author lsx
 */
public class StringUtil {
    private static Pattern chinesePattern = Pattern.compile("[\u4e00-\u9fa5]");

    public static boolean isContainChinese(String str) {
        Matcher m = chinesePattern.matcher(str);
        if (m.find()) {
            return true;
        }
        return false;
    }
}
