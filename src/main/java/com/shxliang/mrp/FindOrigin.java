package com.shxliang.mrp;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 *
 * @author lsx
 * @date 2017/5/2
 */
public class FindOrigin {
    private static Pattern pattern = Pattern.compile("^.*?[:：][ ]*");
    public static String getOrigin(String site,String source)
    {
        String sourceStr = pattern.matcher(source).replaceAll("");
        List<String> sourceList = Arrays.asList(sourceStr.trim().split("[-－]"));
        String siteStr = site;
        if (site!=null)
        {
            if (sourceList.contains(site)) {
                return "1";
            }
        }
        return "0";
    }
}
