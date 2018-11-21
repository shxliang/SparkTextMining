package cn.com.cloudpioneer.mrp;

import java.util.ArrayList;

/**
 *
 * @author lsx
 * @date 2016/10/25
 */

public class RegionRecognition {
    public static String getRegion(String titlePlace,String contentPlace,Integer placeNum)
    {
        String s = null;
        if (titlePlace!=null && contentPlace!=null) {
            s = titlePlace.trim() + " " + titlePlace.trim() + " " + titlePlace.trim() + " " + contentPlace.trim();
        }
        if (titlePlace==null && contentPlace!=null) {
            s = contentPlace.trim();
        }
        if (titlePlace!=null && contentPlace==null) {
            s = titlePlace.trim();
        }
        if (titlePlace==null && contentPlace==null) {
            return null;
        }

        String[] parts = s.split(" ");
        ArrayList<String> place = new ArrayList<String>();
        ArrayList<Integer> count = new ArrayList<Integer>();
        for (int i=0;i<parts.length;i++)
        {
            String curPlace = parts[i];
            if (!place.contains(curPlace))
            {
                place.add(curPlace);
                count.add(1);
            }
            else
            {
                int indx = place.indexOf(curPlace);
                count.set(indx,count.get(indx)+1);
            }
        }
        if (place.size()>1)
        {
            StringBuilder result = new StringBuilder();
            ArrayList<Integer> resultIndx = new ArrayList<Integer>();
            int returnNum = placeNum;
            if (returnNum > place.size()) {
                returnNum = place.size();
            }
            while (returnNum>0)
            {
                int max = 0;
                int maxIndx = -1;
                for (int i=0;i<place.size();i++)
                {
                    if (resultIndx.contains(i)) {
                        continue;
                    }
                    if (count.get(i) > max)
                    {
                        max = count.get(i);
                        maxIndx = i;
                    }
                }
                resultIndx.add(maxIndx);
                result.append(place.get(maxIndx)+" ");
                returnNum--;
            }
            return result.toString().trim();
        }
        else
        {
            return place.get(0).trim();
        }
    }
}
