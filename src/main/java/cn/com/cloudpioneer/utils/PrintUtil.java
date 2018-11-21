package cn.com.cloudpioneer.utils;

/**
 * @author lsx
 */
public class PrintUtil {
    public static void printMatrix(double[][] matrix)
    {
        int nrow = matrix.length;
        int ncol = matrix[0].length;
        for (int r=0; r<nrow; r++)
        {
            for (int c = 0; c < ncol; c++)
            {
                System.out.print(matrix[r][c]);
                System.out.print("\t");
            }
            System.out.print("\n");
        }
    }
}
