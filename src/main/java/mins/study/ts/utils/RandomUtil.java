package mins.study.ts.utils;

public class RandomUtil {

    public static int randomNumber(int min, int max) {
        return (int) Math.floor((Math.random() * (max - min)) + min);
    }
}
