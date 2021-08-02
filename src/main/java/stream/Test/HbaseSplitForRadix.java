package stream.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HbaseSplitForRadix {
    public static void main(String[] args) {
        System.out.println(Long.valueOf("99999999", 16));
        int region = 8;
        int radix = 10;
        String start = "00";
        String end = "99";

        System.out.println(getSplitForRadix(region, radix, start, end));
        // [12, 24, 36, 48, 60, 72, 84, 96]
    }
    public static List<String> getSplitForRadix(int region, int radix, String start, String end) {
        Integer s = Integer.parseInt(start);
        Integer e = Long.valueOf(end, radix).intValue() + 1;
        return IntStream
                .range(s, e)
                .filter(value -> (value % ((e - s) / region)) == 0)
                .mapToObj(value -> {
                    if (radix == 16) {
                        return Integer.toHexString(value);
                    } else {
                        return String.valueOf(value);
                    }
                })
                .skip(1)
                .collect(Collectors.toList());
    }
}
