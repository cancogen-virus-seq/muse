package org.cancogenvirusseq.muse.utils;

import java.util.regex.Pattern;
import lombok.experimental.UtilityClass;

import static java.lang.String.format;

@UtilityClass
public class StringUtils {
  private static final Pattern INTEGER_PATTERN = Pattern.compile("^-?\\d+$");
  private static final Pattern DOUBLE_PATTERN = Pattern.compile("^-?\\d+\\.\\d+$");

  public static Boolean isDouble(String s) {
    return DOUBLE_PATTERN.matcher(s).matches();
  }

  public static Boolean isInteger(String s) {
    return INTEGER_PATTERN.matcher(s).matches();
  }

  public static String stringToArrayOfStrings(String value, String delimiter){
    StringBuilder sb = new StringBuilder();
    for (String n : value.toString().split(delimiter)) {
      if (sb.length() > 0) sb.append(',');
      sb.append(format("\"%s\"", n.toString().replace("\"", "\\\"")));
    }
    return sb.toString();
  }
}
