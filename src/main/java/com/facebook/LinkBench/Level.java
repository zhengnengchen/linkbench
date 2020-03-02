
package com.facebook.LinkBench;

public enum Level {
  TRACE, DEBUG, INFO, WARN, ERROR;

  public boolean isGreaterOrEqual(Level o) {
    int res = compareTo(o);
    // System.out.println("Compare self(" + this + ") with (" + o + ") returns " + (res >= 0));
    return res >= 0;
  }

  public static Level toLevel(String s) {
    if (s.equalsIgnoreCase("TRACE"))
      return TRACE;
    else if (s.equalsIgnoreCase("DEBUG"))
      return DEBUG;
    else if (s.equalsIgnoreCase("INFO"))
      return INFO;
    else if (s.equalsIgnoreCase("WARN"))
      return WARN;
    else if (s.equalsIgnoreCase("ERROR"))
      return ERROR;
    else
      return null;
  }
}

