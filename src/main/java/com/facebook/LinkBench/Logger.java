
package com.facebook.LinkBench;

import java.sql.Timestamp;

public class Logger {
  private static Logger logger = new Logger(Level.INFO);
  public static Logger getLogger() { return logger; }

  private Level level;

  public Logger(Level l) {
    level = l;  
  }

  public Level getLevel() { return level; }
  public void setLevel(Level l) { level = l; }

  public void trace(StringBuilder s)       { logif(Level.TRACE, s); }
  public void trace(String s)              { logif(Level.TRACE, s); }

  public void debug(StringBuilder s)       { logif(Level.DEBUG, s); }
  public void debug(String s)              { logif(Level.DEBUG, s); }

  public void info(StringBuilder s)        { logif(Level.INFO, s); }
  public void info(String s)               { logif(Level.INFO, s); }

  public void warn(String s, Exception e)  { logif(Level.WARN, s, e); }
  public void warn(Exception e)            { logif(Level.WARN, e); }
  public void warn(StringBuilder s)        { logif(Level.WARN, s); }
  public void warn(String s)               { logif(Level.WARN, s); }

  public void error(String s, Exception e) { log(Level.ERROR, s + ": " + e.toString()); }
  public void error(Exception e)           { log(Level.ERROR, e.toString()); }
  public void error(StringBuilder s)       { log(Level.ERROR, s.toString()); }
  public void error(String s)              { log(Level.ERROR, s); }

  private void logif(Level l, String s, Exception e) {
    if (l.isGreaterOrEqual(level))
      log(l, s + ": " + e.toString());
  }

  private void logif(Level l, Exception e) {
    if (l.isGreaterOrEqual(level))
      log(l, e.toString());
  }

  private void logif(Level l, StringBuilder s) {
    if (l.isGreaterOrEqual(level))
      log(l, s.toString());
  }

  private void logif(Level l, String s) {
    if (l.isGreaterOrEqual(level))
      log(l, s);
  }

  private void log(Level l, String s) {
    Timestamp ts = new Timestamp(System.currentTimeMillis());
    System.out.println(l + " " + ts + " [" + Thread.currentThread().getName() + "]: " + s);
  }
}
