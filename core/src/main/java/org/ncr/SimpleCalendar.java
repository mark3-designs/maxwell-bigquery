package org.ncr;

import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.Date;
import java.util.Locale;

public class SimpleCalendar extends GregorianCalendar {

    public static final SimpleDateFormat MYSQL_DATE= new SimpleDateFormat("yyyy-MM-dd");
    public static final SimpleDateFormat MYSQL_DATETIME= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final SimpleDateFormat MYSQL_TIMESTAMP= new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
    public static final SimpleDateFormat MONTH_YEAR= new SimpleDateFormat("MMM yyyy");
    public static final SimpleDateFormat YEAR_MONTH= new SimpleDateFormat("yyyy-MM");
    public static final SimpleDateFormat SIMPLE= new SimpleDateFormat("yyyy-MM-dd HH:mm");
    public static final SimpleDateFormat SIMPLE_TZ= new SimpleDateFormat("yyyy-MM-dd HH:mm z");
    public static final SimpleDateFormat STD_DATE= new SimpleDateFormat("yyyy-MM-dd");
    public static final SimpleDateFormat DATE_HR_MIN= new SimpleDateFormat("yyyy-MM-dd HH:mm");
    public static final SimpleDateFormat STD_FMT= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final SimpleDateFormat ORCL_FMT= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
    public static final SimpleDateFormat ABBR_MO_FMT= new SimpleDateFormat("yyyy-MMM-dd HH:mm:ss");
    public static final SimpleDateFormat COMMON_FMT= new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    public static final SimpleDateFormat COMMON_DATE= new SimpleDateFormat("MM/dd/yyyy");
    public static final SimpleDateFormat MONTH_DAY_YEAR= new SimpleDateFormat("MMMM d, yyyy");
    public static final SimpleDateFormat TIMEKEY= new SimpleDateFormat("yyyyMMddHH");
    public static final SimpleDateFormat ISO8601= new SimpleDateFormat("yyyyMMdd'T'HH:mm:ss");

    public SimpleCalendar() {
    }

    public SimpleCalendar(TimeZone zone) {
        super(zone);
    }

    public SimpleCalendar(Locale aLocale) {
        super(aLocale);
    }

    public SimpleCalendar(TimeZone zone, Locale aLocale) {
        super(zone, aLocale);
    }

    public SimpleCalendar(int year) {
        super(year, JANUARY, 1);
    }

    public SimpleCalendar(int year, int month) {
        super(year, month, 1);
    }

    public SimpleCalendar(int year, int month, int dayOfMonth) {
        super(year, month, dayOfMonth);
    }

    public SimpleCalendar(int year, int month, int dayOfMonth, int hourOfDay, int minute) {
        super(year, month, dayOfMonth, hourOfDay, minute);
    }

    public SimpleCalendar(int year, int month, int dayOfMonth, int hourOfDay, int minute, int second) {
        super(year, month, dayOfMonth, hourOfDay, minute, second);
    }

    public SimpleCalendar(long millis) {
        setTimeInMillis(millis);
    }
    public SimpleCalendar(Date date) {
        setTime(date);
    }


    // simple helpers
    public int year() { return get(YEAR); }
    public int month() { return get(MONTH) + 1; }
    public int day() { return get(DAY_OF_MONTH); }
    public int hour() { return get(HOUR_OF_DAY); }
    public int minute() { return get(MINUTE); }
    public int second() { return get(SECOND); }
    public int millisecond() { return get(MILLISECOND); }
    public int dayOfYear() { return get(DAY_OF_YEAR); }
    public int dayOfWeek() { return get(DAY_OF_WEEK); }


    public boolean isWeekend() {
        return isSaturday() || isSunday();
    }

    public boolean isWeekday() {
        return !isSaturday() && !isSunday();
    }

    public boolean isSunday() { return dayOfWeek() == SUNDAY; }
    public boolean isMonday() { return dayOfWeek() == MONDAY; }
    public boolean isTuesday() { return dayOfWeek() == TUESDAY; }
    public boolean isWednesday() { return dayOfWeek() == WEDNESDAY; }
    public boolean isThursday() { return dayOfWeek() == THURSDAY; }
    public boolean isFriday() { return dayOfWeek() == FRIDAY; }
    public boolean isSaturday() { return dayOfWeek() == SATURDAY; }

    public boolean isFirstDayOfMonth() { return day() == 1; }
    public boolean isLastDayOfMonth() { return day() == getLastDayInMonth(); }

    public int getLastDayInMonth() { return getMaximum(DAY_OF_MONTH); }

    public SimpleCalendar advanceHour(int n) { add(HOUR_OF_DAY, n); return this; }
    public SimpleCalendar advanceMinute(int n) { add(MINUTE, n); return this; }
    public SimpleCalendar advanceSecond(int n) { add(SECOND, n); return this; }
    public SimpleCalendar advanceDay(int n) { add(DAY_OF_MONTH, n); return this; }
    public SimpleCalendar advanceMonth(int n) { add(MONTH, n); return this; }
    public SimpleCalendar advanceYear(int n) { add(YEAR, n); return this; }

    public SimpleCalendar setDay(int dayOfMonth) {
        if (dayOfMonth <= 0) { dayOfMonth= 1; }
        if (dayOfMonth > getLastDayInMonth()) { dayOfMonth= getLastDayInMonth(); }
        set(DAY_OF_MONTH, dayOfMonth);
        return this;
    }

    public SimpleCalendar setMonth(int month) {
        if (month <= 0) { month= 1; }
        if (month > 12) { month= 12; }
        set(MONTH, month - 1);
        return this;
    }

    public SimpleCalendar setYear(int year) {
        if (year <= 0) { year= 1; }
        if (year > 10000) { year= 10000; }
        set(YEAR, year);
        return this;
    }

    public boolean isToday() {
        SimpleCalendar today= new SimpleCalendar();

        if (today.year() == year()) {
            if (today.month() == month()) {
                if (today.day() == day()) {
                    return true;
                }
            }
        }

        return false;
    }

    public boolean isYesterday() {
        SimpleCalendar yesterday= new SimpleCalendar();
        yesterday.advanceDay(-1);

        if (yesterday.year() == year()) {
            if (yesterday.month() == month()) {
                if (yesterday.day() == day()) {
                    return true;
                }
            }
        }

        return false;
    }


    public boolean isTomorrow() {
        SimpleCalendar tomorrow= new SimpleCalendar();
        tomorrow.advanceDay(1);

        if (tomorrow.year() == year()) {
            if (tomorrow.month() == month()) {
                if (tomorrow.day() == day()) {
                    return true;
                }
            }
        }

        return false;
    }

    public SimpleCalendar zeroTime() {
        set(HOUR_OF_DAY, 0);
        set(MINUTE, 0);
        set(SECOND, 0);
        set(MILLISECOND, 0);
        return this;
    }

    public SimpleCalendar getPreviousMonth() {
        SimpleCalendar month= new SimpleCalendar(get(YEAR), get(MONTH), 1);
        month.zeroTime();
        month.advanceMonth(-1);
        return month;
    }

    public int getYear() {
        return get(YEAR);
    }

    public int getMonth() {
        return get(MONTH) + 1;
    }

    public SimpleCalendar getNextMonth() {
        SimpleCalendar month= new SimpleCalendar(get(YEAR), get(MONTH), 1);
        month.zeroTime();
        month.advanceMonth(1);
        return month;
    }

    public String toString() {
        return STD_FMT.format(this);
    }

    public SimpleCalendar getFirstDateOfMonth() {
        SimpleCalendar date= new SimpleCalendar(getYear(), getMonth(), 1);
        return date;
    }
    public SimpleCalendar getLastDateOfMonth() {
        SimpleCalendar date= new SimpleCalendar(getYear(), getMonth(), getLastDayInMonth());
        return date;
    }

}

