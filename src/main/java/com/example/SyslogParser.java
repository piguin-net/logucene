package com.example;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

import lombok.ToString;

public class SyslogParser {

    @ToString()
    public static class Rfc3164 {
        public String format;
        public Integer priority;
        public Integer facility;
        public Integer severity;
        public LocalDateTime date;
        public String host;
        public String message;
        public Rfc3164(Integer priority) {
            this.priority   = priority;
            this.facility   = this.priority / 8;
            this.severity   = this.priority % 8;
            this.format = "rfc3164";
        }
    }

    @ToString(callSuper=true)
    public static class Rfc5424 extends Rfc3164 {
        public Integer version;
        public ZoneId zone;
        public String  app;
        public String  procid;
        public String  msgid;
        public Map<String, String> structured;
        public Rfc5424(Integer priority) {
            super(priority);
            this.format = "rfc5424";
        }
    }

    public static class SyslogParseException extends Exception {
        public SyslogParseException(Exception e) {
            super(e);
        }
    }

    private static Map<String, String> mmm = Map.ofEntries(
        Map.entry("jan", "01"),
        Map.entry("feb", "02"),
        Map.entry("mar", "03"),
        Map.entry("apr", "04"),
        Map.entry("may", "05"),
        Map.entry("jun", "06"),
        Map.entry("jul", "07"),
        Map.entry("aug", "08"),
        Map.entry("sep", "09"),
        Map.entry("oct", "10"),
        Map.entry("nov", "11"),
        Map.entry("dec", "12")
    );

    public static Rfc3164 parse(String log) throws SyslogParseException {
        try {
            int i = 0;
            Map.Entry<Integer, String> priority = parsePriority(log);
            String[] parts = priority.getValue().split(" ");
            if (isNumber(parts[i])) {
                // RFC5424
                Rfc5424 data    = new Rfc5424(priority.getKey());
                data.version    = Integer.valueOf(parts[i++]);
                ZonedDateTime date = ZonedDateTime.parse(parts[i++]);
                data.date       = date.toLocalDateTime();
                data.zone       = date.getZone();
                data.host       = parts[i++];
                data.app        = parts[i++];
                data.procid     = parts[i++];
                data.msgid      = parts[i++];
                data.structured = new LinkedHashMap<>();
                if (!"-".equals(parts[i])) {
                    while (i < parts.length) {
                        String part = parts[i];
                        if (part.startsWith("[")) part = part.substring(1);
                        if (part.endsWith("]")) part = part.substring(0, part.length() - 1);
                        String[] kv = part.split("=");
                        data.structured.put(kv[0], kv[1]);
                        if (parts[i].endsWith("]")) break;
                        i++;
                    }
                }
                i++;
                while (i < parts.length) {
                    if (data.message == null) {
                        data.message = parts[i];
                    } else {
                        data.message = data.message + " " + parts[i];
                    }
                    i++;
                }
                return data;
            } else {
                // RFC3164
                Rfc3164 data = new Rfc3164(priority.getKey());
                String year  = "" + LocalDateTime.now().getYear();
                String month = mmm.get(parts[i++].toLowerCase());
                String day   = parts[i++];
                String time  = parts[i++];
                String date  = String.format("%s %s %s %s", year, month, day, time);
                data.date    = LocalDateTime.parse(date, DateTimeFormatter.ofPattern("yyyy MM dd HH:mm:ss"));
                data.host    = parts[i++];
                while (i < parts.length) {
                    if (data.message == null) {
                        data.message = parts[i];
                    } else {
                        data.message = data.message + " " + parts[i];
                    }
                    i++;
                }
                return data;
            }
        } catch (Exception e) {
            throw new SyslogParseException(e);
        }
    }

    public static Map.Entry<Integer, String> parsePriority(String log) {
        int start = log.indexOf("<");
        int end   = log.indexOf(">");
        String priority = log.substring(start + 1, end);
        return Map.entry(Integer.valueOf(priority), log.substring(end + 1));
    }

    private static boolean isNumber(String value) {
        try {
            Integer.valueOf(value);
            return true;
        } catch(NumberFormatException e) {
            return false;
        }
    }

    public static void main(String[] args) throws SyslogParseException {
        for (String arg: args) {
            Rfc3164 log = parse(arg);
            System.out.println(log);
        }
    }
}
