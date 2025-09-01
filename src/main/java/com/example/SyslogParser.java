package com.example;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import lombok.ToString;

public class SyslogParser {

    public static enum Facility {
        unknown(null),
        kern(0),
        user(1),
        mail(2),
        daemon(3),
        auth(4),
        syslog(5),
        lpr(6),
        news(7),
        uucp(8),
        cron(9),
        authpriv(10),
        ftp(11),
        ntp(12),
        logAudit(13),
        logAlert(14),
        clock(15),
        local0(16),
        local1(17),
        local2(18),
        local3(19),
        local4(20),
        local5(21),
        local6(22),
        local7(23);

        private final Integer id;

        Facility(Integer id) {
            this.id = id;
        }

        public Integer getId() {
            return this.id;
        }

        public static Facility of(Integer priority) {
            Integer facility = priority / 8;
            for (Facility item: Facility.values()) {
                if (item.id == facility) {
                    return item;
                }
            }
            return unknown;
        }
    }

    public static enum Severity {
        unknown(null),
        emerg(0),
        alert(1),
        crit(2),
        err(3),
        warning(4),
        notice(5),
        info(6),
        debug(7);

        private final Integer id;

        Severity(Integer id) {
            this.id = id;
        }

        public Integer getId() {
            return this.id;
        }

        public static Severity of(Integer priority) {
            Integer severity = priority % 8;
            for (Severity item: Severity.values()) {
                if (item.id == severity) {
                    return item;
                }
            }
            return unknown;
        }
    };

    @ToString()
    public static class Rfc3164 {
        public String format;
        public Integer priority;
        public Facility facility;
        public Severity severity;
        public LocalDateTime date;
        public String host;
        public String message;
        public Rfc3164(Integer priority) {
            this.priority   = priority;
            this.facility   = Facility.of(this.priority);
            this.severity   = Severity.of(this.priority);
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
            Map.Entry<Integer, List<String>> priority = parsePriority(log);
            if (isNumber(priority.getValue().get(0))) {
                // RFC5424
                return parseRfc5424(priority);
            } else {
                // RFC3164
                return parseRfc3164(priority);
            }
        } catch (Exception e) {
            throw new SyslogParseException(e);
        }
    }

    // TODO: ちゃんとパースする
    public static Rfc3164 parseRfc3164(Map.Entry<Integer, List<String>> priority) {
        int i = 0;
        List<String> parts = priority.getValue();
        Rfc3164 data       = new Rfc3164(priority.getKey());
        String year        = "" + LocalDateTime.now().getYear();
        String month       = mmm.get(parts.get(i++).toLowerCase());
        String day         = parts.get(i++);
        String time        = parts.get(i++);
        String date        = String.format("%s %s %s %s", year, month, ("0" + day).substring(day.length() - 1), time);
        data.date          = LocalDateTime.parse(date, DateTimeFormatter.ofPattern("yyyy MM dd HH:mm:ss"));
        data.host          = parts.get(i++);
        while (i < parts.size()) {
            if (data.message == null) {
                data.message = parts.get(i);
            } else {
                data.message = data.message + " " + parts.get(i);
            }
            i++;
        }
        return data;
    }

    // TODO: ちゃんとパースする
    public static Rfc5424 parseRfc5424(Map.Entry<Integer, List<String>> priority) {
        int i = 0;
        List<String> parts = priority.getValue();
        Rfc5424 data       = new Rfc5424(priority.getKey());
        data.version       = Integer.valueOf(parts.get(i++));
        ZonedDateTime date = ZonedDateTime.parse(parts.get(i++));
        data.date          = date.toLocalDateTime();
        data.zone          = date.getZone();
        data.host          = parts.get(i++);
        data.app           = parts.get(i++);
        data.procid        = parts.get(i++);
        data.msgid         = parts.get(i++);
        data.structured    = new LinkedHashMap<>();
        if (!"-".equals(parts.get(i))) {
            while (i < parts.size()) {
                String part = parts.get(i);
                if (part.startsWith("[")) part = part.substring(1);
                if (part.endsWith("]")) part = part.substring(0, part.length() - 1);
                String[] kv = part.split("=");
                data.structured.put(kv[0], kv[1]);
                if (parts.get(i).endsWith("]")) break;
                i++;
            }
        }
        i++;
        while (i < parts.size()) {
            if (data.message == null) {
                data.message = parts.get(i);
            } else {
                data.message = data.message + " " + parts.get(i);
            }
            i++;
        }
        return data;
    }

    public static Map.Entry<Integer, List<String>> parsePriority(String log) {
        int start = log.indexOf("<");
        int end   = log.indexOf(">");
        String priority = log.substring(start + 1, end);
        return Map.entry(
            Integer.valueOf(priority),
            Arrays.asList(log.substring(end + 1).split(" ")).stream().filter(x -> !x.isEmpty()).toList()
        );
    }

    private static boolean isNumber(String value) {
        try {
            Integer.valueOf(value);
            return true;
        } catch(NumberFormatException e) {
            return false;
        }
    }

    public static void main(String[] args) throws SyslogParseException, FileNotFoundException, IOException {
        // TODO: RFCのとおりパースできないパターンがある
        for (String arg: args) {
            try (BufferedReader reader = new BufferedReader(new FileReader(arg))) {
                while (true) {
                    try {
                        String line = reader.readLine();
                        if (line == null || line.trim().isEmpty()) break;
                        System.out.println(String.join("", Collections.nCopies(80, "=")));
                        System.out.println(line);
                        System.out.println(String.join("", Collections.nCopies(80, "-")));
                        System.out.println(parse(line));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    System.out.println(String.join("", Collections.nCopies(80, "=")));
                    System.out.println();
                }
            }
        }
    }
}
