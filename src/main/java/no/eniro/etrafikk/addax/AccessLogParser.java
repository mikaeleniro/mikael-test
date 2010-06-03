package no.eniro.etrafikk.addax;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AccessLogParser {

    private static String regEx = "^(\\S+) (\\S+) (\\S+) \\[([^:]+):(\\d+:\\d+:\\d+) ([^\\]]+)\\] \"(\\S+) .+?sik.gif\\?(.+?) (\\S+)\" (\\S+) (\\S+)(?: (\".+?\"))?(?: (\".+?\"))?$";
    private static String datePattern = "d/MMM/yyyy HH:mm:ssZ";
    private static String datePatternOut = "yyyy/M/d HH:mm:ss";

    private static int CLIENT_GROUP = 1;
    private static int DATE_GROUP = 4;
    private static int TIME_GROUP = 5;
    private static int TIME_ZONE_GROUP = 6;
    private static int REQUEST_GROUP = 8;
    private static int REFERRER = 12;
    private static int AGENT = 13;

    private static String[] fields = new String[] {"advert_code", "customer_id", "exposure_type", "exposure_id",
            "heading_code", "rank", "order", "area_code", "stq", "hpp", "page_type", "vhost", "country", "search_word",
            "geo_area", "company_name", "phone_number", "street", "zipcode", "area", "dir_area", "geo_code", "generic",
            "hits", "ref", "random", "test"};

    private static int NUM_FIELDS = fields.length;
    private static StringBuilder header = new StringBuilder();

    private static Map<String, Integer> fieldIndex = new HashMap<String, Integer>();

    static {
        for (int i = 0; i < fields.length; i++) {
            fieldIndex.put(fields[i], i);
        }
    }

    public static void main(String argv[]) throws Exception {

        String inputFileName;
        String outputFileName;
        String defaultInputFileName = "/Data/localhost_access_log.2010-05-20.txt";
        String defaultOutputFileName = "/Data/temp.txt";

        if (argv.length == 1) {
            inputFileName = argv[0];
            outputFileName = argv[0] + ".output";
        } else if (argv.length == 2) {
            inputFileName = argv[0];
            outputFileName = argv[1];
        } else {
            inputFileName = defaultInputFileName;
            outputFileName = defaultOutputFileName;
        }

        // Create a pattern to match comments
        Pattern p = Pattern.compile(regEx, Pattern.MULTILINE);

        // Get a Channel for the source file
        File f = new File(inputFileName);
        FileInputStream fis = new FileInputStream(f);
        FileChannel fc = fis.getChannel();

        // Get a CharBuffer from the source file
        ByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, (int) fc.size());
        Charset cs = Charset.forName("8859_1");
        CharsetDecoder cd = cs.newDecoder();
        CharBuffer cb = cd.decode(bb);

        // Write CSV header to file
        header.append("date;");
        for (String s : fields) {
            header.append(s).append(";");
        }
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputFileName));
        bufferedWriter.write(header.toString());
        bufferedWriter.newLine();

        // Run matches
        DateFormat format = new SimpleDateFormat(datePattern, Locale.US);
        DateFormat formatOut = new SimpleDateFormat(datePatternOut);
        Matcher m = p.matcher(cb);
        
        // Write entries to file
        long startTime = System.currentTimeMillis();
        int count = 0;
        while (m.find()) {

            String request = m.group(REQUEST_GROUP);
            String[] line = new String[NUM_FIELDS];
            int keyStart = 0;
            count++;

            while (true) {
                int keyEnd = request.indexOf('=', keyStart);
                if (keyEnd < 0) {
                    break;
                }
                int valStart = keyEnd + 1;
                int valEnd = request.indexOf('&', valStart);
                if (valEnd < 0) {
                    valEnd = request.length();
                }
                String key = request.substring(keyStart, keyEnd);
                String value = request.substring(valStart, valEnd);
                keyStart = valEnd + 1;

                Integer index = fieldIndex.get(key);
                if (index == null) {
                    // unknown field
                    System.out.println("Unknown field: " + key);
                    continue;
                }
                line[index] = value;
            }
            StringBuilder sb = new StringBuilder();
            Date date = format.parse(m.group(DATE_GROUP) + " " + m.group(TIME_GROUP) + m.group(TIME_ZONE_GROUP));
            sb.append(formatOut.format(date));
            sb.append(";");
            for (int i = 0; i < NUM_FIELDS; i++) {
                if (line[i] != null) {
                    sb.append(line[i].replace(";", "%3B"));
                }
                sb.append(";");
            }
            bufferedWriter.write(sb.toString());
            bufferedWriter.newLine();
        }
        bufferedWriter.close();
        System.out.println("Number of lines: " + count);
        System.out.println("Seconds spent: " + (System.currentTimeMillis() - startTime) / 1000);
    }
}