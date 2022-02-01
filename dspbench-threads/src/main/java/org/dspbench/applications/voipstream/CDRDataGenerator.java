package org.dspbench.applications.voipstream;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.dspbench.utils.IOUtils;
import org.dspbench.utils.JavaUtils;
import org.dspbench.utils.RandomUtil;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author maycon
 */
public class CDRDataGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(CDRDataGenerator.class);
    private static final Random rand = new Random();
            
    private static final String[] RBI = new String[] {"01","10","30","33","35","44","45","49","50","51","52","53","54","86","91","98","99"};
    
    /**
     * Codes taken from http://www.cisco.com/en/US/docs/voice_ip_comm/cucm/service/6_0_1/car/carcdrdef.html#wp1062375
     */
    private static final Map<Integer, String> TERMINATION_CAUSES = new HashMap<>();

    static {
        TERMINATION_CAUSES.put(0, "No error");
        TERMINATION_CAUSES.put(1, "Unallocated (unassigned) number");
        TERMINATION_CAUSES.put(2, "No route to specified transit network (national use)");
        TERMINATION_CAUSES.put(3, "No route to destination");
        TERMINATION_CAUSES.put(4, "Send special information tone");
        TERMINATION_CAUSES.put(5, "Misdialed trunk prefix (national use)");
        TERMINATION_CAUSES.put(6, "Channel unacceptable");
        TERMINATION_CAUSES.put(7, "Call awarded and being delivered in an established channel");
        TERMINATION_CAUSES.put(8, "Preemption");
        TERMINATION_CAUSES.put(9, "Preemption—circuit reserved for reuse");
        TERMINATION_CAUSES.put(16, "Normal call clearing");
        TERMINATION_CAUSES.put(17, "User busy");
        TERMINATION_CAUSES.put(18, "No user responding");
        TERMINATION_CAUSES.put(19, "No answer from user (user alerted)");
        TERMINATION_CAUSES.put(20, "Subscriber absent");
        TERMINATION_CAUSES.put(21, "Call rejected");
        TERMINATION_CAUSES.put(22, "Number changed");
        TERMINATION_CAUSES.put(26, "Non-selected user clearing");
        TERMINATION_CAUSES.put(27, "Destination out of order");
        TERMINATION_CAUSES.put(28, "Invalid number format (address incomplete)");
        TERMINATION_CAUSES.put(29, "Facility rejected");
        TERMINATION_CAUSES.put(30, "Response to STATUS ENQUIRY");
        TERMINATION_CAUSES.put(31, "Normal, unspecified");
        TERMINATION_CAUSES.put(34, "No circuit/channel available");
        TERMINATION_CAUSES.put(38, "Network out of order");
        TERMINATION_CAUSES.put(39, "Permanent frame mode connection out of service");
        TERMINATION_CAUSES.put(40, "Permanent frame mode connection operational");
        TERMINATION_CAUSES.put(41, "Temporary failure");
        TERMINATION_CAUSES.put(42, "Switching equipment congestion");
        TERMINATION_CAUSES.put(43, "Access information discarded");
        TERMINATION_CAUSES.put(44, "Requested circuit/channel not available");
        TERMINATION_CAUSES.put(46, "Precedence call blocked");
        TERMINATION_CAUSES.put(47, "Resource unavailable, unspecified");
        TERMINATION_CAUSES.put(49, "Quality of Service not available");
        TERMINATION_CAUSES.put(50, "Requested facility not subscribed");
        TERMINATION_CAUSES.put(53, "Service operation violated");
        TERMINATION_CAUSES.put(54, "Incoming calls barred");
        TERMINATION_CAUSES.put(55, "Incoming calls barred within Closed User Group (CUG)");
        TERMINATION_CAUSES.put(57, "Bearer capability not authorized");
        TERMINATION_CAUSES.put(58, "Meet-Me secure conference minimum security level not met");
        TERMINATION_CAUSES.put(62, "Inconsistency in designated outgoing access information and subscriber class");
        TERMINATION_CAUSES.put(63, "Service or option not available, unspecified");
        TERMINATION_CAUSES.put(65, "Bearer capability not implemented");
        TERMINATION_CAUSES.put(66, "Channel type not implemented");
        TERMINATION_CAUSES.put(69, "Requested facility not implemented");
        TERMINATION_CAUSES.put(70, "Only restricted digital information bearer capability is available (national use).");
        TERMINATION_CAUSES.put(79, "Service or option not implemented, unspecified");
        TERMINATION_CAUSES.put(81, "Invalid call reference value");
        TERMINATION_CAUSES.put(82, "Identified channel does not exist.");
        TERMINATION_CAUSES.put(83, "A suspended call exists, but this call identity does not.");
        TERMINATION_CAUSES.put(84, "Call identity in use");
        TERMINATION_CAUSES.put(85, "No call suspended");
        TERMINATION_CAUSES.put(86, "Call having the requested call identity has been cleared.");
        TERMINATION_CAUSES.put(87, "User not member of CUG (Closed User Group)");
        TERMINATION_CAUSES.put(88, "Incompatible destination");
        TERMINATION_CAUSES.put(90, "Destination number missing and DC not subscribed");
        TERMINATION_CAUSES.put(91, "Invalid transit network selection (national use)");
        TERMINATION_CAUSES.put(95, "Invalid message, unspecified");
        TERMINATION_CAUSES.put(96, "Mandatory information element is missing.");
        TERMINATION_CAUSES.put(97, "Message type nonexistent or not implemented");
        TERMINATION_CAUSES.put(98, "Message not compatible with the call state, or the message type nonexistent or not implemented");
        TERMINATION_CAUSES.put(99, "An information element or parameter non-existent or not implemented");
        TERMINATION_CAUSES.put(100, "Invalid information element contents");
        TERMINATION_CAUSES.put(101, "Message not compatible with the call state");
        TERMINATION_CAUSES.put(102, "Call terminated when timer expired; a recovery routine executed to recover from the error.");
        TERMINATION_CAUSES.put(103, "Parameter nonexistent or not implemented - passed on (national use)");
        TERMINATION_CAUSES.put(110, "Message with unrecognized parameter discarded");
        TERMINATION_CAUSES.put(111, "Protocol error, unspecified");
        TERMINATION_CAUSES.put(122, "Precedence Level Exceeded");
        TERMINATION_CAUSES.put(123, "Device not Preemptable");
        TERMINATION_CAUSES.put(125, "Out of bandwidth (Cisco specific)");
        TERMINATION_CAUSES.put(126, "Call split (Cisco specific)");
        TERMINATION_CAUSES.put(127, "Interworking, unspecified");
        TERMINATION_CAUSES.put(129, "Precedence out of bandwidth");
    }
    
    public static final Object[] TERMINATION_CAUSE_CODES = TERMINATION_CAUSES.keySet().toArray();
    public static final int TERMINATION_CAUSE_OK = 0;
    
    /**
     * http://en.wikipedia.org/wiki/Um_interface
     */
    public static final String[] CALL_TYPES = new String[] {"MOC", "MTC", "SMS-MT", "SMS-MO"};

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static List<Map<String, String>> countryList;
    private static Map<String, String> countryMap;
    
    static {
        try {
            String strJson;
            
            if (JavaUtils.isJar()) {
                InputStream is = CDRDataGenerator.class.getResourceAsStream("/CountryCodes.json");
                strJson = IOUtils.convertStreamToString(is);
            } else {
                strJson = Files.readString(Paths.get("src/main/resources/CountryCodes.json"), Charset.defaultCharset());
            }

            countryList = objectMapper.readValue(strJson, new TypeReference<List<Map<String, String>>>(){});
            countryMap = new HashMap<String, String>(countryList.size());
            
            for (Map<String, String> obj : countryList) {
                countryMap.put(obj.get("code"), obj.get("dial_code"));
            }
        } catch (IOException ex) {
            LOG.error("Error reading country codes file", ex);
        }
    }
    
    public static String phoneNumber() {
        return phoneNumber("", -1);
    }
    
    
    public static String phoneNumber(String countryName, int numDigits) {
        numDigits = (numDigits == -1) ? 11 : numDigits;
        String number = "";
        String dialCode;
        
        if (countryName.isEmpty()) {
            Map<String, String> country = countryList.get(RandomUtil.randInt(0, countryList.size()-1));
            dialCode = country.get("dial_code");
        } else {
            dialCode = countryMap.get(countryName);
        }
        
        number += dialCode;
        numDigits -= dialCode.length();
        
        for (int i=0; i<numDigits; i++) {
            number += RandomUtil.randInt(0, 9);
        }
                
        return number;
    }
    
    /**
     * Random IMEI (International Mobile Station Equipment Identity) Number Generator.
     * http://en.wikipedia.org/wiki/IMEI
     * https://github.com/LazyZhu/myblog/blob/gh-pages/imei-generator/js/imei-generator.js
     * @author LazyZhu (http://lazyzhu.com/)
     * @return imei
     */
    public static String imei() {
        int len = 15;
        int len_offset = 0;
        int pos = 0;
        int t = 0;
        int sum = 0;
        int final_digit = 0;
        int[] str = new int[len];

        //
        // Fill in the first two values of the string based with the specified prefix.
        // Reporting Body Identifier list: http://en.wikipedia.org/wiki/Reporting_Body_Identifier
        //
        String[] arr = RBI[(int) Math.floor(Math.random() * RBI.length)].split("");
        str[0] = Integer.parseInt(arr[1]);
        str[1] = Integer.parseInt(arr[2]);
        pos = 2;
        
        //
        // Fill all the remaining numbers except for the last one with random values.
        //
        while (pos < len - 1) {
            str[pos++] = (int) Math.floor(Math.random() * 10) % 10;
        }
        
        //
        // Calculate the Luhn checksum of the values thus far.
        //
        len_offset = (len + 1) % 2;
        for (pos = 0; pos < len - 1; pos++) {
            if ((pos + len_offset) % 2 != 0) {
                t = str[pos] * 2;
                if (t > 9) {
                    t -= 9;
                }
                sum += t;
            }
            else {
                sum += str[pos];
            }
        }
        
        //
        // Choose the last digit so that it causes the entire string to pass the checksum.
        //
        final_digit = (10 - (sum % 10)) % 10;
        str[len - 1] = final_digit;
        
        String imei = "";
        for (pos = 0; pos < len; pos++)
            imei += str[pos];

        // Output the IMEI value.
        return imei;//.substring(0, len);
    }
    
    /**
     * Random IMSI (International mobile subscriber identity) generator.
     * @return 
     */
    public static String imsi() {
        String imsi = "";
        
        for (int i = 0; i < 15; i++) {
            imsi += RandomUtil.randInt(0, 9);
        }
        
        return imsi;
    }
    
    public static String callType() {
        return CALL_TYPES[RandomUtil.randInt(0, CALL_TYPES.length-1)];
    }
    
    /**
     * Return the code for a random termination cause.
     * @param errorProb The probability of occurrence of an error (all errors have equal chances)
     * @return the code of the termination cause
     */
    public static int causeForTermination(double errorProb) {
        if (rand.nextDouble() < errorProb)
            return (Integer) TERMINATION_CAUSE_CODES[RandomUtil.randInt(1, TERMINATION_CAUSE_CODES.length-1)];
        else
            return TERMINATION_CAUSE_OK;
    }
    
    /**
     * Return the information about the cause for termination with the given code.
     * @param code The code of the cause for termination
     * @return The information about the cause for termination code
     */
    public static String causeForTerminationInfo(int code) {
        return TERMINATION_CAUSES.get(code);
    }
    
    public static String uniqueId() {
        UUID id = UUID.randomUUID();
        return id.toString();
    }
}