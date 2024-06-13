package xyz.kafka.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.lang.Pair;
import cn.hutool.core.text.StrPool;
import com.alibaba.fastjson2.JSON;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * String 操作工具类
 *
 * @author chaoxin.lu
 * @version V1.0
 * @since 7:37 2016-06-25
 */
public class StringUtil {

    public static final int IP_MIN_LEN = 7;

    public static final int IP_MAX_LEN = 15;

    public static final String N_A = "N/A";

    public static final String TRUE = "true";

    public static final String FALSE = "false";

    public static final String EMPTY = "";

    public static final String IS_PREFIX = "is";

    private static final AtomicBoolean initCnTraditionalSimplified = new AtomicBoolean(false);
    private static Map<Character, Character> simplifiedMap;
    private static Map<Character, Character> traditionalMap;

    /**
     * 判断对象是否为null
     *
     * @param obj
     * @return
     */
    public static boolean isNull(Object obj) {
        return obj == null || "null".equals(obj);
    }

    /**
     * 把一个Object转换成String如果为null则返回空字符串
     *
     * @param obj 要转换成String的对象
     * @return 返回一个字符串
     */
    public static String toString(Object obj) {
        if (Objects.isNull(obj)) {
            return "";
        }

        if (obj instanceof byte[] b) {
            return new String(b, StandardCharsets.UTF_8);
        }

        if (obj instanceof String s) {
            return s.strip();
        }
        return trim(obj.toString());
    }

    /**
     * 把一个Object转换成String如果为null则返回给定默认值defaultValue
     *
     * @param obj          要转换成String的对象
     * @param defaultValue 给定默认值
     * @return 返回一个字符串
     */
    public static String toString(Object obj, String defaultValue) {
        if (isNull(obj)) {
            return defaultValue;
        }
        return toString(obj);
    }

    public static <T> String toString(Iterable<T> iterable, Function<T, String> value, CharSequence delimiter) {
        if (iterable == null) {
            return null;
        }
        Iterator<T> it = iterable.iterator();
        StringBuilder sb = new StringBuilder();
        while (it.hasNext()) {
            sb.append(value.apply(it.next())).append(delimiter);
        }
        if (!sb.isEmpty()) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    /**
     * 把一个Object对象转换成Integer,如果Object不是int类型则返回-1
     *
     * @param obj 要转换成Integer的对象
     * @return 返回Integer对象
     */
    public static Integer toInteger(Object obj) {
        return toInteger(obj, -1);
    }

    /**
     * 把一个Object对象转换成Integer,如果Object不是int类型则返回给定默认值defaultValue
     *
     * @param obj          要转换成Integer的对象
     * @param defaultValue 给定默认值
     * @return 返回Integer对象
     */
    public static Integer toInteger(Object obj, Integer defaultValue) {
        if (obj == null) {
            return defaultValue;
        }
        if (obj instanceof Integer i) {
            return i;
        }
        String value = toString(obj, EMPTY);
        if (isInteger(value)) {
            return Integer.parseInt(value);
        }
        return defaultValue;
    }

    /**
     * 把一个Object对象转换成Float,如果Object不是int类型则返回0
     *
     * @param obj 要转换成Float的对象
     * @return 返回Float对象
     */
    public static Float toFloat(Object obj) {
        return toFloat(obj, 0F);
    }

    /**
     * 把一个Object对象转换成Float,如果Object不是int类型则返回给定默认值defaultValue
     *
     * @param obj          要转换成Float的对象
     * @param defaultValue 给定默认值
     * @return 返回Float对象
     */
    public static Float toFloat(Object obj, Float defaultValue) {
        if (obj == null) {
            return defaultValue;
        }
        String value = toString(obj, EMPTY);
        if (isFloat(value)) {
            return Float.parseFloat(value);
        }
        return defaultValue;
    }

    /**
     * 把一个Object对象转换成Long,如果Object不是int类型则返回给定默认值defaultValue
     *
     * @param obj          要转换成Long的对象
     * @param defaultValue 给定默认值
     * @return 返回Long对象
     */
    public static Long toLong(Object obj, Long defaultValue) {
        if (obj == null) {
            return defaultValue;
        }
        String value = toString(obj, EMPTY);
        if (isLong(value)) {
            return Long.parseLong(value);
        }
        return defaultValue;
    }

    /**
     * 把一个Object对象转换成Double,如果Object不是int类型则返回0
     *
     * @param obj 要转换成Double的对象
     * @return 返回Double对象
     */
    public static Double toDouble(Object obj) {
        return toDouble(obj, 0D);
    }

    /**
     * 把一个Object对象转换成Double,如果Object不是int类型则返回给定默认值defaultValue
     *
     * @param obj          要转换成Double的对象
     * @param defaultValue 给定默认值
     * @return 返回Double对象
     */
    public static Double toDouble(Object obj, Double defaultValue) {
        if (obj == null) {
            return defaultValue;
        }
        String value = toString(obj, EMPTY);
        if (isFloat(value)) {
            return Double.parseDouble(value);
        }
        return defaultValue;
    }

    /**
     * 把一个Object对象转换成Short,如果Object不是int类型则返回给定默认值defaultValue
     *
     * @param obj          要转换成Short的对象
     * @param defaultValue 给定默认值
     * @return 返回Double对象
     */
    public static Short toShort(Object obj, Short defaultValue) {
        if (obj == null) {
            return defaultValue;
        }
        String value = toString(obj, EMPTY);
        if (isInteger(value)) {
            return Short.parseShort(value);
        }
        return defaultValue;
    }

    /**
     * 把一个Object对象转换成Short,如果Object不是int类型则返回给定默认值defaultValue
     *
     * @param obj          要转换成Short的对象
     * @param defaultValue 给定默认值
     * @return 返回Double对象
     */
    public static Byte toByte(Object obj, Byte defaultValue) {
        if (obj == null) {
            return defaultValue;
        }
        String value = toString(obj, EMPTY);
        if (isInteger(value)) {
            return Byte.parseByte(value);
        }
        return defaultValue;
    }

    public static byte[] getByte(String resource, String charset) throws UnsupportedEncodingException {
        return StringUtil.isEmpty(resource) ? new byte[1] : resource.getBytes(charset);
    }

    public static byte[] getByte(String resource) throws UnsupportedEncodingException {
        return getByte(resource, StandardCharsets.UTF_8.name());
    }

    /**
     * 把一个Object对象转换成Boolean,如果Object不是false,或者true类型则返回给定默认值defaultValue
     *
     * @param obj          要转换成Boolean的对象
     * @param defaultValue 给定默认值
     * @return 返回Boolean对象
     */
    public static Boolean toBoolean(Object obj, Boolean defaultValue) {
        if (obj == null) {
            return defaultValue;
        }
        String str = toString(obj, EMPTY);
        if (TRUE.equalsIgnoreCase(str) || FALSE.equalsIgnoreCase(str)) {
            return Boolean.parseBoolean(str);
        }

        return defaultValue;
    }

    /**
     * 获取32为UUID
     */
    public static String getUuid() {
        return UUID.randomUUID().toString().replace(StrPool.DASHED, EMPTY);
    }

    /**
     * 获取16为int数字，可以用作订单号
     */
    public static String getOrderId() {
        // 最大支持1-9个集群机器部署
        int machineId = 1;
        int hashCodeV = UUID.randomUUID().toString().hashCode();
        // 有可能是负数
        if (hashCodeV < 0) {
            hashCodeV = -hashCodeV;
        }
        // 0 代表前面补充0
        // 4 代表长度为4
        // d 代表参数为正数型
        return machineId + String.format("%015d", hashCodeV);
    }

    /**
     * 去除字符串首尾的空字符，如果字符串为空则返回指定的默认值forNull
     *
     * @param str     需要去除首尾的空字符的字符串
     * @param forNull 默认值
     * @return 返回String
     */
    public static String trim(String str, String forNull) {
        return null == str ? forNull : str.strip();
    }

    /**
     * 去除字符串首尾的空字符，如果字符串为空则返回空
     *
     * @param str 需要去除首尾的空字符的字符串
     * @return 返回String
     */
    public static String trim(String str) {
        return trim(str, EMPTY);
    }

    /**
     * 求字符串长度如果字符串为null则返回0
     *
     * @param str
     * @return
     */
    public static int length(String str) {
        return isEmpty(str) ? 0 : str.length();
    }

    /**
     * 判断字符串是否为空
     *
     * @param input 需要判断的字符串
     * @return 返回字符串是否为空
     */
    public static boolean isEmpty(CharSequence input) {
        if (input != null) {
            if ("null".contentEquals(input)) {
                return true;
            }
            for (int i = 0; i < input.length(); i++) {
                char c = input.charAt(i);
                if (!Character.isWhitespace(c)) {
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean isAllEmpty(CharSequence... inputs) {
        if (inputs != null) {
            for (CharSequence charSequence : inputs) {
                if (!isEmpty(charSequence)) {
                    return false;
                }
            }
            return true;
        }
        return true;
    }

    public static boolean isAnyEmpty(CharSequence... inputs) {
        if (inputs != null) {
            for (CharSequence charSequence : inputs) {
                if (isEmpty(charSequence)) {
                    return true;
                }
            }
            return false;
        }
        return false;
    }

    /**
     * 判断字符串是否非空
     *
     * @param input 需要判断的字符串
     * @return 返回字符串是否为空
     */
    public static boolean isNotEmpty(CharSequence input) {
        return !isEmpty(input);
    }

    /**
     * 判断字符串是否为int
     *
     * @param str 需要判断的字符串
     * @return 如果字符串为整数返回true, 否则返回false
     */
    public static boolean isInteger(String str) {
        return isValid(str, Integer::parseInt);
    }

    /**
     * 判断字符串是否为float
     *
     * @param str 需要判断的字符串
     * @return 如果字符串为整数返回true, 否则返回false
     */
    public static boolean isFloat(String str) {
        return isValid(str, Float::parseFloat);
    }

    /**
     * 判断字符串是否为long
     *
     * @param str 需要判断的字符串
     * @return 如果字符串为整数返回true, 否则返回false
     */
    public static boolean isLong(String str) {
        return isValid(str, Long::parseLong);
    }

    /**
     * 判断字符串是否为double
     *
     * @param str 需要判断的字符串
     * @return 如果字符串为整数返回true, 否则返回false
     */
    public static boolean isDouble(String str) {
        return isValid(str, Double::parseDouble);
    }

    public static boolean isShort(String value) {
        return isValid(value, Short::parseShort);
    }

    public static boolean isByte(String value) {
        return isValid(value, Byte::parseByte);
    }

    public static boolean isValid(String str, Function<String, Object> valid) {
        if (isEmpty(str)) {
            return false;
        }
        try {
            valid.apply(str);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    public static boolean isBoolean(String value) {
        return Objects.equals("true", value) || Objects.equals("false", value);
    }

    /**
     * 判断字符串是否为ip地址
     *
     * @param str 需要判断的字符串
     * @return 如果字符串为整数返回true, 否则返回false
     */
    public static boolean isIpAddress(String str) {

        if (isEmpty(str)) {
            return false;
        }

        if (str.length() < IP_MIN_LEN || str.length() > IP_MAX_LEN) {
            return false;
        }

        String rexp = "([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}";
        Pattern pat = Pattern.compile(rexp);
        return pat.matcher(str).find();
    }

    /**
     * 判断字符串是否为日期 此方法只能判断格式为 1.年 月 日 2.年/月/日 3.年-月-日
     *
     * @param strDate
     * @return
     */
    public static boolean isDate(String strDate) {
        String patt = "^((\\d{2}(([02468][048])|([13579][26]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])))))|(\\d{2}(([02468][1235679])|([13579][01345789]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|(1[0-9])|(2[0-8]))))))(\\s(((0?[0-9])|([1-2][0-3]))\\:([0-5]?[0-9])((\\s)|(\\:([0-5]?[0-9])))))?$";
        Pattern pattern = Pattern.compile(patt);
        Matcher m = pattern.matcher(strDate);
        return m.matches();
    }

    /**
     * 获取指定长的的随机字符串
     *
     * @param length 指定字符串长度
     * @return 返回指定长的的随机字符串
     */
    public static String getRandomString(int length) {
        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random = new SecureRandom();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(str.length() - 1);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }

    public static boolean hasText(CharSequence str) {
        if (!hasLength(str)) {
            return false;
        }
        int strLen = str.length();
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    public static boolean hasLength(CharSequence str) {
        return (str != null && !str.isEmpty());
    }

    /**
     * 判断是否包含其中某个字符
     *
     * @param text
     * @param ch
     * @return
     */
    public static boolean contain(String text, char... ch) {
        if (isEmpty(text)) {
            return false;
        }
        for (char c : text.toCharArray()) {
            for (char ch1 : ch) {
                if (c == ch1) {
                    return true;
                }
            }

        }
        return false;
    }

    /**
     * 判断是否包含其中某个字符
     *
     * @param text
     * @param ch
     * @return
     */
    public static boolean containAll(String text, char... ch) {
        if (isEmpty(text)) {
            return false;
        }
        int n = 0;
        for (char c : text.toCharArray()) {
            for (char ch1 : ch) {
                if (c != ch1) {
                    return false;
                }
                n++;
            }
        }
        return n == ch.length;
    }

    public static String md5(String resource) {
        return md5(resource, StandardCharsets.UTF_8.name());
    }

    public static String md5(Object obj) {
        Map<String, Object> map = BeanUtil.beanToMap(obj, new TreeMap<>(), false, true);
        return md5(map);
    }

    public static <K, V> String md5(SortedMap<K, V> map) {
        return md5(JSON.toJSONString(map));
    }

    /**
     * 给大文本生产一个MD5，如果文本一样则返回MD5也是一样的
     *
     * @param resource 需要转换成md5的字符串
     * @return 字符数组转换成字符串返回
     */
    public static String md5(String resource, String charset) {
        /**
         * 拿到一个MD5转换器（如果想要SHA1加密参数换成"SHA1"）
         **/
        MessageDigest messageDigest = null;
        try {
            messageDigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            return resource;
        }
        /**
         * 输入的字符串转换成字节数组
         */
        byte[] inputByteArray = resource.getBytes(Charset.forName(charset));
        /**
         * inputByteArray是输入字符串转换得到的字节数组
         */
        messageDigest.update(inputByteArray);
        /**
         * 转换并返回结果，也是字节数组，包含16个元素
         */
        byte[] resultByteArray = messageDigest.digest();

        return byteArrayToHex(resultByteArray);
    }

    /**
     * 字符数组组合成字符串返回
     *
     * @param byteArray byte字符数组
     * @return 返回byte数组生产的String
     */
    public static String byteArrayToHex(byte[] byteArray) {
        /**
         * 首先初始化一个字符数组，用来存放每个16进制字符
         */
        char[] hexDigits = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
        /**
         * new一个字符数组，这个就是用来组成结果字符串的（解释一下：一个byte是八位二进制，也就是2位十六进制字符）
         */
        char[] resultCharArray = new char[byteArray.length << 1];
        /**
         * 遍历字节数组，通过位运算（位运算效率高），转换成字符放到字符数组中去
         */
        int index = 0;
        for (byte b : byteArray) {
            resultCharArray[index++] = hexDigits[b >>> 4 & 0xf];
            resultCharArray[index++] = hexDigits[b & 0xf];
        }
        return new String(resultCharArray);
    }

    /**
     * 获取字符串的最后一个词语
     *
     * @param re   用于切分的正则
     * @param text 需要切分的字符串
     * @return 最后一个词
     */
    public static String getLastWord(String re, String text) {
        return text.split(re)[text.split(re).length - 1];
    }

    /**
     * InputStream 转换为字符串
     *
     * @param in      InputStream
     * @param charset 编码格式
     * @return
     * @throws IOException
     */
    public static String streamToString(InputStream in, Charset charset) throws IOException {
        StringBuilder out = new StringBuilder();
        byte[] b = new byte[4096];
        for (int n; (n = in.read(b)) != -1; ) {
            if (charset != null) {
                out.append(new String(b, 0, n, charset));
            } else {
                out.append(new String(b, 0, n, StandardCharsets.UTF_8));
            }
        }
        return out.toString();
    }

    /**
     * InputStream 转换为字符串
     *
     * @param in InputStream
     * @return
     * @throws IOException
     */
    public static String streamToString(InputStream in) throws IOException {
        if (in == null) {
            return EMPTY;
        }
        StringBuilder out = new StringBuilder();
        byte[] b = new byte[4096];
        for (int n; (n = in.read(b)) != -1; ) {
            out.append(new String(b, 0, n, StandardCharsets.UTF_8));
        }
        return out.toString();
    }

    /***
     * 下划线命名转为驼峰命名
     *
     * @param para
     *            下划线命名的字符串
     */

    public static String underlineToLowerCamel(String para) {
        if (isEmpty(para)) {
            return EMPTY;
        }
        StringBuilder result = new StringBuilder();

        if (!para.contains(StrPool.UNDERLINE)) {
            return lowerCaseFirstChar(para);
        }
        String[] a = para.split(StrPool.UNDERLINE);
        for (String s : a) {
            if (result.isEmpty()) {
                result.append(s.toLowerCase());
            } else {
                result.append(s.substring(0, 1).toUpperCase());
                result.append(s.substring(1).toLowerCase());
            }
        }
        return result.toString();
    }

    public static String upperCaseFirstChar(String str) {
        if (isEmpty(str)) {
            return EMPTY;
        }

        char fistChar = str.charAt(0);
        int a = 32;
        int z = 90;
        int diff = fistChar - a;
        if (a <= diff && diff <= z) {
            return (char) diff + str.substring(1);
        }
        return str;
    }

    public static String lowerCaseFirstChar(String str) {
        if (isEmpty(str)) {
            return EMPTY;
        }
        char fistChar = str.charAt(0);
        int a = 32;
        int b = 97;
        int c = 122;
        int diff = fistChar + a;
        if (b <= diff && diff <= c) {
            return (char) diff + str.substring(1);
        }
        return str;
    }

    /**
     * 驼峰命名转为下划线命名
     *
     * @param para 驼峰命名的字符串
     */
    public static String lowerCamelToUnderline(String para) {
        if (isEmpty(para)) {
            return EMPTY;
        }
        para = lowerCaseFirstChar(para);
        StringBuilder sb = new StringBuilder(para);
        int temp = 0;
        int a = 32;
        for (int i = 0; i < para.length(); i++) {
            char ch = para.charAt(i);
            if (Character.isUpperCase(ch)) {
                sb.replace(i + temp, i + temp + 1, StrPool.UNDERLINE + (char) (ch + a));
                temp += 1;
            }
        }
        return sb.toString();
    }

    /***
     * 空格命名转为驼峰命名
     *
     * @param para
     *            下划线命名的字符串
     */
    public static String toLowerCamel(String para) {
        if (isEmpty(para)) {
            return EMPTY;
        }
        if (para.contains(StrPool.UNDERLINE)) {
            return underlineToLowerCamel(para);
        } else {
            return underlineToLowerCamel(para.replace(EMPTY, StrPool.UNDERLINE));
        }
    }

    /**
     * 中文简体转繁体
     *
     * @param source
     * @return
     */
    public static String simplifiedToTraditional(String source) throws IOException {
        initCnTraditionalSimplified();
        return doTranslate(source, true);
    }

    /**
     * 中文繁体转简体
     *
     * @param source
     * @return
     */
    public static String traditionalToSimplified(String source) throws IOException {
        initCnTraditionalSimplified();
        return doTranslate(source, false);
    }

    private static String doTranslate(String str, boolean simplified2traditional) {
        char[] chars = str.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            char currentChar = chars[i];
            Character character = null;
            if (simplified2traditional) {
                character = simplifiedMap.get(currentChar);
            } else {
                character = traditionalMap.get(currentChar);
            }
            if (character != null) {
                chars[i] = character;
            }
        }
        return new String(chars);
    }

    public static boolean equals(String source0, String source1) {
        return !isEmpty(source0) && source0.equals(source1);
    }

    public static Set<String> commaDelimitedListToStringSet(String str) {
        if (StringUtil.isEmpty(str)) {
            return Collections.emptySet();
        }
        return Stream.of(str.split(StrPool.COMMA)).collect(Collectors.toSet());
    }

    public static Set<Character> commaDelimitedListToCharSet(String str) {
        Set<Character> result = new HashSet<>();
        if (StringUtil.isEmpty(str)) {
            return result;
        }

        for (char c : str.toCharArray()) {
            if (c != ',') {
                result.add(c);
            }
        }
        return result;
    }

    /**
     * 1,2,3,4转化为Set<Integer>集合
     *
     * @param str
     * @return
     */
    public static Set<Integer> commaDelimitedListToIntegerSet(String str) {
        Set<Integer> result = new HashSet<>();
        if (StringUtil.isEmpty(str)) {
            return result;
        }

        for (String s : str.split(StrPool.COMMA)) {
            result.add(StringUtil.toInteger(s, -1));

        }
        return result;
    }

    /**
     * 1,2,3,4转化为Set<Long>集合
     *
     * @param str
     * @return
     */
    public static Set<Long> commaDelimitedListToLongSet(String str) {
        Set<Long> result = new HashSet<>();
        if (StringUtil.isEmpty(str)) {
            return result;
        }

        for (String s : str.split(StrPool.COMMA)) {
            result.add(StringUtil.toLong(s, -1L));

        }
        return result;
    }

    /**
     * Set<Long>集合转化为1,2,3,4
     *
     * @param collection
     * @return
     */
    public static String collectionToCommaDelimitedList(Collection<String> collection) {
        if (CollUtil.isEmpty(collection)) {
            return EMPTY;
        }
        String[] array = collection.toArray(new String[0]);
        return arrayToCommaDelimitedList(array);
    }

    /**
     * String数组转化为1,2,3,4
     *
     * @param array
     * @return
     */
    public static String arrayToCommaDelimitedList(String[] array) {
        if (array == null || array.length == 0) {
            return EMPTY;
        }
        return String.join(",", array);
    }

    public static List<String[]> parsePairs(String str, String listSplit, String pairSplit) {
        String[] tmp = split(str, listSplit);
        List<String[]> r = new ArrayList<>(tmp.length);
        for (int i = 0; i < tmp.length; i++) {
            String p = tmp[i];
            String[] pair = split(p, pairSplit);
            if (pair.length != 2) {
                throw new IllegalArgumentException("Wrong pair [ " + p + " ]");
            }
            if (pair[0] == null || pair[1] == null) {
                throw new IllegalArgumentException("Wrong pair [ " + p + " ]");
            }
            String[] kv = new String[]{pair[0].trim(), pair[1].trim()};
            r.add(kv);
        }
        return r;
    }

    public static String[] split(String str, String split) {
        List<String> strings = new ArrayList<>(64);
        StringTokenizer token = new StringTokenizer(str, split, false);
        while (token.hasMoreElements()) {
            strings.add(token.nextToken());
        }
        String[] strArray = new String[strings.size()];
        return strings.toArray(strArray);
    }

    public static String toString(String[] strs, String seperator) {
        if (strs == null) {
            return null;
        }
        return String.join(seperator, strs);
    }

    /**
     * 输入一个正整数，输出mode后，一定长度的数值
     *
     * @param n
     * @param mode
     * @param padLength
     * @return
     */
    public static String modeStr(int n, int mode, int padLength) {
        if (padLength < 1) {
            throw new IllegalArgumentException("padLength must larget than 0 .");
        }
        String leave = String.valueOf(n % mode);
        while (leave.length() < padLength) {
            leave = "0" + leave;
        }

        return leave;
    }

    private static void initCnTraditionalSimplified() throws IOException {
        if (initCnTraditionalSimplified.compareAndSet(false, true)) {
            try (InputStream in = StringUtil.class.getResourceAsStream("/traditional-simplified-map.dict");
                 InputStreamReader isr = new InputStreamReader(in, StandardCharsets.UTF_8);
                 BufferedReader reader = new BufferedReader(isr)) {
                simplifiedMap = new HashMap<>(2600);
                traditionalMap = new HashMap<>(2600);
                String line = null;
                while ((line = reader.readLine()) != null) {
                    String[] texts = line.split("=");
                    String traditional = texts[0].strip();
                    String simplified = texts[1].strip();
                    char s = simplified.charAt(0);
                    char t = traditional.charAt(0);
                    simplifiedMap.put(s, t);
                    traditionalMap.put(t, s);
                }
            }
        }
    }


    public static int compareTo(String left, String right) {
        boolean label = (left == null && right == null) || Objects.equals(left, right);
        if (label) {
            return 0;
        }

        if (left == null) {
            return -1;
        }

        if (right == null) {
            return 1;
        }
        return left.compareTo(right);
    }

    /**
     * 生成中文字符
     *
     * @return
     */
    public static Pair<Character, String> genChineseCharacter() {
        int start = 0x4E00;
        int end = 0x9FEF;
        int times = end - start;
        int val = start + new Random(100).nextInt() * times;
        String hexString = Integer.toHexString(val);
        char ch = (char) Integer.parseInt(hexString, 16);
        return Pair.of(ch, hexString);
    }

    /**
     * 16进制数转字符
     *
     * @param hexString
     * @return
     */
    public static Character toCharacter(String hexString) {
        return (char) Integer.parseInt(hexString, 16);
    }

    /**
     * unicode 编码转字符串
     *
     * @param str
     * @return
     */
    public static String unicode2str(String str) {
        Pattern pattern = Pattern.compile("(\\\\u(\\p{XDigit}{4}))");
        Matcher matcher = pattern.matcher(str);
        char ch;
        while (matcher.find()) {
            String str1 = matcher.group(1);
            String str2 = matcher.group(2);
            ch = (char) Integer.parseInt(str2, 16);
            str = str.replace(str1, String.valueOf(ch));
        }
        return str;
    }

    /**
     * 字符串转unicode 编码
     *
     * @param source
     * @return
     */
    public static String str2unicode(String source) {
        StringBuilder unicode = new StringBuilder();
        for (int i = 0; i < source.length(); i++) {
            char c = source.charAt(i);
            unicode.append("\\u").append(Integer.toHexString(c));
        }
        return unicode.toString();
    }

    public static String toLowerCase(String str) {
        if (str == null) {
            return null;
        }
        return str.toLowerCase(Locale.getDefault());
    }

    public static String toUpperCase(String str) {
        if (str == null) {
            return null;
        }
        return str.toUpperCase(Locale.getDefault());
    }
}
