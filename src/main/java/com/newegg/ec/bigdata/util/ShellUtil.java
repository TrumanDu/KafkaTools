package com.newegg.ec.bigdata.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author：Truman.P.Du
 * @createDate: 2018年12月8日 上午9:54:35
 * @version:1.0
 * @description: 执行shell工具类
 */
public class ShellUtil {
	public static final Log logger = LogFactory.getLog(ShellUtil.class);

	public static String exec(String cmd) {
		String result = "";
		try {
			String[] cmds = { "bash", "-c",cmd};
		    System.out.println("bash -c "+cmd);
			Process ps = Runtime.getRuntime().exec(cmds);
			InputStream in = ps.getInputStream();
			result = processStdout(in);
			InputStream errorIn = ps.getErrorStream();
			result += processStdout(errorIn);
		} catch (IOException e) {
			logger.error(e);
		}
		return result;
	}

	private static String processStdout(InputStream in) {
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));

		StringBuilder sb = new StringBuilder();
		String line = null;
		try {
			while ((line = reader.readLine()) != null) {
				sb.append(line + "\n");
			}
		} catch (IOException e) {
			logger.error(e);
		} finally {
			try {
				in.close();
			} catch (IOException e) {
				logger.error(e);
			}
		}
		return sb.toString();

	}
	
	public static void main(String[] args) {
		String[] a = {"a","b"};
		
		String[] d = {"d","e","f"};
		a = Arrays.copyOf(a,a.length+d.length);
		System.out.println(Arrays.toString(a));
		System.arraycopy(d, 0, a, 2, d.length);
		System.out.println(Arrays.toString(a));
	}
}
