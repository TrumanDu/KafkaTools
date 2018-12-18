package com.newegg.ec.bigdata.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author：Truman.P.Du
 * @createDate: 2018年12月7日 下午1:32:09
 * @version:1.0
 * @description:文件处理工具类
 */
public class FileUtil {

	public static boolean writeFile(String pathName, String content) throws IOException {
		FileWriter fw = null;
		BufferedWriter bw = null;
		try {
			File file = new File(pathName);
			if (!file.exists()) {
				file.createNewFile();
			}
			// 覆盖写文件
			fw = new FileWriter(file);
			bw = new BufferedWriter(fw);
			bw.write(content);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			if (bw != null)
				bw.close();
			if (fw != null)
				fw.close();
		}
		return true;
	}

	public static String readFile(String pathName) throws IOException {
		FileReader fr = null;
		BufferedReader br = null;
		StringBuilder stringBuilder = new StringBuilder();
		File file = new File(pathName);
		try {
			fr = new FileReader(file);
			br = new BufferedReader(fr);
			String line = br.readLine();
			while (line != null) {
				stringBuilder.append(line);
				line = br.readLine();
			}
			br.readLine();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null)
				br.close();
			if (fr != null)
				fr.close();
		}
		return stringBuilder.toString();
	}

	public static boolean existsNotEmpty(String pathName) {
		File file = new File(pathName);
		if (!file.exists() || file.length() == 0) {
			return false;
		}
		return true;
	}
	
	public static boolean shExits(String shPathName) {
		boolean result = false;
		String path = "./";
		String shName = shPathName;
		if(shPathName.lastIndexOf("/")>0) {
			path = shPathName.substring(0, shPathName.lastIndexOf("/"));
			shName = shPathName.substring(shPathName.lastIndexOf("/")+1, shPathName.length());
		}
		File file = new File(path);
		File[] files = file.listFiles();
		for (File f : files) {
			if (f.getName().equals(shName)) {
				result = true;
				break;
			}
		}
		return result;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
