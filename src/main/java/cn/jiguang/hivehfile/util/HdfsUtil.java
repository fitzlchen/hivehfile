package cn.jiguang.hivehfile.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by: fitz
 * <p>
 * Date: 2017/12/28
 * <p>
 * Description:
 */
public class HdfsUtil {

   private static final Logger LOG = LoggerFactory.getLogger(HdfsUtil.class);

   private static void setFilePaths(FileSystem hdfs, List<String> filePaths, String path)
           throws IOException {
      // 源文件
      Path srcPath = new Path(path);
      if (hdfs.isFile(srcPath)) { // 是文件直接添加
         filePaths.add(path);
         LOG.info("load input path: " + srcPath.toString());
      } else if (hdfs.getFileStatus(srcPath).isDirectory()) {// 是目录获取目录下文件and目录
         FileStatus[] status = hdfs.listStatus(srcPath);
         for (int i = 0; i < status.length; ++i) {
            setFilePaths(hdfs, filePaths, status[i].getPath().toString());
         }
      }
   }

   private static void setFilePaths(FileSystem hdfs, List<String> filePaths, String path, String filePattern)
           throws IOException {
      // 源文件
      Path srcPath = new Path(path);
      if (hdfs.isFile(srcPath)) { // 是文件直接添加
         getFilePaths(hdfs, filePaths, srcPath, filePattern);
      } else if (hdfs.getFileStatus(srcPath).isDirectory()) {// 是目录获取目录下文件and目录
         FileStatus[] status = hdfs.listStatus(srcPath);
         for (int i = 0; i < status.length; ++i) {
            setFilePaths(hdfs, filePaths, status[i].getPath().toString(), filePattern);
         }
      }
   }

   private static void getFilePaths(FileSystem hdfs, List<String> filePaths, Path dir,
                                    String filePattern) throws IOException {
      final Pattern pattern = Pattern.compile(filePattern);
      PathFilter filter = new PathFilter() {
         public boolean accept(Path path) {
            String fileName = path.getName();
            return pattern.matcher(fileName).find();
         }
      };
      FileStatus[] status = hdfs.listStatus(dir, filter);
      for (int i = 0; i < status.length; i++) {
         Path path = status[i].getPath();
         long fileSize = status[i].getLen();
         // fileSize greater then 42, because flume has exception when close the compression data file
         if (hdfs.isFile(path) && fileSize > 42) {
            filePaths.add(path.toString());
            LOG.info(path.toString());
         }
      }
   }


   private static void getFilePaths(FileSystem hdfs, List<String> filePaths, Path dir,
                                    final long timestamp) throws IOException {
      PathFilter filter = new PathFilter() {
         public boolean accept(Path path) {

            String fileTimestamp = path.getName().split("\\.")[1];
            if (Long.parseLong(fileTimestamp) >= timestamp
                    && Long.parseLong(fileTimestamp) <= (timestamp + 3600000)) {
               return true;
            } else
               return false;
         }
      };
      FileStatus[] status = hdfs.listStatus(dir, filter);
      for (int i = 0; i < status.length; i++) {
         Path path = status[i].getPath();
         if (hdfs.isFile(path)) {
            filePaths.add(path.toString());
            LOG.info(path.toString());
         }
      }
   }

   /*
    * 获取路径下所有文件的路径--任意深度遍历
    */
   public static List<String> getAllFilePaths(String path) throws IOException {
      List<String> resultList = new ArrayList<String>();
      if (null == path)
         return resultList;

      Configuration conf = new Configuration();
      FileSystem hdfs = FileSystem.get(conf);
      try {
         setFilePaths(hdfs, resultList, path);
      } finally {
         hdfs.close();
      }
      return resultList;
   }

   /*
    * 获取路径下所有文件的路径--依据给定正则表达式，任意深度遍历
    */
   public static List<String> getAllFilePaths(String path, String filePattern) throws IOException {
      List<String> resultList = new ArrayList<String>();
      if (null == path)
         return resultList;

      Configuration conf = new Configuration();
      FileSystem hdfs = FileSystem.get(conf);
      try {
         setFilePaths(hdfs, resultList, path, filePattern);
      } finally {
         hdfs.close();
      }
      return resultList;
   }

   public static List<String> getSegmentFilePaths(String path, Long timestamp) throws IOException {
      List<String> resultList = new ArrayList<String>();
      if (null == path)
         return resultList;

      Configuration conf = new Configuration();
      FileSystem hdfs = FileSystem.get(conf);
      Path dir = new Path(path);
      try {
         getFilePaths(hdfs, resultList, dir, timestamp);
      } finally {
         hdfs.close();
      }
      return resultList;
   }

   /**
    * 获取路径下按时间片正则匹配的文件路径
    *
    * @throws IOException
    */
   public static List<String> getSegmentFilePaths(String path, String filePattern)
           throws IOException {
      List<String> resultList = new ArrayList<String>();
      if (null == path)
         return resultList;

      Configuration conf = new Configuration();
      FileSystem hdfs = FileSystem.get(conf);
      Path dir = new Path(path);
      try {
         getFilePaths(hdfs, resultList, dir, filePattern);
      } finally {
         hdfs.close();
      }
      return resultList;
   }

   /**
    * HDFS内文件转
    *
    * 多个文件==>目录
    *
    * @param srcFiles 多个文件list
    * @param dstDir 目录
    */
   public static boolean rename(List<String> srcFiles, String dstDir) throws IOException {
      LOG.info("HdfsUtil.rename() start.");
      long startTime = System.currentTimeMillis();

      if (srcFiles.size() == 0) {
         return true;
      }
      if (dstDir.endsWith("/")) {
         dstDir = dstDir.substring(0, dstDir.length() - 1);
      }

      Configuration conf = null;
      FileSystem hdfs = null;
      Boolean resultStatus = false;
      try {
         conf = new Configuration();
         hdfs = FileSystem.get(conf);
         //
         Path[] srcFilePaths = new Path[srcFiles.size()];
         for (int i = 0; i < srcFiles.size(); i++) {
            srcFilePaths[i] = new Path(srcFiles.get(i));
         }

         // 目标
         Path dstDirPath = new Path(dstDir);
         if (!hdfs.exists(dstDirPath)) {// 不存在则创建
            hdfs.mkdirs(dstDirPath);
         }

         for (int i = 0; i < srcFilePaths.length; i++) {
            Path srcFilePath = srcFilePaths[i];
            Path dstFilePath = new Path(dstDir + "/" + srcFilePath.getName());

            resultStatus = hdfs.rename(srcFilePath, dstFilePath);
            if (!resultStatus) {
               return false;
            }
         }
      } finally {
         hdfs.close();
      }

      long endTime = System.currentTimeMillis();
      LOG.info("HdfsUtil.rename() end.");
      LOG.info("HdfsUtil.rename() execution time [" + (new Double(endTime - startTime) / 1000)
              + " s].");
      return resultStatus;
   }

   /**
    * HDFS内文件转
    *
    * 说明: 1)单文=>单文)目录 ==> 目录
    *
    * @param src
    * @param dst 目录
    */
   public static boolean rename(String src, String dst) throws IOException {
      Configuration conf = null;
      FileSystem hdfs = null;
      Boolean resultStatus = false;
      try {
         conf = new Configuration();
         hdfs = FileSystem.get(conf);
         Path srcPath = new Path(src);
         Path dstPath = new Path(dst);

         if (hdfs.exists(dstPath)) {// 存在,则删除目标已经存在,则rename失败)
            hdfs.delete(dstPath, true);
         }
         resultStatus = hdfs.rename(srcPath, dstPath);
         if (!resultStatus) {
            return false;
         }
      } finally {
         hdfs.close();
      }

      return resultStatus;
   }

   /**
    * 判断文件路径是否存在
    *
    * @param pathStr 文件路径
    */
   public static Boolean exists(String pathStr) throws IOException {
      Configuration conf = new Configuration();
      FileSystem hdfs = FileSystem.get(conf);

      Boolean resultStatus = false;
      try {
         Path path = new Path(pathStr);
         resultStatus = hdfs.exists(path);
      } finally {
         hdfs.close();
      }
      return resultStatus;
   }

   /**
    * 删除HDFS上的文件or文件
    *
    * 说明: 1)目录or文件不存不异
    *
    * @param pathStr
    * @param recursive 是否递归 --false,如果文件夹不是空则异 --该参数对于文件没有影
    */
   public static void delete(String pathStr, boolean recursive) throws IOException {
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);
      try {
         fs.delete(new Path(pathStr), recursive);
      } finally {
         fs.close();
      }
   }

   /**
    * 获取reduce输出的目录下的有效数据路
    *
    * 1)过滤不存在的文件;
    *
    * @param reduceNums reduce数量
    * @param reduceOutputPath reduce输出目录
    * @param fileSuffix 文件后缀(为null则不增加)
    */
   public static ArrayList<String> getReducerOutputFile(int reduceNums, String reduceOutputPath,
                                                        String fileSuffix) throws IOException {
      ArrayList<String> resultPaths = new ArrayList<String>();
      String realFileSuffix = fileSuffix;
      if (null == fileSuffix) {
         realFileSuffix = "";
      }
      for (int i = 0; i < reduceNums; i++) {
         String inpathStr = reduceOutputPath + "/" + getReducerOutputName(i) + realFileSuffix;
         if (exists(inpathStr)) {
            resultPaths.add(inpathStr);
         }
      }

      return resultPaths;
   }

   /**
    * 获取reducer的输出名称part-r-00469
    *
    */
   public static String getReducerOutputName(int index) {
      String a = String.valueOf(index);
      int length = a.length();
      for (int i = 0; i < 5 - length; i++) {
         a = "0" + a;
      }
      return "part-r-" + a;
   }

   public static List<String> getSubDirectories(String path) throws IOException {
      List<String> directoryList = new ArrayList<String>();
      Configuration conf = new Configuration();
      FileSystem hdfs = FileSystem.get(conf);
      FileStatus[] status = hdfs.listStatus(new Path(path));
      for (FileStatus file : status) {
         if (file.isDirectory()) {
            directoryList.add(file.getPath().toString());
         }
      }

      return directoryList;
   }

   /**
    * Closes object.
    *
    * @param object Object to close.
    */
   public static void close(final Closeable object) {
      try {
         if (object != null) {
            object.close();
         }
      } catch (IOException e) {
         LOG.error("Error while closing object", e);
      }
   }
}
