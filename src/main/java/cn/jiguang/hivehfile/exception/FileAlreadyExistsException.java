package cn.jiguang.hivehfile.exception;

/**
 * Created by: fitz
 * <p>
 * Date: 2018/1/17
 * <p>
 * Description:
 */
public class FileAlreadyExistsException extends Exception{
    public FileAlreadyExistsException(String msg){
        super(msg);
    }
}
