package elasticsearch;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {
	
	public static void main(String[] args) throws IOException {
		
//		String[] ids = TimeZone.getAvailableIDs();
//		for (String id:ids) 
//		 System.out.println(id+", ");
		
		String log = "{\"message\":\"[2018-04-26 02:05:19.459] INFO    org.mule.api.processor.LoggerMessageProcessor [[rs-balances-service].Balances-Service-SFTP-Flow.stage1.650]: 4527efc0-48f6-11e8-9229-060be056ede8: RS-Balances request has no EasyPay Vendor specified, so routing to Featurespace (RME)\",\"path\":\"/home/frank/elk6/NEWGEN_log/rs-balances-service/20180426_122122/5ad06ec89d095b0fb99baf8c-0.txt\",\"@timestamp\":\"2018-04-26T00:05:19.459Z\",\"@version\":\"1\",\"host\":\"0.0.0.0\",\"type\":\"rs-balances-service\"}\r\n";
		String code = "\"IATACode\":\"54471180013\",\"EventType\":\"Update\"";
		
		// match event ID
		// String regx = "([a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12})";
		
		// 截取字符串中间的内容 ！！！
		String regx = "message\":\"(.*?)INFO";
		
		String regx2 = "\"IATACode\":\"(.*?)\"";
		
		Pattern pattern = Pattern.compile(regx2);
		
		Matcher match = pattern.matcher(code);
		
		StringBuilder result = new StringBuilder("");
		
		if(match.find()) {
			System.out.println(match.group(1));
			result.append(match.group(1));
		}
		
//		String date = result.substring(1, 24);
//		
//		System.out.println(date);
		
//		File file = null;
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//        String now = sdf.format(new Date());
//        String path = "c:/Users/wangjm@iata.org/Desktop/";
//        
//        file = new File(path + now);
//        
//        if (!file.exists()) {
//        	file.mkdir();
//        	System.out.println("Dir success");
//        } else {
//        	System.out.println("File already exist!");
//        }
//        
//        File file2 = new File(path + now + "/", "667776.txt");
////        try {
////			file2.createNewFile();
////		} catch (IOException e) {
////			// TODO Auto-generated catch block
////			e.printStackTrace();
////		}
//        
//        
//        FileWriter fw = new FileWriter(file2);
//        
//        fw.write("777");
//        fw.write("\r\n");
//        fw.write("888");
        
//        FileOutputStream fOutputStream = null;
//        OutputStreamWriter writer = null;
//        try {
//			fOutputStream=new FileOutputStream(file2);
//			writer=new OutputStreamWriter(fOutputStream);
//			
//			writer.append("ajls;djf;alsjdlkfja;");
//			
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} finally {
////			fOutputStream.close();
////			 .close();
//		}
//        fw.close();
        
//        System.out.println(file.getAbsolutePath());
		
		
//		
//		Date now = new Date();
//		String s = now.getTime() + "";
//		System.out.println(now.getTime());
        
	}

}
