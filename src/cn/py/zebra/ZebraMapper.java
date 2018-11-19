package cn.py.zebra;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ZebraMapper extends Mapper<LongWritable, Text, Text, HttpAppHost>{
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, HttpAppHost>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] data = line.split("\\|");
		HttpAppHost hah = new HttpAppHost();
		
		FileSplit split = (FileSplit) context.getInputSplit();
		//设置用户上网时间
		hah.setReportTime(split.getPath().getName().split("_")[1]);
		//小区id
		hah.setCellid(data[16]);
		//--应用大类
				hah.setAppType(Integer.parseInt(data[22]));
				//--应用子类
				hah.setAppSubtype(Integer.parseInt(data[23]));
				//--用户上网ip
				hah.setUserIP(data[26]);
				//--用户上午端口
				hah.setUserPort(Integer.parseInt(data[28]));
				//--服务端ip
				hah.setAppServerIP(data[30]);
				//--服务端端口号
				hah.setAppServerPort(Integer.parseInt(data[32]));
				//--域名
				hah.setHost(data[58]);
				
				//--获取请求的响应码，在此业务中，只关心是否=103,如果是103,表示是一次成功的HTTP请求
				int appTypeCode=Integer.parseInt(data[18]);
				
				//--获取HTTP的状态码
				String transStatus=data[54];
				
				if(hah.getCellid()==null||hah.getCellid().equals("")){
				hah.setCellid("000000000");
				}
				
				//--设置请求次数为1
				if(appTypeCode==103){
				hah.setAttempts(1);
				}
				
				//--设置接收次数为1
				if(appTypeCode==103&&"10,11,12,13,14,15,32,33,34,35,36,37,38,48,49,50,51,52,53,54,55,199,200,201,202,203,204,205,206,302,304,306".contains(transStatus)){
				hah.setAccepts(1);
				}else{
				hah.setAccepts(0);
				}
				
				//--设置上行流量
				if(appTypeCode == 103){
				hah.setTrafficUL(Long.parseLong(data[33]));
				}
				//-设置下行流量
				if(appTypeCode == 103){
				hah.setTrafficDL(Long.parseLong(data[34]));
				}
				
				//--设置重传上行流量
				if(appTypeCode == 103){
				hah.setRetranUL(Long.parseLong(data[39]));
				}
				//--设置重传下行流量
				if(appTypeCode == 103){
				hah.setRetranDL(Long.parseLong(data[40]));
				}
				
				//--设置请求时长
				if(appTypeCode==103){
				hah.setTransDelay(Long.parseLong(data[20]) - Long.parseLong(data[19]));
				}
				
				String userKey=hah.getReportTime() + "|" + hah.getAppType() + "|" + hah.getAppSubtype() + "|" + hah.getUserIP() + "|" + hah.getUserPort() + "|" + hah.getAppServerIP() + "|" + hah.getAppServerPort() +"|" + hah.getHost() + "|" + hah.getCellid();
				
				context.write(new Text(userKey), hah);
				
			}
}
