package cn.py.zebra;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ZebraReduce extends Reducer<Text, HttpAppHost, HttpAppHost, NullWritable>{
	
	@Override
	protected void reduce(Text key, Iterable<HttpAppHost> values,
			Reducer<Text, HttpAppHost, HttpAppHost, NullWritable>.Context context)
					throws IOException, InterruptedException {
		
		HttpAppHost hah = new HttpAppHost();
		
		for (HttpAppHost value : values) {
			hah.setReportTime(value.getReportTime());
			hah.setCellid(value.getCellid());
			hah.setAppType(value.getAppType());
			hah.setAppSubtype(value.getAppSubtype());
			hah.setUserIP(value.getUserIP());
			hah.setUserPort(value.getUserPort());
			hah.setAppServerIP(value.getAppServerIP());
			hah.setAppServerPort(hah.getAppServerPort());
			
			//累加总的接收次数
			hah.setAccepts(value.getAccepts()+hah.getAccepts());
			//累加总的请求次数
			hah.setAttempts(hah.getAttempts()+value.getAttempts());
			//累加总的下行流量
			hah.setTrafficDL(hah.getTrafficDL()+value.getTrafficDL());
			//累加总的上行流量
			hah.setTrafficUL(hah.getTrafficUL()+value.getTrafficUL());
			//累加重传下行流量
			hah.setRetranDL(hah.getRetranDL()+value.getRetranDL());
			//累加重传下行流量
			hah.setRetranUL(hah.getRetranUL()+value.getRetranUL());
			//累加总的请求时长
			hah.setTransDelay(hah.getTransDelay()+value.getTransDelay());
			
			context.write(hah, NullWritable.get());
		}
	}
}
