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
			
			//�ۼ��ܵĽ��մ���
			hah.setAccepts(value.getAccepts()+hah.getAccepts());
			//�ۼ��ܵ��������
			hah.setAttempts(hah.getAttempts()+value.getAttempts());
			//�ۼ��ܵ���������
			hah.setTrafficDL(hah.getTrafficDL()+value.getTrafficDL());
			//�ۼ��ܵ���������
			hah.setTrafficUL(hah.getTrafficUL()+value.getTrafficUL());
			//�ۼ��ش���������
			hah.setRetranDL(hah.getRetranDL()+value.getRetranDL());
			//�ۼ��ش���������
			hah.setRetranUL(hah.getRetranUL()+value.getRetranUL());
			//�ۼ��ܵ�����ʱ��
			hah.setTransDelay(hah.getTransDelay()+value.getTransDelay());
			
			context.write(hah, NullWritable.get());
		}
	}
}
