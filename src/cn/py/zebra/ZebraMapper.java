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
		//�����û�����ʱ��
		hah.setReportTime(split.getPath().getName().split("_")[1]);
		//С��id
		hah.setCellid(data[16]);
		//--Ӧ�ô���
				hah.setAppType(Integer.parseInt(data[22]));
				//--Ӧ������
				hah.setAppSubtype(Integer.parseInt(data[23]));
				//--�û�����ip
				hah.setUserIP(data[26]);
				//--�û�����˿�
				hah.setUserPort(Integer.parseInt(data[28]));
				//--�����ip
				hah.setAppServerIP(data[30]);
				//--����˶˿ں�
				hah.setAppServerPort(Integer.parseInt(data[32]));
				//--����
				hah.setHost(data[58]);
				
				//--��ȡ�������Ӧ�룬�ڴ�ҵ���У�ֻ�����Ƿ�=103,�����103,��ʾ��һ�γɹ���HTTP����
				int appTypeCode=Integer.parseInt(data[18]);
				
				//--��ȡHTTP��״̬��
				String transStatus=data[54];
				
				if(hah.getCellid()==null||hah.getCellid().equals("")){
				hah.setCellid("000000000");
				}
				
				//--�����������Ϊ1
				if(appTypeCode==103){
				hah.setAttempts(1);
				}
				
				//--���ý��մ���Ϊ1
				if(appTypeCode==103&&"10,11,12,13,14,15,32,33,34,35,36,37,38,48,49,50,51,52,53,54,55,199,200,201,202,203,204,205,206,302,304,306".contains(transStatus)){
				hah.setAccepts(1);
				}else{
				hah.setAccepts(0);
				}
				
				//--������������
				if(appTypeCode == 103){
				hah.setTrafficUL(Long.parseLong(data[33]));
				}
				//-������������
				if(appTypeCode == 103){
				hah.setTrafficDL(Long.parseLong(data[34]));
				}
				
				//--�����ش���������
				if(appTypeCode == 103){
				hah.setRetranUL(Long.parseLong(data[39]));
				}
				//--�����ش���������
				if(appTypeCode == 103){
				hah.setRetranDL(Long.parseLong(data[40]));
				}
				
				//--��������ʱ��
				if(appTypeCode==103){
				hah.setTransDelay(Long.parseLong(data[20]) - Long.parseLong(data[19]));
				}
				
				String userKey=hah.getReportTime() + "|" + hah.getAppType() + "|" + hah.getAppSubtype() + "|" + hah.getUserIP() + "|" + hah.getUserPort() + "|" + hah.getAppServerIP() + "|" + hah.getAppServerPort() +"|" + hah.getHost() + "|" + hah.getCellid();
				
				context.write(new Text(userKey), hah);
				
			}
}
