package cn.py.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReduce extends Reducer<Text, Item, Item, NullWritable>{
	Map<String, Item> productMap=new HashMap<>();
	
	@Override
	protected void reduce(Text key, Iterable<Item> values,
			Reducer<Text, Item, Item, NullWritable>.Context context) throws IOException, InterruptedException {
		
		List<Item> list = new ArrayList<Item>(); 
		
		for (Item value : values) {
			//--此处注意，因为迭代器用到了地址复用技术，不能直接存储
			Item item=value.clone();
			//--在迭代器里，注意地址复用的知识点
			list.add(item);
			if(value.getOrderId().equals("")){
				productMap.put(item.getProductId(),item);
			}
		}
		
		for (Item item : list) {
			if(!item.getOrderId().equals("")){
				Item pItem = new Item();
				pItem.setOrderId(item.getOrderId());
				pItem.setOrderTime(item.getOrderTime());
				pItem.setProductId(item.getProductId());
				pItem.setNum(item.getNum());
				pItem.setName(productMap.get(item.getProductId()).getName());
				pItem.setPrice(productMap.get(item.getProductId()).getPrice());
				
				context.write(pItem, NullWritable.get());
			}
		}
	}
}
