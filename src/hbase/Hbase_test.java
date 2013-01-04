package hbase;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class Hbase_test {
	private static HBaseConfiguration hBaseConfiguration=null;
	private static Configuration conf=null;
	
	static
	{
		conf=HBaseConfiguration.create();
		conf.set("hbase.master", "192.168.1.2");
		conf.set("hbase.zookeeper.property.clientPort","2181");
		conf.set("hbase.zookeeper.quorum","192.168.1.2,192.168.1.3," +
				" 192.168.1.4, 192.168.1.5, 192.168.1.6, 192.168.1.7, " +
				"192.168.1.8, 192.168.1.9, 192.168.1.10, 192.168.1.11, " +
				"192.168.1.12, 192.168.1.13, 192.168.1.14, 192.168.1.15," +
				"192.168.1.16, 192.168.1.17, 192.168.1.18, 192.168.1.19, 192.168.1.20");
	}
	//新建一个表
	public static void creatTable(String tableName,String [] familys) throws Exception
	{
		HBaseAdmin admin= new HBaseAdmin(conf);
		if(admin.tableExists(tableName))
		{
			System.out.println("table already exists");
		}
		else
		{
			HTableDescriptor hTableDescriptor=new HTableDescriptor(tableName);
			for(int i=0;i<familys.length;i++)
			{
				hTableDescriptor.addFamily(new HColumnDescriptor(familys[i]));
			}
			admin.createTable(hTableDescriptor);
			System.out.println("create table:"+tableName+"  Ok");
		}
	}
	//删除一个表
	public static void deleteTable(String tableName) throws Exception
	{
		try {
			HBaseAdmin admin=new HBaseAdmin(conf);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println("delete table:"+tableName+"  Ok");
			} catch (MasterNotRunningException e) {
		        e.printStackTrace();
			} catch (ZooKeeperConnectionException e) {
		        e.printStackTrace();
			}
	}
	//插入一行记录，参数分别为表名，行关键词，列族，列，值。
	public static void addRecord (String tableName, String rowKey, String family, String qualifier, String value)throws Exception
	{
		try {
		      HTable table = new HTable(conf, tableName);
		      Put put = new Put(Bytes.toBytes(rowKey));
		      put.add(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(value));
		      table.put(put);
		      System.out.println("insert recored " + rowKey + " to table " + tableName +" ok.");
		     } catch (IOException e) {
		      e.printStackTrace();
		     }
	}
	//删除一行记录
	public static void delRecord (String tableName, String rowKey) throws IOException{
	     HTable table = new HTable(conf, tableName);
	     List<Delete> list = new ArrayList<Delete>();
	     Delete del = new Delete(rowKey.getBytes());
	     list.add(del);
	     table.delete(list);
	     System.out.println("del recored " + rowKey + " ok.");
	    }
	//查找一行记录
	public static void getOneRecord (String tableName, String rowKey) throws IOException{
	     HTable table = new HTable(conf, tableName);
	     Get get = new Get(rowKey.getBytes());
	     Result rs = table.get(get);
	     for(KeyValue kv : rs.raw()){
	      System.out.print(new String(kv.getRow()) + " " );
	      System.out.print(new String(kv.getFamily()) + ":" );
	      System.out.print(new String(kv.getQualifier()) + " " );
	      System.out.print(kv.getTimestamp() + " " );
	      System.out.println(new String(kv.getValue()));
	     }
	    }
	//查询表中所有数据
	 public static void getAllRecord (String tableName) 
	 {
	     try{
	          HTable table = new HTable(conf, tableName);
	          Scan s = new Scan();
	          ResultScanner ss = table.getScanner(s);
	          for(Result r:ss){
	              for(KeyValue kv : r.raw()){
	              System.out.print(new String(kv.getRow()) + " ");
	              System.out.print(new String(kv.getFamily()) + ":");
	              System.out.print(new String(kv.getQualifier()) + " ");
	              System.out.print(kv.getTimestamp() + " ");
	                 System.out.println(new String(kv.getValue()));
	              }
	          }
	     } catch (IOException e){
	      e.printStackTrace();
	     }
	 }
	 //主函数
	 public static void  main (String [] agrs) {
		  try {
		   String tablename = "Hbase_test";
		   String[] familys = {"id", "name","score"};
		   Hbase_test.creatTable(tablename, familys);
		   
		   //添加一条记录
		   Hbase_test.addRecord(tablename,"2","id","","095832");
		   Hbase_test.addRecord(tablename,"2","name","","zmac");
		   Hbase_test.addRecord(tablename,"2","score","math","97");
		   Hbase_test.addRecord(tablename,"2","score","chinese","87");
		   Hbase_test.addRecord(tablename,"2","score","english","85");
		   //add record  baoniu
		   
		   System.out.println("===========get one record========");
		   Hbase_test.getOneRecord(tablename, "2");
		   
		   System.out.println("===========show all record========");
		   Hbase_test.getAllRecord(tablename);
		   
		  System.out.println("===========del one record========");
		  Hbase_test.delRecord(tablename, "2");
		   Hbase_test.getAllRecord(tablename);
		   
		  } catch (Exception e) {
		   e.printStackTrace();
		  }
		 }

	

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

}
