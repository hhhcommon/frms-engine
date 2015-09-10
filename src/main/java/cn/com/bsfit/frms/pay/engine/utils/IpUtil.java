package cn.com.bsfit.frms.pay.engine.utils;


public class IpUtil {
	/**
	 * ip地址转换成16进制long
	 * @param ipString
	 * @return
	 */
	public static Long ipToLong(String ipString) {
		Long[] ip = new Long[4];
		int pos1= ipString.indexOf(".");
		int pos2= ipString.indexOf(".",pos1+1);
		int pos3= ipString.indexOf(".",pos2+1);
		ip[0] = Long.parseLong(ipString.substring(0 , pos1));
		ip[1] = Long.parseLong(ipString.substring(pos1+1 , pos2));
		ip[2] = Long.parseLong(ipString.substring(pos2+1 , pos3));
		ip[3] = Long.parseLong(ipString.substring(pos3+1));
		return (ip[0]<<24)+(ip[1]<<16)+(ip[2]<<8)+ip[3];
	}
	
	public static void main(String[] args){
		 System.out.println(ipToLong("192.168.1.1"));
	 }
}
