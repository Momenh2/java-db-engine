import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;

public class Tuple implements Serializable {
	
	Vector<Object> content = new Vector<Object>();
	
	
	public Tuple(){
		
	}

	public void addElement(Object element) {
		  content.add(element);
		}

	
	public boolean contains(Object element) {
		  return content.contains(element);
		}

	public Object getElement(int index) {
		  if (index >= 0 && index < content.size()) {
		    return content.get(index);
		  } else {
		    throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + content.size());
		  }
		}

	public int getSize() {
		  return content.size();
		}

	
	public String toString() {
		String res = "";
		
		for(int i =0; i<content.size();i++) {
			res+=content.get(i);
			
			if(i==content.size()-1)
				break;
			else
				res+=",";
		}
	//System.out.println("the size of the tuple is :"+content.size());
     return res;
		
	// ["ahmed" , "egypt" , "20"];	
		
		
	}
	
	public void update_index(String strTableName,String pagename) throws Exception {
		String csvFilePath = "metadata.csv";
		BufferedReader reader = new BufferedReader(new FileReader(csvFilePath));
		  //Enumeration<String> key = htblColNameValue.keys();
		String line;
        int m=0;
        bplustree m2;
        String treetbr;
		while ((line = reader.readLine()) != null) {
			String[] parts = line.split(",");
			if (parts[0].equals(strTableName)) {
				
				//newTuple.content.add(""+htblColNameValue.get(parts[1]));
				if(parts[5].equals("B+tree")) {
					
					//String indexinhash = ((Enumeration<String>)htblColNameValue).nextElement();
					treetbr=parts[4];
					//htblColNameValue.get(key)
					FileInputStream fileIn = new FileInputStream(treetbr+".class");
					ObjectInputStream in = new ObjectInputStream(fileIn);
					m2 = (bplustree) in.readObject();
					System.out.println(treetbr+"");
					//m2.delete(""+this.content.get(m));
					if(DBApp.canConvertToInt(""+this.content.get(m))) {
						m2.delete((Integer.parseInt(""+this.content.get(m))));
						System.out.println("inside the first if");
					}
					else {
						m2.delete(""+this.content.get(m));
						System.out.println("inside the else if");
					}
					//m2.insert(""+this.content.get(m),pagename+"");
					if(DBApp.canConvertToInt(""+this.content.get(m))) {
						m2.insert((Integer.parseInt(""+this.content.get(m))), pagename);
						System.out.println("inside the first if");
					}
					else {
						m2.insert(""+this.content.get(m), pagename);
						System.out.println("inside the else if");
					}
					//System.out.println(""+this.content.get(m)+"WEWAWEAWEWAWEWAWEWAWEWAWEWAWWEAWAWEWAWEAWEWAWEWAWEWAWEWAWEWAWWEAWAWEWAWEAWEWAWEWAWEWAWEWAWEWAWWEAWA");
					in.close();
					fileIn.close();
					line="";
					FileOutputStream fileOut = new FileOutputStream(treetbr+".class");
					ObjectOutputStream out = new ObjectOutputStream(fileOut);
					out.writeObject(m2);
					out.close();
					fileOut.close();
				}

			}
			m++;




		}reader.close();
	}
	
	
	
	public void update_indexssss(String strTableName,String pagename) throws Exception{
		
		String csvFilePath = "metadata.csv";
		BufferedReader reader = new BufferedReader(new FileReader(csvFilePath));
		  //Enumeration<String> key = htblColNameValue.keys();
		String line;
        int m=0;
        bplustree m2;
        String treetbr;
        while ((line = reader.readLine()) != null) {
            
        	if (line.equals(strTableName)) {
			    	   String[] parts = line.split(",");
			    
			    	   if(parts[5].equals("B+tree")) {
			    		    treetbr=parts[4];

			    		    FileInputStream fileIn = new FileInputStream(treetbr+".ser");
				            ObjectInputStream in = new ObjectInputStream(fileIn);
				            m2 = (bplustree) in.readObject();
				            m2.delete(""+this.content.get(m));
				            m2.insert(""+this.content.get(m),pagename+".ser");
				            in.close();
				            fileIn.close();
				            
				            
			        	    line="";
			        	    
			        	    
			        	    FileOutputStream fileOut = new FileOutputStream(treetbr+".ser");
			                ObjectOutputStream out = new ObjectOutputStream(fileOut);
			                out.writeObject(m2);
			                out.close();
			                fileOut.close();
			        	 }
			    	   
			      }
        	
        		m++;
            }
	}

	 public static boolean canConvertToInt(String str) {
	        try {
	            Integer.parseInt(str);
	            return true; 
	        } catch (NumberFormatException e) {
	            return false; 
	        }
	 }
	public static boolean comparTo(Tuple first, Tuple second, int clusterindex) {
		if (canConvertToInt((String)first.getElement(clusterindex))) {
			//System.out.println("INTEGER YABAAA" +Integer.parseInt((String) first.getElement(clusterindex))+Integer.parseInt((String) (second.getElement(clusterindex))));
			 int firstValue = Integer.parseInt((String) first.getElement(clusterindex));
		     int secondValue = Integer.parseInt((String) second.getElement(clusterindex));
		     return firstValue < secondValue || firstValue == secondValue;
		}
		
		else if (first.getElement(clusterindex) instanceof String) {
			//System.out.println("STRING YABAAA"+first.getElement(clusterindex)+second.getElement(clusterindex));
			if (((String)first.getElement(clusterindex)).compareTo((String)second.getElement(clusterindex))<0) {
				return true;
			}
			else return false;
		}
		else {
			
			System.out.println("erga3 leh comparto 3ashan msh sha8alah");
			return false;
		}
	}
	
	public static boolean eqTo(Tuple first, Tuple second) {
		for(int clusterindex=0;clusterindex<first.content.size();clusterindex++) {
		if (canConvertToInt((String)first.getElement(clusterindex))) {
			 int firstValue = Integer.parseInt((String) first.getElement(clusterindex));
		     int secondValue = Integer.parseInt((String) second.getElement(clusterindex));
		     if(firstValue == secondValue) {
		    	 return true;
		     }
		}
		
		else if (first.getElement(clusterindex) instanceof String) {
			if (((String)first.getElement(clusterindex)).compareTo((String)second.getElement(clusterindex))==0) {
				return true;
			}
		}}
			
			System.out.println("erga3 leh comparto 3ashan msh sha8alah");
			return false;
		}
	
	
	public Hashtable<String, String> getValues() {
		// TODO Auto-generated method stub
		return null;
	}

	/*public Integer getPrimaryKeyValue() {
		String targetPageName ="";
		BufferedReader reader = new BufferedReader(new FileReader("metadata.csv"));

		String line;

		int pos = -1;

		while ((line = reader.readLine()) != null) {
			String[] parts = line.split(",");
			if (parts[0].equals(strTableName)) {
				pos++;
				if(parts[3].equals("True")) {
					clusterIndex = c;
				}
				c++;
				if(htblColNameValue.containsKey(parts[1])) {
					positions.add(pos);
					TheNewValues.add( ""+htblColNameValue.get(parts[1]));
				}

			}
		}

	}
	*/

	
	

	
	
}
