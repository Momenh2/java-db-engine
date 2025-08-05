import java.io.Serializable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.*;

public class Table implements Serializable {
   // private static final long serialVersionUID = 1L;

     String tableName; 
     Vector<String> pageFileNames = new Vector<String>();
     int PageCount=1;
     Vector<String> columnNames = new Vector<String>();
 	static DbappConfig config = new DbappConfig("/C://Users//mokam//git//DB_engine//dbengine//src//DBApp.config/");

     
     public Table(String tableName) {
        this.tableName = tableName;
       
        
    }
     

     public  Vector<String> getPageFilenames() {
    	    
    	 return pageFileNames;
    	  }


    public void addPage() {
    	pageFileNames.add(tableName+""+PageCount+""+".class");
    	PageCount++;
    	//System.out.println(this.pageFileNames.lastElement());
    }
    


    public static void saveTable(Table table) {
        
        try {
            FileOutputStream fileOut = new FileOutputStream(table.tableName+".class");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(table);
            out.close();
            fileOut.close();
            //System.out.println("Serialized data is saved in table.table");
        } catch (IOException i) {
            i.printStackTrace();
        }
    }



    public static Table loadTable(String tableName) {
        Table table=null;
        try {
            FileInputStream fileIn = new FileInputStream(tableName+".class");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            table = (Table) in.readObject();
            in.close();
            fileIn.close();
            return table;
        } catch (IOException i) {
            i.printStackTrace();
            return null;
        } catch (ClassNotFoundException c) {
            System.out.println("Page class not found");
            c.printStackTrace();
            return null;
        }
        
    }
    public static Page findPageWithSpace(String tableName) {
		  // Get the list of page filenames from the Table object
    	
		Table table=loadTable( tableName);
		Vector<String> FileNames = table.pageFileNames; // Assuming a getter method

		  // Check each filename for existence
		  for (String filename : FileNames) {
		    File pageFile = new File(filename);
		    if (pageFile.exists()) {
		      // Load the existing page (implementation not shown)
		      Page page = Page.loadFile(filename);
		      if (!page.isFull()) 
		        return page;
		        
		      }
		    }
		  
    
		table.addPage();
		saveTable(table);
  	  	Page newPage=new Page(table.pageFileNames.lastElement(),config);
  	  	return newPage;
    }
    
    
    
    
    
    public static String namePageWithSpace(String tableName) {
		  // Get the list of page filenames from the Table object
  	
		Table table=loadTable( tableName);
		Vector<String> FileNames = table.pageFileNames; // Assuming a getter method

		  // Check each filename for existence
		  for (String filename : FileNames) {
		    File pageFile = new File(filename);
		    if (pageFile.exists()) {
		      // Load the existing page (implementation not shown)
		      Page page = Page.loadFile(filename);
		      if (!page.isFull()) 
		        return page.Name;
		        
		      }
		    }
		  
  
		table.addPage();
		saveTable(table);
	  	Page newPage=new Page(table.pageFileNames.lastElement(),config);
	  	return newPage.Name;
  }


	public String getnextelement(Page page) {
		int i=pageFileNames.indexOf(page.Name);
		return pageFileNames.get(i+1);
		
	}
	public Boolean isEmpty() {
		if(this.pageFileNames.isEmpty()) {
			return true;
		}
		else {
			return false;
		
		}
	}


	public Hashtable<String, String> getBTrees() {
		// TODO Auto-generated method stub
		return null;
	}


	public Hashtable<String, String> getIndices() {
		// TODO Auto-generated method stub
		return null;
	}


	public Integer getSize() {
		// TODO Auto-generated method stub
		return this.pageFileNames.size();
	}


	public Vector getPageNames() {
		
		return this.getPageNames();
	}


	public Page getPageAtPosition(int i) {
		
		return Page.loadFile(this.pageFileNames.get(i));
		///////////////////////////////////////////////////////////////////////////////check/////////////////////
	}
	public int getColumnIndex(String columnName) throws DBAppException {
		System.out.println(columnNames.size());
		for (int i = 0; i < columnNames.size(); i++) {
			System.out.println(columnNames.get(i));
			
            if (columnNames.get(i).equals(columnName)) {
                return i;
            }
        }
        throw new DBAppException("Column does not exist: " + columnName);
    }
    
    
		  
		}

