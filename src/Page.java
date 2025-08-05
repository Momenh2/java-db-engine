import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;
import java.util.Vector;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class Page implements Serializable,ConfigChangeListener {

	Vector<Tuple> tuples = new Vector <Tuple>();
	String Name;
	int MaximumRowsCountinPage; // Maximum number of rows/tuples in the page
	int count;
	static DbappConfig config = new DbappConfig("/C://Users//mokam//git//DB_engine//dbengine//src//DBApp.config/");

	/*public Page(String Name,int maxRows) {
		this.Name=Name;
		this.MaximumRowsCountinPage = maxRows;
		this.count=0;

	}*/
	public Page(String Name, DbappConfig config) {
		this.Name=Name;
		//	this.config = config;
		this.MaximumRowsCountinPage = Integer.parseInt(config.getProperty("MaximumRowsCountinPage"));
		this.count=0;
		config.addConfigChangeListener(this);
	}
	@Override
	public void onConfigChange(String key, String value) {
		if (key.equals("MaximumRowsCountinPage")) {
			this.MaximumRowsCountinPage = Integer.parseInt(value);
			// Handle attribute updates or take necessary actions
		}


	}

	// Method to add a tuple to the page
	public boolean addTuple(Tuple tuple) {
		if (tuples.size() < MaximumRowsCountinPage) {
			tuples.add(tuple);
			count++;
			return true;
		}
		return false; // Page is full
	}

	// Override toString method to return the tuples in the page as a string
	@Override
	public String toString() {
		String res = "";

		for(int i =0; i<tuples.size();i++) {
			res+=tuples.get(i).toString();

			System.out.println();

			if(i==tuples.size()-1)
				break;
			else
				res+=",,";
		}
		return res;
	}
	public void increment() {
		count++;

	}

	public void decrement() {
		count--;
	}  


	public boolean isFull() {
		return count == MaximumRowsCountinPage;
	}


	public boolean isEmpty() {
		return count==0;
	}




	public void shiftSSS(Tuple rr,Table t) throws Exception {
		// make change to tuples. 
		// Shift the elements to the right by 1 position. left most elements goes to the left most pos.


		if (this.isFull()) {
			System.out.println("ANANANANANA HEENNNNANAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
			Tuple temp=tuples.get(count-1);

			tuples.set(count-1, rr);
			Tuple lastElement = tuples.remove(tuples.size() - 1); 
			tuples.add(0, lastElement);
			rr.update_index(t.tableName, Name); 
			String pagenext=t.getnextelement(this);
			if(DBApp.isSerializedObjectExists(pagenext+".class")){
				Page next=loadFile(pagenext);
				next.shiftSSS(temp, t);
				Page.saveFile(next);
			}

			else {
				t.addPage();
				Page newer=new Page(t.pageFileNames.lastElement(),config);
				newer.addTuple(rr);
				Page.saveFile(newer);

			}


			Page.saveFile(this);
		}
		else {
			tuples.add(0,rr);
			Page.saveFile(this);
		}
	}


	public void shift(Tuple rr,Table t) throws Exception {
		// make change to tuples. 
		// Shift the elements to the right by 1 position. left most elements goes to the left most pos.
		//System.out.println("Shifting tuple:"+rr.toString());
		if(this.isFull()) {
			Tuple temp = this.tuples.lastElement();
			this.tuples.remove(this.count-1);
			this.tuples.add(0,rr);
			rr.update_index(t.tableName, this.Name);
			if ((t.pageFileNames.lastElement()).equals(this.Name)){
				t.addPage();
				Page newer =new Page(t.pageFileNames.lastElement(),config);
				newer.addTuple(temp);
				temp.update_index(t.tableName, newer.Name);
				Page.saveFile(newer);
				Page.saveFile(this);
			}else {
				//t.addPage();
				Page newer=Page.loadFile(t.getnextelement(this));
				System.out.println(newer.Name+"WEWAWEAWEWAWEWAWEWAWEWAWEWAWWEAWAWEWAWEAWEWAWEWAWEWAWEWAWEWAWWEAWAWEWAWEAWEWAWEWAWEWAWEWAWEWAWWEAWA");
				newer.shift(temp, t);
				temp.update_index(t.tableName, newer.Name);
				Page.saveFile(this);
			}

		}else {
			this.tuples.add(0,rr);
			this.count++;
			Page.saveFile(this);



		}

	}










	public static void saveFile(Page page) {

		try {
			FileOutputStream fileOut = new FileOutputStream(page.Name);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(page);
			out.close();
			fileOut.close();
			//System.out.println("Serialized data is saved in page.class");
		} catch (IOException i) {
			i.printStackTrace();
		}
	}




	public static Page loadFile(String pageName) {
		Page page=null;
		try {
			FileInputStream fileIn = new FileInputStream(pageName);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			page = (Page) in.readObject();
			in.close();
			fileIn.close();
			return page;
		} catch (IOException i) {
			i.printStackTrace();
			return null;
		} catch (ClassNotFoundException c) {
			//System.out.println("Page class not found");
			c.printStackTrace();
			return null;
		}

	}

	public Tuple getTuples(int i) {
		return tuples.get(i);
	}



}	    





