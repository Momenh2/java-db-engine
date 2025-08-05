import java.util.*;
import java.awt.AWTException;
import java.io.*;


public class DBApp {

	boolean flag1 =false;

	private boolean flag2=false;

	private static final String Break = null;

	static DbappConfig config = new DbappConfig("C://Users//mokam//git//DB_engine//dbengine//src//DBApp.config/");

	public DBApp( ){

	}

	// this does whatever initialization you would like 
	// or leave it empty if there is no code you want to 
	// execute at application startup 
	public void init(){


	}


	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data 
	// type as value
	public void createTable(String strTableName, 
            String strClusteringKeyColumn,
            Hashtable<String,String> htblColNameType) throws DBAppException{
        try {
            String filePath = "metadata.csv";
            File metadataFile = new File(filePath);
            if (!metadataFile.exists()) {
                FileWriter writer = new FileWriter(metadataFile);
                System.out.println("metadata.csv created successfully.");
            } else {
                System.out.println("metadata.csv already exists.");
            }
            BufferedReader reader2 = new BufferedReader(new FileReader(metadataFile));
            String line;
            while ((line = reader2.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts[0].equals(strTableName)) {
                    throw new DBAppException("table already exist");

                }
            }
            reader2.close();
Table newe=new Table(strTableName);
            String str = "";
            Enumeration<String> key = htblColNameType.keys();
            while (key.hasMoreElements()) {
                str = "";
                str += strTableName;
                str += ",";
                String keyy = key.nextElement();
                newe.columnNames.add(keyy);
                System.out.println(keyy+"ana el key");
                //System.out.println(key.toString());
                str += keyy;
                str += ",";
                str += htblColNameType.get(keyy);
                if(keyy == strClusteringKeyColumn) 
                    str +=",True";
                else 
                    str +=",False";
                str += ",";
                str += "null";
                str += ",";
                str += "null";
                BufferedWriter writer = new BufferedWriter(new FileWriter("metadata.csv",true));
                writer.write(str); 
                writer.newLine();

                if (writer != null) {
                    writer.flush();
                    writer.close();
                }


            }
            Table.saveTable(newe);
        }catch(Exception e) {
            throw new DBAppException("");
        }

    }

	/////////////////////////////////////////////////////////////////////////////////////
	// following method creates a B+tree index 
	public void createIndex(String strTableName,String strColName,String strIndexName) throws DBAppException{
		try{
			Table table=Table.loadTable(strTableName);
			if(!checkIfTableExists(strTableName))
				throw new DBAppException("The table does not exist");
			else {



				String csvFilePath = "metadata.csv";
				//int lineToUpdate = 3; // specify the line number to update (starting from 0)

				//String newData = "new,data,to,replace"; // new data to replace the line with

				//try {
				BufferedReader reader = new BufferedReader(new FileReader(csvFilePath));
				List<String> Alllines = new ArrayList<>();

				String line;
				int m=0;
				int fm=0;
				//int currentLine = 0;
				while ((line = reader.readLine()) != null) {
					String[] parts = line.split(",");
					if (parts[0].equals(strTableName)) {
						m++;
						if(parts[1].equals(strColName)) {
							parts[4]=strIndexName;
							parts[5]="B+tree";
							line="";
							fm=m;
							for(String i : parts) {
								line+=i;
								line+=",";
							}
						}
					}

					Alllines.add(line);
				}
				reader.close();
				PrintWriter writer = new PrintWriter(new FileWriter(csvFilePath));
				for (String updatedLine : Alllines) {
					writer.println(updatedLine);
				}
				writer.close();
				bplustree newe=new bplustree(5);
				int i1=1;
				String i2=i1+"";
				if (isSerializedObjectExists(strTableName+i2+".class")) {

					fm--;
					Page pager = Page.loadFile(strTableName+i1+".class");
					for (int j=0;j<table.pageFileNames.size();j++) {
						//pager.tuples
						System.out.println(table.pageFileNames.get(j));
						if (isSerializedObjectExists(table.pageFileNames.get(j))) {
							pager = Page.loadFile(table.pageFileNames.get(j));
							Tuple tupler;

							for(int i=0;i<pager.count;i++) {
								tupler=pager.tuples.get(i);
								System.out.println(tupler.toString());
								System.out.println(tupler.content.get(fm)+"rpomfrpmfplmwplf");
								System.out.println(DBApp.canConvertToInt(tupler.content.get(fm)));

								//System.out.println("page number"+i2);
								if(DBApp.canConvertToInt(""+tupler.content.get(fm)) ) {
									newe.insert((Integer.parseInt(""+ tupler.content.get(fm))), table.pageFileNames.get(j));
									System.out.println("inside the first if");
								}
								else {
									newe.insert(""+tupler.content.get(fm), table.pageFileNames.get(j));
									System.out.println("inside the else if");
								}


							}}
						i1++;
						i2=i1+"";



					}
				}

				FileOutputStream fileOut = new FileOutputStream(strIndexName+".class");
				ObjectOutputStream out = new ObjectOutputStream(fileOut);

				newe.print();
				out.writeObject(newe);
				out.close();
				fileOut.close();
				//System.out.println("bplusstree created");
				for(String i:table.pageFileNames) {
					Page pager=Page.loadFile(i);
					for(Tuple tupler:pager.tuples) {
						if (newe.search(""+tupler.getElement(fm)) != null) {
							System.out.print("Found");
						} else {
							System.out.println("Not Found");
						}
						;
					}
				}
				//} catch (Exception e) {
				//System.err.println("Error updating CSV file: " + e.getMessage());

				//}









			}







		}catch(Exception e) {
			//throw new DBAppException("ay kalam fady ma32ol");
			System.err.println(e.getMessage());
		}



	}



	//////////////////////////////////////////////////////////


	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws Exception {	  
		//try {
		flag2=false;
		flag1=false; 
		if (!checkIfTableExists(strTableName)) {
			return;
		}
		Table table=Table.loadTable(strTableName);
		String targetPage = "" ;

		Tuple newTuple=new Tuple();


		//////////////////////////////////////////////////////////////////////
		int counter=0;
		int findexcluster=0;
		String csvFilePath = "metadata.csv";
		BufferedReader reader = new BufferedReader(new FileReader(csvFilePath));
		String line;
		while ((line = reader.readLine()) != null) {
			String[] parts = line.split(",");
			if (parts[0].equals(strTableName)) {

				if(parts[3].equals("True")) {
					findexcluster=counter;
					System.out.println("clusterkey found:"+findexcluster);
				}
				counter++;
				newTuple.content.add(String.valueOf(htblColNameValue.get(parts[1])));
			}
		}
		System.out.println(newTuple.toString());
		reader.close();

		bplustree m2;
		String treetbr;

		int i1=1;																									
		if (isSerializedObjectExists(strTableName+i1+".class")) {
			Page pager = Page.loadFile(strTableName+i1+".class");
			String t1=i1+"";
			for(int j=0;j<table.pageFileNames.size();j++) {
				String pagename=table.pageFileNames.get(j);
				pager = Page.loadFile(pagename);  																								
				int i=0;
				if (flag1==false){
					for(Tuple tupler:pager.tuples) {																			
						tupler=pager.tuples.get(i);
						if(Tuple.comparTo(pager.tuples.lastElement(),newTuple,findexcluster)) {
							break;
						}
						if((Tuple.comparTo(newTuple,tupler,findexcluster)) && newTuple!=tupler) {  // || second or for new page creation
							t1=i1+"";
							if(pager.count==pager.MaximumRowsCountinPage && flag1==false) {
								//System.out.println("newTuple"+newTuple.toString());
								Tuple temp = pager.tuples.lastElement();
								pager.tuples.remove(pager.tuples.lastElement());
								pager.tuples.add(i,newTuple);
								flag1=true;
								if (!isSerializedObjectExists(strTableName+(i1+1)+".class")) {///////////
									table.addPage();
									Page newer =new Page(table.pageFileNames.lastElement(),config);
									newer.addTuple(temp);
									targetPage=pager.Name;
									Page.saveFile(newer);
									Page.saveFile(pager);

									Table.saveTable(table);
								}else {
									Page newer=Page.loadFile(table.pageFileNames.get(j+1));
									targetPage=pager.Name;
									newer.shift(temp, table);
									Page.saveFile(pager);
									Table.saveTable(table);
									//flag2=true;
								}
								break;
							}
							else {
								pager.tuples.add(i,newTuple);
								flag1=true;
								targetPage=pager.Name;
								pager.count++;
								Page.saveFile(pager);
								break;
							}
						}

						i++;
					}
					i1++;

					if(pager.Name.equals(table.pageFileNames.lastElement()) && flag1==false) {
						if(pager.count==pager.MaximumRowsCountinPage) {
							flag1=true;
							table.addPage();
							Page newer =new Page(table.pageFileNames.lastElement(),config);
							newer.addTuple(newTuple);
							targetPage=pager.Name;
							Page.saveFile(newer);
							Page.saveFile(pager);

							Table.saveTable(table);
							break;
						}
						else {
							pager.tuples.add(newTuple);
							flag1=true;
							targetPage=pager.Name;
							pager.count++;
							Page.saveFile(pager);
							break;
						}
					}

				}


			}


			/////////////////////////////////////////////////////////////////////////

			BufferedReader reader1 = new BufferedReader(new FileReader(csvFilePath));
			while ((line = reader1.readLine()) != null & flag2==false) {
				String[] parts = line.split(",");
				if (parts[0].equals(strTableName)) {
					if(parts[5].equals("B+tree")) {
						treetbr=parts[4];
						//htblColNameValue.get(key)

						FileInputStream fileIn = new FileInputStream(treetbr+".class");
						ObjectInputStream in = new ObjectInputStream(fileIn);
						m2 = (bplustree) in.readObject();
						System.out.print(treetbr+"");
						//m2.insert(""+htblColNameValue.get(parts[1]),targetPage);
						if(DBApp.canConvertToInt(""+htblColNameValue.get(parts[1]))) {
							m2.insert((Integer.parseInt(""+htblColNameValue.get(parts[1]))), targetPage);
							System.out.println("inside the first if");
						}
						else {
							m2.insert(""+htblColNameValue.get(parts[1]), targetPage);
							System.out.println("inside the else if");
						}
						System.out.println(""+htblColNameValue.get(parts[1])+"WEWAWEAWEWAWEWAWEWAWEWAWEWAWWEAWAWEWAWEAWEWAWEWAWEWAWEWAWEWAWWEAWAWEWAWEAWEWAWEWAWEWAWEWAWEWAWWEAWA");
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




			}reader1.close();
		}
		else {

			System.out.println("first insert ever");
			Table r=Table.loadTable(strTableName);
			r.addPage();
			Page newPage=new Page( r.pageFileNames.get(0) , config);
			newPage.tuples.add(newTuple);
			newPage.count++;
			Page.saveFile(newPage);
			Table.saveTable(r);


			BufferedReader reader2 = new BufferedReader(new FileReader(csvFilePath));
			while ((line = reader2.readLine()) != null) {
				String[] parts = line.split(",");
				if (parts[0].equals(strTableName)) {


					//newTuple.content.add((String)htblColNameValue.get(parts[1]));
					System.out.println("miknkrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr"+parts[5].equals("B+tree"));
					if(parts[5].equals("B+tree")) {
						//String indexinhash = ((Enumeration<String>)htblColNameValue).nextElement();
						treetbr=parts[4];
						//htblColNameValue.get(key)
						FileInputStream fileIn = new FileInputStream(treetbr+".class");
						ObjectInputStream in = new ObjectInputStream(fileIn);
						m2 = (bplustree) in.readObject();
						//m2.insert(""+htblColNameValue.get(parts[1]),targetPage);
						if(DBApp.canConvertToInt(""+htblColNameValue.get(parts[1]))) {
							m2.insert((Integer.parseInt(""+htblColNameValue.get(parts[1]))), targetPage);
							System.out.println("inside the first if");
						}
						else {
							m2.insert(""+htblColNameValue.get(parts[1]), targetPage);
							System.out.println("inside the else if");
						}
						//System.out.print(treetbr+"_-_------------------------------------------------------------");
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




			}reader2.close();



		}

		for(String teb:table.pageFileNames) {
			Page tom=Page.loadFile(teb);
			System.out.println(tom.tuples.toString()+"page :"+teb+"COUNT"+tom.count+"MAX ROWS"+tom.MaximumRowsCountinPage);

		}


		//	}catch(Exception e) {
		//	throw new DBAppException("");
		//}

	}


	public void insert (bplustree t,Object o,String p) {
		if(DBApp.canConvertToInt(""+o)) {
			t.insert(0, p);

		}
		else {
			t.insert(""+0, p);
		}


	}





	public void insertIntoTabless(String strTableName, Hashtable<String, Object> htblColNameValue) throws Exception {	  
		flag1=false; 
		if (!checkIfTableExists(strTableName)) {
			return;
		}
		Table table=Table.loadTable(strTableName);

		//String primaryKey = getTablePrimaryKey(strTableName); 
		// if (!htblColNameValue.containsKey(primaryKey)) {
		// return;
		//}
		//in the insert what if the columns were out of order
		Page targetPage =Table.findPageWithSpace(strTableName);

		//Tuple newTuple = createTupleFromHashtable(htblColNameValue);
		Tuple newTuple=new Tuple();
		//targetPage.addTuple(newTuple);
		//targetPage.increment();
		//Page.saveFile(targetPage);



		String clust="";
		int counter=0;
		int findexcluster=0;

		//////////////////////////////////////////////////////////////////////

		String csvFilePath = "metadata.csv";
		BufferedReader reader = new BufferedReader(new FileReader(csvFilePath));
		//Enumeration<String> key = htblColNameValue.keys();
		String line;
		//int m=0;

		while ((line = reader.readLine()) != null) {
			String[] parts = line.split(",");
			if (parts[0].equals(strTableName)) {

				if(parts[3].equals("True")) {
					clust=parts[1];
					findexcluster=counter;
					System.out.println("clusterkey found:"+findexcluster);
					//System.out.println(counter);

				}
				counter++;
				newTuple.content.add(String.valueOf(htblColNameValue.get(parts[1])));


			}


		}
		System.out.println(newTuple.toString());

		reader.close();

		bplustree m2;
		String treetbr;

		///////////////////////////////////////////////////
		///////////////////////////////////////////////////////////////////////////////////////
		// |_) |_  \	  / |_) | + |_   |_) |_  \    / |_) | + |_   |_) |_  \    / |_) | + |_ //
		// | \ |_   \/\/  | \ | | |  _   | \ |_   \/\/  | \ | | |_   | \ |_   \/\/  | \ | | |_ //
		///////////////////////////////////////////////////////////////////////////////////////
		//targetPage.addTuple(newTuple);
		int i1=1;
		//System.out.println(strTableName+i1+".class");
		if (isSerializedObjectExists(strTableName+i1+".class")) {
			//System.out.println("inside the if");
			Page pager = Page.loadFile(strTableName+i1+".class");
			//System.out.print("the size of the page is:"+pager.count);
			Boolean flag=false;
			String t1=i1+"";
			//System.out.print(table.pageFileNames.toString()+"ffffffffffffffffffffffffffffffffff");
			for(int j=0;j<table.pageFileNames.size();j++) {
				String pagename=table.pageFileNames.get(j);
				pager = Page.loadFile(pagename);  																								//System.out.println("inside the while");
				int i=0;
				//int pagelength=pager.count;

				if (flag1==false){
					for(Tuple tupler:pager.tuples) {																					//System.out.println("inside the for loopsssssssssssssss");
						tupler=pager.tuples.get(i);
						if((Tuple.comparTo(newTuple,tupler,findexcluster) && newTuple!=tupler)||(pager.count-i==1 && pager.Name.equals(table.pageFileNames.lastElement()))||(pager.count<pager.MaximumRowsCountinPage && pager.Name.equals(table.pageFileNames.lastElement()))) {  //second or for new page creation
							t1=i1+"";
							System.out.println("MAX ROWWWWSSSSS"+pager.Name);
							//System.out.println("MAX COUNTTTTT"+pager.count);
							if(pager.count==pager.MaximumRowsCountinPage) {
								pager.tuples.add(i, newTuple);
								Tuple temp=pager.tuples.remove(pager.MaximumRowsCountinPage-1);
								int gf = Integer.parseInt(t1);
								gf++;
								String secondpageindex=gf+"";
								if(!isSerializedObjectExists(strTableName+secondpageindex+".class")){
									table.addPage();
									Page newer=new Page(table.pageFileNames.lastElement(),config);
									//System.out.print(table.pageFileNames.lastElement());

									Page.saveFile(newer); 	
								}
								System.out.print("page name:"+pager.Name+"Page size:"+pager.MaximumRowsCountinPage);
								Page.loadFile(strTableName+secondpageindex+".class").shift(temp, table);
								Page.saveFile(pager);
								Table.saveTable(Table.loadTable(strTableName));

								//System.out.println("CSV file updated successfully.");
								Page.saveFile(pager);
								flag1=true;
								break;

							}
							else {
								pager.tuples.add(i,newTuple);
								pager.count++;
								flag=true;
								Page.saveFile(pager);
								System.out.print("page name:"+pager.Name+"Page size:"+pager.MaximumRowsCountinPage);
								flag1=true;
								break;
							}
						}

						i++;
					}
					i1++;
				}}
			/*
          if (isSerializedObjectExists(strTableName+t1+".class")) {
      		pager = Page.loadFile(strTableName+t1+".class");
      		System.out.println("loaded page:"+t1+".class");
		  }else{
      			System.out.print("p1");
      			pager=null;
      			int s1=Integer.parseInt(t1);
      			s1--;
      			t1=s1+"";
      			System.out.println("not inserted yet page null");
      		}

          	if(pager==null&&flag==false&&(Page.loadFile(strTableName+t1+".class").count)==(Page.loadFile(strTableName+t1+".class").MaximumRowsCountinPage)) {
          		Table r=Table.loadTable(strTableName);
          		r.addPage();
          		//////////////
          		Page newPage=new Page(r.pageFileNames.lastElement(),200);
          		newPage.tuples.add(newTuple);
          		Page.saveFile(newPage);
          		Table.saveTable(r);
          		System.out.println("new page created");
          	}
          	else if(pager==null&&flag==false&&(Page.loadFile(strTableName+t1+".class").count)<(Page.loadFile(strTableName+t1+".class").MaximumRowsCountinPage)){
          		Page.loadFile(strTableName+t1+".class").addTuple(newTuple);
          		Page.saveFile(Page.loadFile(strTableName+t1+".class"));
          		System.out.println("inserted into previous");
          	}*/




			/////////////////////////////////////////////////////////////////////////
			BufferedReader reader1 = new BufferedReader(new FileReader(csvFilePath));
			while ((line = reader1.readLine()) != null) {
				String[] parts = line.split(",");
				if (parts[0].equals(strTableName)) {


					//newTuple.content.add((String)htblColNameValue.get(parts[1]));
					if(parts[5].equals("B+tree")) {
						//String indexinhash = ((Enumeration<String>)htblColNameValue).nextElement();
						treetbr=parts[4];
						//htblColNameValue.get(key)
						FileInputStream fileIn = new FileInputStream(treetbr+".class");
						ObjectInputStream in = new ObjectInputStream(fileIn);
						m2 = (bplustree) in.readObject();
						m2.insert(""+htblColNameValue.get(parts[1]),targetPage.Name+".class");
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




			}}
		else {

			System.out.println("first insert ever");
			Table r=Table.loadTable(strTableName);
			System.out.println(r.pageFileNames.get(0)+"                   eeee");
			//////////////
			Page newPage=new Page(r.pageFileNames.get(0),config);
			//System.out.println("the seralized page name:"+r.pageFileNames.lastElement());
			newPage.tuples.add(newTuple);
			newPage.count++;
			Page.saveFile(newPage);
			//r.addPage();
			Table.saveTable(r);
			//System.out.println("new page created");


			BufferedReader reader2 = new BufferedReader(new FileReader(csvFilePath));
			while ((line = reader2.readLine()) != null) {
				String[] parts = line.split(",");
				if (parts[0].equals(strTableName)) {


					//newTuple.content.add((String)htblColNameValue.get(parts[1]));
					if(parts[5].equals("B+tree")) {
						//String indexinhash = ((Enumeration<String>)htblColNameValue).nextElement();
						treetbr=parts[4];
						//htblColNameValue.get(key)
						FileInputStream fileIn = new FileInputStream(treetbr+".class");
						ObjectInputStream in = new ObjectInputStream(fileIn);
						m2 = (bplustree) in.readObject();
						m2.insert(""+htblColNameValue.get(parts[1]),targetPage.Name+".class");
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




			}



		}

		for(String teb:table.pageFileNames) {
			Page tom=Page.loadFile(teb);
			System.out.println(tom.tuples.toString()+"page :"+teb+"COUNT"+tom.count+"MAX ROWS"+tom.MaximumRowsCountinPage);

		}



	}





	//////************///////////////////****************/////////////////*****************////////////

	public boolean checkIfTableExists(String tableName) {

		BufferedReader reader = null;
		String line;
		try {
			reader = new BufferedReader(new FileReader("metadata.csv"));
			while ((line = reader.readLine()) != null) {
				String[] parts = line.split(",");
				if (parts[0].equals(tableName)) {
					//System.out.println("mawgoda");
					return true;
				}
			}
		} catch (IOException e) {
			// Ignoring errors for now
		} 
		finally {
			try {
				if (reader != null) reader.close();
			} catch (IOException e) {
				// Ignoring errors for now
			}
		}
		return false;
	}

	public String getTablePrimaryKey(String tableName) {
		// Read metadata file (.csv) and find the primary key for the table
		BufferedReader reader = null;
		String line;
		try {
			reader = new BufferedReader(new FileReader("metadata.csv"));
			while ((line = reader.readLine()) != null) {
				if (line.equals(tableName)) {
					String[] parts = line.split(",");
					if (parts[3].equals("True")) {
						return parts[1]; // Primary key is in the second column
					}
				}
			}
		} catch (IOException e) {
			// Ignoring errors for now
		} finally {
			try {
				if (reader != null) reader.close();
			} catch (IOException e) {
				// Ignoring errors for now
			}
		}
		return null; // Table might not exist or primary key not found
	}





	private Tuple createTupleFromHashtable(Hashtable<String, Object> htblColNameValue) {
		// Map column-value pairs to a new Tuple object
		Tuple newTuple = new Tuple();
		for (String key : htblColNameValue.keySet()) {
			newTuple.content.add(""+ htblColNameValue.get(key));
		}
		return newTuple;

	}

	public Page ReturnPage(String strTableName, String strClusteringKeyValue,String TRname ) throws Exception {
        String treeName=TRname;
        FileInputStream fileIn = new FileInputStream(treeName+".class");
        ObjectInputStream in = new ObjectInputStream(fileIn);
        bplustree tree = (bplustree) in.readObject();
        String pageNAme = tree.search(strClusteringKeyValue);
        Page targetpage = Page.loadFile(pageNAme);
        in.close();
        fileIn.close();
        String line="";
        FileOutputStream fileOut = new FileOutputStream(treeName+".class");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(tree);
        out.close();
        fileOut.close();


        return targetpage;
    }












	public void updateTable(String strTableName, String strClusteringKeyValue,Hashtable<String,Object> htblColNameValue)  throws DBAppException{
		//sPage TargetPage;
		try {
			Page TargetPage=null;
			//Page TargetPage;
			Table table=Table.loadTable(strTableName); //The Table
			Tuple TheOldTuple = new Tuple(); // Tuple that is going to be updated

			int clusterIndex =0;
			int c=0;
			int IntPageCounter = 1;
			boolean flag = false;
			ArrayList<String> TheNewValues = new ArrayList<String>();
			ArrayList<Integer> positions = new ArrayList<Integer>();

			String targetPageName ="";
			BufferedReader reader = new BufferedReader(new FileReader("metadata.csv"));

			String line;

			int pos = -1;
			//	Page TargetPage;
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
					if(parts[3].equals("True") && parts[5].equals("B+tree")){
						flag=true;
						TargetPage= ReturnPage(strTableName,strClusteringKeyValue,parts[4]);  
						System.out.print("mihaw");
					}
				}
			}
			reader.close();
			if(!flag){
				if (isSerializedObjectExists(strTableName+IntPageCounter+".class")) {
					Page Current_page = Page.loadFile(strTableName+IntPageCounter+".class");
					Boolean Found=false;
					for(int j=0;j<table.PageCount-1;j++) {////////looping on the page
						String pagename=table.pageFileNames.get(j); 
						Current_page = Page.loadFile(pagename);  	
						System.out.println(Current_page.Name);
						for(Tuple Current_Tuple:Current_page.tuples) {// looping on the tuples in the page	
							System.out.println(Current_Tuple.toString());
							if(DBApp.AreEqual(Current_Tuple.content.get(clusterIndex),strClusteringKeyValue)){  
								for(int t=0;t<Current_Tuple.content.size();t++)
									TheOldTuple.content.add(Current_Tuple.content.get(t)); //save the old tuple

								for(int same =0; same<positions.size() ;same++) 
									Current_Tuple.content.set(positions.get(same),TheNewValues.get(same));
								targetPageName = Current_page.Name;
								Found = true;
								Page.saveFile(Current_page);
								Table.saveTable(Table.loadTable(strTableName));
								Page.saveFile(Current_page);
								break;
							}
							Page.saveFile(Current_page);
							if(Found) break;
						}
						Page.saveFile(Current_page);
						IntPageCounter++;
					}
					// <<<<<<<<<<<<FASEL>>>>>>>>>>>>>>>>

					if(!Found) throw new DBAppException("The tuple to be updated is not found");
					bplustree tree;
					String treeName;
					int In_tuple_counter =-1;
					BufferedReader reader1 = new BufferedReader(new FileReader("metadata.csv"));
					while ((line = reader1.readLine()) != null) {
						String[] parts = line.split(",");
						if (parts[0].equals(strTableName)) {

							In_tuple_counter++;
							if(parts[5].equals("B+tree")&& positions.contains(In_tuple_counter)) { //to remove and put only the updated ones
								treeName=parts[4];
								FileInputStream fileIn = new FileInputStream(treeName+".class");
								ObjectInputStream in = new ObjectInputStream(fileIn);
								tree = (bplustree) in.readObject();
								//tree.delete(""+TheOldTuple.content.get(In_tuple_counter));
								if(DBApp.canConvertToInt(""+TheOldTuple.content.get(In_tuple_counter))) {
									tree.delete((Integer.parseInt(""+TheOldTuple.content.get(In_tuple_counter))));
									System.out.println("inside the first if");
								}
								else {
									tree.delete(""+TheOldTuple.content.get(In_tuple_counter));
									System.out.println("inside the else if");
								}
								tree.insert(""+htblColNameValue.get(parts[1]),targetPageName+".class");
								in.close();
								fileIn.close();
								line="";
								FileOutputStream fileOut = new FileOutputStream(treeName+".class");
								ObjectOutputStream out = new ObjectOutputStream(fileOut);
								out.writeObject(tree);
								out.close();
								fileOut.close();
							}
						}
					}
				}else{
					throw new DBAppException("The Table is Empty");
				}
			}
			else {
				System.out.println("wahim");
				for(Tuple Current_Tuple:TargetPage.tuples) {	
					System.out.println(Current_Tuple.toString());
					if(DBApp.AreEqual(Current_Tuple.content.get(clusterIndex),strClusteringKeyValue)){  
						for(int t=0;t<Current_Tuple.content.size();t++)
							TheOldTuple.content.add(Current_Tuple.content.get(t)); //save the old tuple
						for(int same =0; same<positions.size() ;same++) {
							System.out.println(Current_Tuple.content.toString()+"dah ely hayt5ayar");
							Current_Tuple.content.set(positions.get(same),TheNewValues.get(same));}
						targetPageName = TargetPage.Name;
						//Found = true;
						Page.saveFile(TargetPage);
						Table.saveTable(Table.loadTable(strTableName));
						Page.saveFile(TargetPage);
						break;
					}}
				bplustree tree;
				String treeName;
				int In_tuple_counter =-1;
				BufferedReader reader1 = new BufferedReader(new FileReader("metadata.csv"));
				while ((line = reader1.readLine()) != null) {
					String[] parts = line.split(",");
					if (parts[0].equals(strTableName)) {
						In_tuple_counter++;
						if(parts[5].equals("B+tree")&& positions.contains(In_tuple_counter)) { //to remove and put only the updated ones
							treeName=parts[4];
							FileInputStream fileIn = new FileInputStream(treeName+".class");
							ObjectInputStream in = new ObjectInputStream(fileIn);
							tree = (bplustree) in.readObject();
							//tree.delete(""+TheOldTuple.content.get(In_tuple_counter));
							if(DBApp.canConvertToInt(""+TheOldTuple.content.get(In_tuple_counter))) {
								tree.delete((Integer.parseInt(""+TheOldTuple.content.get(In_tuple_counter))));
								System.out.println("inside the first if");
							}
							else {
								tree.delete(""+TheOldTuple.content.get(In_tuple_counter));
								System.out.println("inside the else if");
							}
							tree.insert(""+htblColNameValue.get(parts[1]),targetPageName+".class");
							in.close();
							fileIn.close();
							line="";
							FileOutputStream fileOut = new FileOutputStream(treeName+".class");
							ObjectOutputStream out = new ObjectOutputStream(fileOut);

							out.writeObject(tree);
							out.close();
							fileOut.close();
						}
					}
				}
			}
		}catch(Exception e) {
			System.err.println(e.getMessage());
			throw new DBAppException("Update");

		}
	}







	public void DeleteItsbplustreeElements(Tuple TupleToBeReplaced, String strTableName) throws Exception {/// Helper Method 


		String line;
		bplustree tree;
		String treeName;

		int In_tuple_counter =-1;

		BufferedReader reader1 = new BufferedReader(new FileReader("metadata.csv"));

		while ((line = reader1.readLine()) != null) {
			String[] parts = line.split(",");
			if (parts[0].equals(strTableName)) {

				In_tuple_counter++;
				if(parts[5].equals("B+tree")) {

					treeName=parts[4];

					FileInputStream fileIn = new FileInputStream(treeName+".class");
					ObjectInputStream in = new ObjectInputStream(fileIn);

					tree = (bplustree) in.readObject();

					//tree.delete(""+TupleToBeReplaced.content.get(In_tuple_counter));
					if(DBApp.canConvertToInt(""+TupleToBeReplaced.content.get(In_tuple_counter))) {
						tree.delete((Integer.parseInt(""+TupleToBeReplaced.content.get(In_tuple_counter))));
						System.out.println("inside the first if");
					}
					else {
						tree.delete(""+TupleToBeReplaced.content.get(In_tuple_counter));
						System.out.println("inside the else if");
					}



					in.close();
					fileIn.close();

					line="";

					FileOutputStream fileOut = new FileOutputStream(treeName+".class");
					ObjectOutputStream out = new ObjectOutputStream(fileOut);

					out.writeObject(tree);
					out.close();
					fileOut.close();
				}

			}



		}




	}





	public static boolean canConvertToInt(Object str) {
		try {
			Integer.parseInt( str+""
					);
			return true; 
		} catch (NumberFormatException e) {
			return false; 
		}
	}



	public static boolean canConvertToFloat(Object str) {
		try {
			Float.parseFloat(str + "");
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}


	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search 
	// to identify which rows/tuples to delete. 	
	// htblColNameValue enteries are ANDED together 

	public static void viewall(String strTableNam) {
		Table table=Table.loadTable(strTableNam);
		System.out.println(table.pageFileNames.toString());
		for(String teb:table.pageFileNames) {
			Page tom=Page.loadFile(teb);
			System.out.println(tom.tuples.toString()+"page :"+teb+"COUNT"+tom.count+"MAX ROWS"+tom.MaximumRowsCountinPage);

		}
	}

	public ArrayList<Page> ReturnPages(String strTableName, String strKeyValue,String TRname ) throws IOException, ClassNotFoundException{
        ArrayList<Page> Pages = new ArrayList<Page>();
        ArrayList<String> values = new ArrayList<String>(); //pagenames


          String treeName=TRname;

            FileInputStream fileIn = new FileInputStream(treeName+".class");
            ObjectInputStream in = new ObjectInputStream(fileIn);

            bplustree tree = (bplustree) in.readObject();

            //tree.delete(""+TheOldTuple.content.get(In_tuple_counter));
            while(tree.search(strKeyValue)!=null) {
             //tree.insert(""+htblColNameValue.get(parts[1]),targetPageName+".class");
            String pageName = tree.search(strKeyValue); 
            Page targetpage = Page.loadFile(pageName);

            Pages.add(targetpage);

            values.add(pageName);

            if(DBApp.canConvertToInt(""+strKeyValue)) tree.delete((Integer.parseInt(""+strKeyValue)));

            else tree.delete(""+strKeyValue);
 
            }
           int t = 0;
           while(!values.isEmpty()){
              if(DBApp.canConvertToInt(""+strKeyValue)) tree.insert((Integer.parseInt(""+strKeyValue)) ,  values.get(t));

              else tree.insert(""+strKeyValue, values.get(t));

              values.remove(t);
          }


            in.close();
             fileIn.close(); 
            FileOutputStream fileOut = new FileOutputStream(treeName+".class");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(tree);
            out.close();
            fileOut.close();





        return Pages;



    }
	
	
public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException,Exception{
		//try {
		ArrayList<Integer> PosOfEmptyPages =new ArrayList<>();
		Table table=Table.loadTable(strTableName);
		BufferedReader reader = new BufferedReader(new FileReader("metadata.csv"));
		String line;
		boolean flag=false;
		ArrayList<ArrayList<Page>> Pages_Array = new ArrayList<ArrayList<Page>>();
		// find the position of every column that was mentioned in the htblColNameValue


		ArrayList<String> TheConditions = new ArrayList<>();
		ArrayList<Tuple> TuplesRemoved = new ArrayList<>();

		ArrayList<Integer> positions = new ArrayList<Integer>();
		int pos =-1;

		while ((line = reader.readLine()) != null) {
			String[] parts = line.split(",");
			if (parts[0].equals(strTableName)) {
				pos++;

				if(htblColNameValue.containsKey(parts[1])) {
					positions.add(pos);
					TheConditions.add(htblColNameValue.get(parts[1])+"");


				}
				if(htblColNameValue.containsKey(parts[1]) && parts[5].equals("B+tree")) {
					flag = true;
					Pages_Array.add(ReturnPages(strTableName,""+htblColNameValue.get(parts[1]),parts[4]));

				}


			}
		}

		System.out.println(positions.size());
		reader.close();

		if(!flag) {

			if (isSerializedObjectExists(strTableName+1+".class")) {

				Page Current_page = Page.loadFile(strTableName+1+".class");


				for(int j=0;j<table.PageCount-1;j++) {  ////////Page Count looping on page////////////

					String pagename=table.pageFileNames.get(j);

					Current_page = Page.loadFile(pagename);                                                                                                  

					int TmamAwy =0;
					for(int g=0;g<Current_page.tuples.size();g++) {     /// looping on the Tuples in the page                                                                        
						//Tuple Current_Tuple:Current_page.tuples
						TmamAwy =0;
						Tuple Current_Tuple=Current_page.tuples.get(g);

						for(int same =0; same<positions.size() ;same++) {// looping on the conditions
							if(DBApp.AreEqual(Current_Tuple.content.get(positions.get(same)),TheConditions.get(same)))  
								TmamAwy++;
						}

						if(TmamAwy==TheConditions.size()) {
							//TmamAwy =0;
							TuplesRemoved.add(Current_Tuple);

							DeleteItsbplustreeElements(Current_Tuple,strTableName); // method i created to remove every element that has BTree index in the Tuple deleted

							Current_page.tuples.remove(Current_Tuple);
							Current_page.count--;
							if (Current_page.count==0) PosOfEmptyPages.add(j);
							Page.saveFile(Current_page);
							//Table.saveTable(Table.loadTable(strTableName));
							Page.saveFile(Current_page);

						}



						Page.saveFile(Current_page);




					}
					Page.saveFile(Current_page);




				}
				for(int i =0;i<PosOfEmptyPages.size();i++) {
					String pagename=table.pageFileNames.get(PosOfEmptyPages.get(i));


					Current_page = Page.loadFile(pagename); 
					new File(Current_page.Name).delete();


					table.pageFileNames.remove(pagename);
					// refresh the names of the pages in the table?

				}
				if(!PosOfEmptyPages.isEmpty()) {
					int i=1;
					for(String pager : table.pageFileNames) {
						Current_page = Page.loadFile(pager);
						System.out.print("checking page"+table.tableName+i+".class"+(""+Current_page.Name));
						if(!(""+Current_page.Name).equals(table.tableName+i+".class")) {
							new File(Current_page.Name).delete();
							Current_page.Name=table.tableName+i+".class";
							for(Tuple rr:Current_page.tuples) {
								rr.update_index(table.tableName, Current_page.Name);}
							Page.saveFile(Current_page);
							System.out.println(table.pageFileNames.size()-1);
							table.pageFileNames.remove(table.pageFileNames.size()-1);	
							table.pageFileNames.add(table.tableName+i+".class");
							Table.saveTable(table);
							
								
							
						}
						i++;

					}
					
					
				}
				System.out.println("m.omar monkeY"+table.pageFileNames.toString());
				Table.saveTable(table);


			}else return;
		}else {
			//System.out.println(RED+ "ANA HENA" +RESET);
			Page Current_page = null;
			Tuple Current_Tuple =null;
			for(int k=0; k<Pages_Array.size();k++) {
				for(int j=0;j<Pages_Array.get(k).size();j++) {  ////////Page Count looping on page////////////
					//String pagename=table.pageFileNames.get(j);
					Current_page = Pages_Array.get(k).get(j);
					//int TmamAwy =0;
					for(int i =0;i<Current_page.tuples.size();i++) {     /// looping on the Tuples in the page
						Current_Tuple =Current_page.tuples.get(i);
						int TmamAwy =0;
						for(int same =0; same<positions.size() ;same++) { // looping on the conditions
							if(DBApp.AreEqual(Current_Tuple.content.get(positions.get(same)),TheConditions.get(same)))
								TmamAwy++;    }
						if(TmamAwy==TheConditions.size()) {
							TuplesRemoved.add(Current_Tuple);
							DeleteItsbplustreeElements(Current_Tuple,strTableName); // method i created to remove every element that has BTree index in the Tuple deleted
							Current_page.tuples.remove(Current_Tuple);
							Current_page.count--;
							if (Current_page.count==0) PosOfEmptyPages.add(j);
							Page.saveFile(Current_page);
							Table.saveTable(Table.loadTable(strTableName));
							Page.saveFile(Current_page);
						}//deleting the tuple
						Page.saveFile(Current_page);
					}
					Page.saveFile(Current_page);
				}
			}

			for(int i =0;i<PosOfEmptyPages.size();i++) {
				String pagename=table.pageFileNames.get(PosOfEmptyPages.get(i));


				//Current_page = Page.loadFile(pagename); 

				Current_page = Page.loadFile(pagename); 
				new File(Current_page.Name).delete();


				table.pageFileNames.remove(pagename);
				// refresh the names of the pages in the table?

			}
			if(!PosOfEmptyPages.isEmpty()) {
				int i=1;
				for(String pager : table.pageFileNames) {
					Current_page = Page.loadFile(pager);
					System.out.print("checking page"+table.tableName+i+".class"+(""+Current_page.Name));
					if(!(""+Current_page.Name).equals(table.tableName+i+".class")) {
						new File(Current_page.Name).delete();
						Current_page.Name=table.tableName+i+".class";
						for(Tuple rr:Current_page.tuples) {
							rr.update_index(table.tableName, Current_page.Name);}
						Page.saveFile(Current_page);
						System.out.println(table.pageFileNames.size()-1);
						table.pageFileNames.remove(table.pageFileNames.size()-1);	
						table.pageFileNames.add(table.tableName+i+".class");
						Table.saveTable(table);
						
							
						
					}
					i++;

				}
				
				
			}


		}

	}





	public static boolean compar_to(Object first, Object second) {
		if (canConvertToInt(""+first)) {
			//System.out.println("INTEGER YABAAA" +Integer.parseInt((String) first.getElement(clusterindex))+Integer.parseInt((String) (second.getElement(clusterindex))));
			int firstValue = Integer.parseInt(first+"");
			int secondValue = Integer.parseInt( second+"");
			return firstValue == secondValue;
		}

		else if (first instanceof String) {
			if ((first+"").compareTo(second+"")==0) {
				return true;
			}
			else return false;
		}
		else {

			System.out.println("erga3 leh comparto 3ashan msh sha8alah");
			return false;
		}
	}

	public static boolean equal_to(Object first, Object second) {
		if (canConvertToInt(""+first)) {
			//System.out.println("INTEGER YABAAA" +Integer.parseInt(""+ first.getElement(clusterindex))+Integer.parseInt(""+ (second.getElement(clusterindex))));
			int firstValue = Integer.parseInt(first+"");
			int secondValue = Integer.parseInt( second+"");
			return firstValue == secondValue;
		}

		else if (first instanceof String) {
			if ((first+"").compareTo(second+"")==0) {
				return true;
			}
			else return false;
		}
		else {

			System.out.println("erga3 leh equal 3ashan msh sha8alah");
			return false;
		}
	}


	public static boolean AreEqual(Object first, Object second) {
		if (canConvertToInt(""+first)) {
			//System.out.println("INTEGER YABAAA" +Integer.parseInt((String) first.getElement(clusterindex))+Integer.parseInt((String) (second.getElement(clusterindex))));
			int firstValue = Integer.parseInt(first+"");
			int secondValue = Integer.parseInt( second+"");
			return firstValue == secondValue;
		}

		else if (first instanceof String) {
			if ((first+"").compareTo(second+"")==0) {
				return true;
			}
			else return false;
		}
		else {

			System.out.println("erga3 leh comparto 3ashan msh sha8alah");
			return false;
		}
	}


	public static boolean NotEqual(Object first, Object second) {
		if (canConvertToInt(""+first)) {
			//System.out.println("INTEGER YABAAA" +Integer.parseInt((String) first.getElement(clusterindex))+Integer.parseInt((String) (second.getElement(clusterindex))));
			int firstValue = Integer.parseInt(first+"");
			int secondValue = Integer.parseInt( second+"");
			return firstValue!=secondValue;
		}

		else if (first instanceof String) {
			if ((first+"").compareTo(second+"")!=0) {
				return true;
			}
			else return false;
		}
		else {

			System.out.println("erga3 leh comparto 3ashan msh sha8alah");
			return false;
		}
	}


	public static boolean LessThan(Object first, Object second) {
		if (canConvertToInt(""+first)) {
			//System.out.println("INTEGER YABAAA" +Integer.parseInt((String) first.getElement(clusterindex))+Integer.parseInt((String) (second.getElement(clusterindex))));
			int firstValue = Integer.parseInt(first+"");
			int secondValue = Integer.parseInt( second+"");
			return firstValue <secondValue;
		}

		else if (first instanceof String) {
			if ((first+"").compareTo(second+"")<0) {
				return true;
			}
			else return false;
		}
		else {

			System.out.println("erga3 leh comparto 3ashan msh sha8alah");
			return false;
		}
	}



	public static boolean BiggerThan(Object first, Object second) {
		if (canConvertToInt(""+first)) {
			//System.out.println("INTEGER YABAAA" +Integer.parseInt((String) first.getElement(clusterindex))+Integer.parseInt((String) (second.getElement(clusterindex))));
			int firstValue = Integer.parseInt(first+"");
			int secondValue = Integer.parseInt( second+"");
			return firstValue > secondValue;
		}

		else if (first instanceof String) {
			if ((first+"").compareTo(second+"")>0) {
				return true;
			}
			else return false;
		}
		else {

			System.out.println("erga3 leh comparto 3ashan msh sha8alah");
			return false;
		}
	}



	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		List<Tuple> selectedTuples = new ArrayList<Tuple>();
		for (SQLTerm term : arrSQLTerms) {
			if (!checkIfTableExists(term._strTableName)) {
				throw new DBAppException("Table does not exist: " + term._strTableName);
			}
		}

		Table table = Table.loadTable(arrSQLTerms[0]._strTableName); // should be "term" not arrSQLTerms[0] 

		if ((arrSQLTerms[0]._strOperator)=="=") selectedTuples = applyConditionEqual(arrSQLTerms[0], table); // should be "term" not arrSQLTerms[0] 
		//System.out.print(selectedTuples.toString());

		if ((arrSQLTerms[0]._strOperator)=="!=") selectedTuples = applyConditionNotEqual(arrSQLTerms[0], table);

		if((arrSQLTerms[0]._strOperator)=="<=") selectedTuples = applyConditionLessThanOrEqual(arrSQLTerms[0], table);

		if((arrSQLTerms[0]._strOperator)==">=") selectedTuples = applyConditionBiggerThanOrEqual(arrSQLTerms[0], table);

		if((arrSQLTerms[0]._strOperator)=="<") 	selectedTuples = applyConditionLessThan(arrSQLTerms[0], table);

		if((arrSQLTerms[0]._strOperator)==">") selectedTuples = applyConditionBiggerThan(arrSQLTerms[0], table);




		for (int i = 1; i < arrSQLTerms.length; i++) {
			if (strarrOperators[i - 1].equalsIgnoreCase("AND")) {
				System.out.println("and");
				selectedTuples = applyAND(selectedTuples, arrSQLTerms[i], table);
			} 
			else if (strarrOperators[i - 1].equalsIgnoreCase("OR")) {
				System.out.println("or");
				selectedTuples = applyOR(selectedTuples, arrSQLTerms[i], table);
			} 
			else if (strarrOperators[i - 1].equalsIgnoreCase("XOR")) {
				System.out.println("xor");
				selectedTuples = applyXOR(selectedTuples , arrSQLTerms[i], table);
			} 
			else {
				throw new DBAppException("Invalid operator: " + strarrOperators[i - 1]);
			}
		}

		Table.saveTable(table);

		return selectedTuples.iterator();
	}


	private List<Tuple> applyConditionEqual(SQLTerm sqlTerm, Table table) throws DBAppException {
		List<Tuple> selectedTuples = new ArrayList<>();
		//System.out.println(sqlTerm._strColumnName);
		int columnIndex = table.getColumnIndex(sqlTerm._strColumnName); // for example "Age"

		for (String pageFileName : table.pageFileNames) { //looping on the pages in the table
			Page page = Page.loadFile(pageFileName);
			for (Tuple tuple : page.tuples) { // looping on the Tuples in the page
				if (DBApp.AreEqual(tuple.content.get(columnIndex), sqlTerm._objValue)) { // applying equal
					selectedTuples.add(tuple);
				}
			}
			Page.saveFile(page);
		}

		return selectedTuples;
	}



	private List<Tuple> applyConditionNotEqual(SQLTerm sqlTerm, Table table) throws DBAppException{
		List<Tuple> selectedTuples = new ArrayList<>();
		//System.out.println(sqlTerm._strColumnName);
		int columnIndex = table.getColumnIndex(sqlTerm._strColumnName); // for example "Age"

		for (String pageFileName : table.pageFileNames) { //looping on the pages in the table
			Page page = Page.loadFile(pageFileName);
			for (Tuple tuple : page.tuples) { // looping on the Tuples in the page
				if (DBApp.NotEqual(tuple.content.get(columnIndex), sqlTerm._objValue)) { // applying Not Equal
					selectedTuples.add(tuple);
				}
			}
			Page.saveFile(page);
		}

		return selectedTuples;
	}

	private List<Tuple> applyConditionLessThanOrEqual(SQLTerm sqlTerm, Table table) throws DBAppException{
		List<Tuple> selectedTuples = new ArrayList<>();
		//System.out.println(sqlTerm._strColumnName);
		int columnIndex = table.getColumnIndex(sqlTerm._strColumnName); // for example "Age"

		for (String pageFileName : table.pageFileNames) { //looping on the pages in the table
			Page page = Page.loadFile(pageFileName);
			for (Tuple tuple : page.tuples) { // looping on the Tuples in the page
				if (DBApp.AreEqual(tuple.content.get(columnIndex), sqlTerm._objValue) || DBApp.LessThan(tuple.content.get(columnIndex), sqlTerm._objValue)  ) { // applying Not Equal
					selectedTuples.add(tuple);
				}
			}
			Page.saveFile(page);
		}

		return selectedTuples;


	}


	private List<Tuple>  applyConditionLessThan(SQLTerm sqlTerm, Table table) throws DBAppException{

		List<Tuple> selectedTuples = new ArrayList<>();
		//System.out.println(sqlTerm._strColumnName);
		int columnIndex = table.getColumnIndex(sqlTerm._strColumnName); // for example "Age"

		for (String pageFileName : table.pageFileNames) { //looping on the pages in the table
			Page page = Page.loadFile(pageFileName);
			for (Tuple tuple : page.tuples) { // looping on the Tuples in the page
				if ( DBApp.LessThan(tuple.content.get(columnIndex), sqlTerm._objValue)) { // applying Not Equal
					selectedTuples.add(tuple);
				}
			}
			Page.saveFile(page);
		}

		return selectedTuples;


	}





	private List<Tuple>  applyConditionBiggerThanOrEqual(SQLTerm sqlTerm, Table table) throws DBAppException{
		List<Tuple> selectedTuples = new ArrayList<>();
		//System.out.println(sqlTerm._strColumnName);
		int columnIndex = table.getColumnIndex(sqlTerm._strColumnName); // for example "Age"

		for (String pageFileName : table.pageFileNames) { //looping on the pages in the table
			Page page = Page.loadFile(pageFileName);
			for (Tuple tuple : page.tuples) { // looping on the Tuples in the page
				if (DBApp.AreEqual(tuple.content.get(columnIndex), sqlTerm._objValue) || DBApp.BiggerThan(tuple.content.get(columnIndex), sqlTerm._objValue)  ) { // applying Not Equal
					selectedTuples.add(tuple);
				}
			}
			Page.saveFile(page);
		}

		return selectedTuples;

	}


	private List<Tuple> applyConditionBiggerThan(SQLTerm sqlTerm, Table table)  throws DBAppException{
		List<Tuple> selectedTuples = new ArrayList<>();
		//System.out.println(sqlTerm._strColumnName);
		int columnIndex = table.getColumnIndex(sqlTerm._strColumnName); // for example "Age"

		for (String pageFileName : table.pageFileNames) { //looping on the pages in the table
			Page page = Page.loadFile(pageFileName);
			for (Tuple tuple : page.tuples) { // looping on the Tuples in the page
				if ( DBApp.BiggerThan(tuple.content.get(columnIndex), sqlTerm._objValue)  ) { // applying Not Equal
					selectedTuples.add(tuple);
				}
			}
			Page.saveFile(page);
		}

		return selectedTuples;

	}








	private List<Tuple> applyAND(List<Tuple> selectedTuples, SQLTerm sqlTerm, Table table) throws DBAppException {
		List<Tuple> newSelectedTuples = new ArrayList<>();
		int columnIndex = table.getColumnIndex(sqlTerm._strColumnName);
		List<Tuple> selectedTuples2 = null ;


		if ((sqlTerm._strOperator)=="=") selectedTuples2 = applyConditionEqual(sqlTerm, table); // should be "term" not arrSQLTerms[0] 
		//System.out.print(selectedTuples.toString());

		if ((sqlTerm._strOperator)=="!=") selectedTuples2 = applyConditionNotEqual(sqlTerm, table);

		if((sqlTerm._strOperator)=="<=") selectedTuples2 = applyConditionLessThanOrEqual(sqlTerm, table);

		if((sqlTerm._strOperator)==">=") selectedTuples2 = applyConditionBiggerThanOrEqual(sqlTerm, table);

		if((sqlTerm._strOperator)=="<") selectedTuples2 = applyConditionLessThan(sqlTerm, table);

		if((sqlTerm._strOperator)==">") selectedTuples2 = applyConditionBiggerThan(sqlTerm, table);


		//
		//System.out.println(selectedTuples.toString());
		//System.out.println(selectedTuples2.toString());
		if(selectedTuples2==null) {
			System.out.print("second tuple =0");
		}
		System.out.println(selectedTuples.toString());
		System.out.println(selectedTuples2.toString());
		/*
		for (String pageFileName : table.pageFileNames) {
			Page page = Page.loadFile(pageFileName);
			for (Tuple tuple : page.tuples) {
				if (tuple.content.get(columnIndex).equals(sqlTerm._objValue)) {
					if (selectedTuples.contains(tuple)) {
						newSelectedTuples.add(tuple);
					}
				}
			}
		}
		 */

		for(Tuple tuple : selectedTuples) {
			for(Tuple tuple2 : selectedTuples2) {
				//System.out.println("ssssss"+Tuple.eqTo(tuple, tuple2));
				if (Tuple.eqTo(tuple, tuple2)) {
					newSelectedTuples.add(tuple);
					break;
				}
			}
		}

		return newSelectedTuples;
	}


	private List<Tuple> applyOR(List<Tuple> selectedTuples, SQLTerm sqlTerm, Table table) throws DBAppException {
		List<Tuple> newSelectedTuples = new ArrayList<>();
		int columnIndex = table.getColumnIndex(sqlTerm._strColumnName);
		//List<Tuple> selectedTuples2 = applyConditionEqual(sqlTerm, table);
		List<Tuple> selectedTuples2 = null ;


		if ((sqlTerm._strOperator)=="=") selectedTuples2 = applyConditionEqual(sqlTerm, table); // should be "term" not arrSQLTerms[0] 
		//System.out.print(selectedTuples.toString());

		if ((sqlTerm._strOperator)=="!=") selectedTuples2 = applyConditionNotEqual(sqlTerm, table);

		if((sqlTerm._strOperator)=="<=") selectedTuples2 = applyConditionLessThanOrEqual(sqlTerm, table);

		if((sqlTerm._strOperator)==">=") selectedTuples2 = applyConditionBiggerThanOrEqual(sqlTerm, table);

		if((sqlTerm._strOperator)=="<") selectedTuples2 = applyConditionLessThan(sqlTerm, table);

		if((sqlTerm._strOperator)==">") selectedTuples2 = applyConditionBiggerThan(sqlTerm, table);


		//
		//System.out.println(selectedTuples.toString());
		//System.out.println(selectedTuples2.toString());
		if(selectedTuples2==null) {
			System.out.print("second tuple =0");
		}

		for (Tuple tuple : selectedTuples2){
			if (DBApp.AreEqual(tuple.content.get(columnIndex), sqlTerm._objValue)) {
				if (!selectedTuples.contains(tuple)){
					newSelectedTuples.add(tuple);
				}
			} 
			else {
				if (!newSelectedTuples.contains(tuple)){
					newSelectedTuples.add(tuple);
				}
			}
		}
		for(Tuple tuple : selectedTuples) {
			for(Tuple tuple2 : selectedTuples2){
				//System.out.print(!Tuple.eqTo(tuple, tuple2));
				if (!Tuple.eqTo(tuple, tuple2)) {
					newSelectedTuples.add(tuple);
				}
			}
		}

		//newSelectedTuples.addAll(selectedTuples);
		//newSelectedTuples.addAll(selectedTuples2);


		return newSelectedTuples;
	}

	private List<Tuple> applyXOR(List<Tuple> selectedTuples, SQLTerm sqlTerm, Table table) throws DBAppException {
		List<Tuple> newSelectedTuples = new ArrayList<>();
		int columnIndex = table.getColumnIndex(sqlTerm._strColumnName);
		List<Tuple> selectedTuples2 = null ;


		if ((sqlTerm._strOperator)=="=") selectedTuples2 = applyConditionEqual(sqlTerm, table); // should be "term" not arrSQLTerms[0] 
		//System.out.print(selectedTuples.toString());

		if ((sqlTerm._strOperator)=="!=") selectedTuples2 = applyConditionNotEqual(sqlTerm, table);

		if((sqlTerm._strOperator)=="<=") selectedTuples2 = applyConditionLessThanOrEqual(sqlTerm, table);

		if((sqlTerm._strOperator)==">=") selectedTuples2 = applyConditionBiggerThanOrEqual(sqlTerm, table);

		if((sqlTerm._strOperator)=="<") selectedTuples2 = applyConditionLessThan(sqlTerm, table);

		if((sqlTerm._strOperator)==">") selectedTuples2 = applyConditionBiggerThan(sqlTerm, table);


		//
		//System.out.println(selectedTuples.toString());
		//System.out.println(selectedTuples2.toString());
		if(selectedTuples2==null) {
			System.out.print("second tuple =0");
		}

		//List<Tuple> selectedTuples2 = applyConditionEqual(sqlTerm, table);
		System.out.println(selectedTuples.toString());
		System.out.println(selectedTuples2.toString());


		for (Tuple tuple : selectedTuples2) {
			if (DBApp.AreEqual(tuple.content.get(columnIndex), sqlTerm._objValue)) {
				if (!selectedTuples.contains(tuple)) {
					newSelectedTuples.add(tuple);
				}
			} else {
				if (!newSelectedTuples.contains(tuple)) {
					newSelectedTuples.add(tuple);
				}
			}
		}
		for(Tuple tuple : selectedTuples) {
			for(Tuple tuple2 : selectedTuples2) {
				System.out.print(!Tuple.eqTo(tuple, tuple2));
				if (!Tuple.eqTo(tuple, tuple2)) {
					newSelectedTuples.add(tuple);
				}
			}
		}
		for(Tuple tuple : selectedTuples) {
			for(Tuple tuple2 : selectedTuples2) {
				//System.out.println("ssssss"+Tuple.eqTo(tuple, tuple2));
				if (Tuple.eqTo(tuple, tuple2)) {
					System.out.println("dewd");
					newSelectedTuples.remove(tuple);
					newSelectedTuples.remove(tuple2);
				}
			}
		}
		return newSelectedTuples;
	}

















	public static Page search(String strTableName, String Colname ,Object strClusteringKeyValue)  throws DBAppException{
		try {
			String line;
			bplustree tree;
			String treeName;
			Page page;
			BufferedReader reader1 = new BufferedReader(new FileReader("metadata.csv"));
			while ((line = reader1.readLine()) != null) {
				String[] parts = line.split(",");
				if (parts[0].equals(strTableName)) {
					if (parts[1].equals(Colname)) {
						if(parts[5].equals("B+tree")) {
							treeName=parts[4];
							FileInputStream fileIn = new FileInputStream(treeName+".class");
							ObjectInputStream in = new ObjectInputStream(fileIn);

							tree = (bplustree) in.readObject();

							if(DBApp.canConvertToInt(strClusteringKeyValue)) {
								page = Page.loadFile(tree.search(Integer.parseInt(""+strClusteringKeyValue)));
								System.out.println(tree.search(Integer.parseInt(""+strClusteringKeyValue)));
								return page;
							}
							else {
								page = Page.loadFile(tree.search(""+strClusteringKeyValue));
								System.out.println(tree.search(""+strClusteringKeyValue));
								return page;
							}
							


						}
					}
				}
			}


		return null
				;
		
		}catch(Exception e) {
			throw new DBAppException("till the end");
		}
		
		}






















	public static boolean isSerializedObjectExists(String filename) {
		File file = new File(filename);
		return file.exists();
	}

	public static void main( String[] args ){

		try{

			String strTableName = "Student";
			DBApp	dbApp = new DBApp( );
			Hashtable htblColNameValue = new Hashtable( );;


			/*
				Hashtable htblColNameType = new Hashtable( );
				htblColNameType.put("id", "java.lang.Integer");
				htblColNameType.put("name", "java.lang.String");
				htblColNameType.put("gpa", "java.lang.double");
				dbApp.createTable( strTableName, "gpa", htblColNameType );




				htblColNameValue.put("id", new Integer( 2343432 ));
				htblColNameValue.put("name", new String("Moemen Hesham" ) );
				htblColNameValue.put("gpa", new Double( 0.65 ) );
				dbApp.insertIntoTable( strTableName , htblColNameValue );


				htblColNameValue.clear( );
				htblColNameValue.put("id", new Integer( 5674567 ));
				htblColNameValue.put("name", new String("MArk Medhat" ) );
				htblColNameValue.put("gpa", new Double( 1.07 ) );
				dbApp.insertIntoTable( strTableName , htblColNameValue );



				htblColNameValue.clear( );
				htblColNameValue.put("id", new Integer( 78452 ));
				htblColNameValue.put("name", new String("Mark Maged" ) );
				htblColNameValue.put("gpa", new Double( 0.91 ) );
				dbApp.insertIntoTable( strTableName , htblColNameValue );

				htblColNameValue.clear( );
				htblColNameValue.put("id", new Integer( 23498 ));
				htblColNameValue.put("name", new String("John el Kess" ) );
				htblColNameValue.put("gpa", new Double( 1.4 ) );
				dbApp.insertIntoTable( strTableName , htblColNameValue );

				htblColNameValue.clear( );
				htblColNameValue.put("id", new Integer( 453455 ));
				htblColNameValue.put("name", new String("ALyyyy ffffff" ) );
				htblColNameValue.put("gpa", new Double( 0.69 ) );
				dbApp.insertIntoTable( strTableName , htblColNameValue );


				htblColNameValue.put("id", new Integer( 2343432 ));
				htblColNameValue.put("name", new String("Ahmed Noor" ) );
				htblColNameValue.put("gpa", new Double( 0.95 ) );
				dbApp.insertIntoTable( strTableName , htblColNameValue );


				htblColNameValue.clear( );
				htblColNameValue.put("id", new Integer( 5674567 ));
				htblColNameValue.put("name", new String("Dalia Noor" ) );
				htblColNameValue.put("gpa", new Double( 1.25 ) );
				dbApp.insertIntoTable( strTableName , htblColNameValue );


				dbApp.createIndex( strTableName, "gpa", "gpaIndex" );
				dbApp.createIndex( strTableName, "id", "idIndex" );
				dbApp.createIndex( strTableName, "name", "nameIndex" );

				htblColNameValue.clear( );
				htblColNameValue.put("id", new Integer( 78452 ));
				htblColNameValue.put("name", new String("Zaky Noor" ) );
				htblColNameValue.put("gpa", new Double( 0.88 ) );
				dbApp.insertIntoTable( strTableName , htblColNameValue );

				htblColNameValue.clear( );
				htblColNameValue.put("id", new Integer( 23498 ));
				htblColNameValue.put("name", new String("John Noor" ) );
				htblColNameValue.put("gpa", new Double( 1.5 ) );
				dbApp.insertIntoTable( strTableName , htblColNameValue );

				htblColNameValue.clear( );
				htblColNameValue.put("id", new Integer( 453455 ));
				htblColNameValue.put("name", new String("Ahmed ffffff" ) );
				htblColNameValue.put("gpa", new Double( 0.96 ) );
				dbApp.insertIntoTable( strTableName , htblColNameValue );



				viewa("nameIndex");
				viewa("idIndex");
				viewa("gpaIndex");
				viewall(strTableName);

	*/		 

		//	Page m=search(strTableName,"id",453455);

		

			
			//Hashtable htblColNameValue = new Hashtable( );

			viewall(strTableName);

			htblColNameValue.clear( );
			//htblColNameValue.put("id", new Integer( 237688 ));
			//htblColNameValue.put("name", new String("EHNA SAH" ) );
			htblColNameValue.put("gpa", new Double( 1.95 ));
			dbApp.deleteFromTable(strTableName, htblColNameValue);

			viewall(strTableName);


/*
			viewa("nameIndex");
			viewa("idIndex");
			viewa("gpaIndex");


				htblColNameValue.clear( );
			htblColNameValue.put("id", new Integer( 237688 ));
			htblColNameValue.put("name", new String("EHNA SAH" ) );
			htblColNameValue.put("gpa", new Double( 0.77 ) );
			dbApp.updateTable( strTableName , "1.5", htblColNameValue );


			htblColNameValue.clear( );
			htblColNameValue.put("id", new Integer( 948213 ));
			htblColNameValue.put("name", new String("Mohamed omar" ) );
			htblColNameValue.put("gpa", new Double( 1.3 ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );*/

			
			viewall(strTableName);

			SQLTerm[] arrSQLTerms;
			arrSQLTerms = new SQLTerm[2];
			arrSQLTerms[0]=new SQLTerm();
			arrSQLTerms[0]._strTableName =  "Student";
			arrSQLTerms[0]._strColumnName=  "gpa";
			arrSQLTerms[0]._strOperator  =  "=";
			arrSQLTerms[0]._objValue     =  "0.91";

			arrSQLTerms[1]=new SQLTerm();
			arrSQLTerms[1]._strTableName =  "Student";
			arrSQLTerms[1]._strColumnName=  "gpa";
			arrSQLTerms[1]._strOperator  =  "=";
			arrSQLTerms[1]._objValue     =  "0.69";
/*
			arrSQLTerms[2]=new SQLTerm();
			arrSQLTerms[2]._strTableName =  "Student";
			arrSQLTerms[2]._strColumnName=  "id";
			arrSQLTerms[2]._strOperator  =  "=";
			arrSQLTerms[2]._objValue     =  new Integer( 4232 );

			arrSQLTerms[3]=new SQLTerm();
			arrSQLTerms[3]._strTableName =  "Student";
			arrSQLTerms[3]._strColumnName=  "id";
			arrSQLTerms[3]._strOperator  =  "=";
			arrSQLTerms[3]._objValue     =  new Integer( 4232 );*/
			

			String[]strarrOperators = new String[1];
			strarrOperators[0] = "XOR";
			//strarrOperators[1] = "XOR";
			//strarrOperators[2] = "XOR";
			// select * from Student where name = "John Noor" or gpa = 1.5;
			Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
			while(resultSet.hasNext())
			{
				System.out.println(resultSet.next());
			}
			 
		}

		catch(Exception exp){
			exp.printStackTrace( );
		}

















	}

	private static void viewa(String string) throws IOException, ClassNotFoundException {
		FileInputStream fileIn = new FileInputStream(string+".class");
		ObjectInputStream in = new ObjectInputStream(fileIn);
		bplustree t = (bplustree) in.readObject();
		System.out.print("ana hena");
		System.out.print(string);
		t.printTree();
		t.print();
		in.close();
		fileIn.close();




	}







}











































