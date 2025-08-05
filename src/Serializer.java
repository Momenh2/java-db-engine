
public class Serializer {

	public static Table deserializeTable(String newTableName) {
		
		return Table.loadTable(newTableName);
	}

	public static Page deserializePage(String newTableName, int i) {
		
		return Page.loadFile(newTableName+i+"");
	}
	


}
