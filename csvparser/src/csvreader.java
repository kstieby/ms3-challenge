
package csvparser;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets; 
import java.nio.file.Files; 
import java.nio.file.Path; 
import java.nio.file.Paths; 
import java.util.ArrayList; 
import java.util.List;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
  
public class CSVreader {
  public static void main(String... args) { 
    readEntriesFromCSV("src/ms3Interview.csv");  
  } 
  
  private static void readEntriesFromCSV(String fileName) {
	// entries both for database, and trash file
    List<Entry> entries = new ArrayList<>(); 
    // arraylist of strings to write to the bad csv file
    List<String[]> bad_entries = new ArrayList<>();
    Path pathToFile = Paths.get(fileName); 
    
    // create an instance of BufferedReader  
    try (BufferedReader br = Files.newBufferedReader(pathToFile, StandardCharsets.UTF_8)) { 
    
      // read the first line from the text file 
      String line = br.readLine(); 
      
      // loop until all lines are read 
      while (line != null) { 
        // use string.split to load a string array with the values from 
        // each line of 
        // the file, using a comma as the delimiter 
        String[] attributes = line.split(","); 
        // can check here to see if the amount of values is correct
        // doing so here, would allow for the bad information to immediately be written to the 
        // bad csv file
        boolean correct = true;
        for(int i = 0; i < attributes.length;i++) {
        	// check if the values are null
        	if(attributes[i] == null || attributes[i].length() == 0) {
        		correct = false;
        	}
        }
        if(correct && attributes.length == 11) {
        	Entry entry = createEntry(attributes); 
        	// adding the entry into ArrayList 
        	entries.add(entry);
        }
        // write the line to the bad csv file
        else {
        	
        	bad_entries.add(attributes);
        }
        line = br.readLine(); 
      } 
    } catch (IOException ioe) { 
      ioe.printStackTrace(); 
    } 
    
    // file for all of the incorrect rows to be printed to
    File badcsv = new File(fileName + "-bad.csv");
    PrintWriter pw = null;
    try {
		pw = new PrintWriter(badcsv);
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    
    // section below appends all of the info for the bad csv file
    // then prints it to the file
    int i = 1;
    StringBuilder builder = new StringBuilder();
    String columnNames = "A,B,C,D,E,F,G,H,I,J";
    builder.append(columnNames + "\n");
    while(i < bad_entries.size()) {
    	String[] current = bad_entries.get(i);
    	int j = 0;
    	while(j < current.length) {
    		builder.append(current[j] + ",");
    		j++;
    	}
    	builder.append("\n");
    	i++;
    }
    pw.write(builder.toString());
    pw.close();
    //System.out.printf("File is located at %s%n", badcsv.getAbsolutePath());
    
    // all aspects of writing to the log file are below
    // will display total number of records, as well as how many are correct or incorrect
    
    File logfile = new File(fileName + ".log");
    PrintWriter pw_log = null;
    try {
 		pw_log = new PrintWriter(logfile);
 	} catch (IOException e) {
 		// TODO Auto-generated catch block
 		e.printStackTrace();
 	}
    StringBuilder builder_log = new StringBuilder();
    builder_log.append("Input from:" + fileName + "\n");
    builder_log.append("total number of records:" + (entries.size() + bad_entries.size()) + "\n");
    builder_log.append("Number of correct records:" + entries.size() + "\n");
    builder_log.append("Number of incorrect records:" + bad_entries.size() + "\n");
    pw_log.write(builder_log.toString());
    pw_log.close();
    CreateTable(entries);
     
  } 
  
  
/* Function below takes the correct list of entries, and both
 * creates and fills a table within the given sqlite database
 * 
 * 
 */
  
  public static void CreateTable(List<Entry> entries) {
	    // SQLite connection string
	    // this will need to be changed per computer
	    String url = "jdbc:sqlite:C:/sqlite/ms3Interview.db";
	    // SQL statement for creating a new table
	    
	    
	    try (Connection conn = DriverManager.getConnection(url);
	            Statement stmt = conn.createStatement()) {
	    	
	    	String drop = "DROP TABLE ms3Interview;";
	    	stmt.execute(drop);
	    	
	        String sql = "CREATE TABLE IF NOT EXISTS ms3Interview (\n"
	        		+ " id INTEGER PRIMARY KEY,\n"
	                + "	A TEXT NOT NULL,\n"
	                + "	B TEXT NOT NULL,\n"
	                + "	C TEXT NOT NULL,\n"
	                + "	D TEXT NOT NULL,\n"
	                + "	E TEXT NOT NULL,\n"
	                + "	F TEXT NOT NULL,\n"
	                + "	G TEXT NOT NULL,\n"
	                + "	H TEXT NOT NULL,\n"
	                + "	I TEXT NOT NULL,\n"
	                + "	J TEXT NOT NULL\n"
	                + ");";
	        stmt.execute(sql);
	        
	        // deletes all from table before storing from the file
	        sql = "DELETE FROM ms3Interview;";
	        stmt.execute(sql);
	    } catch (SQLException e) {
	        System.out.println(e.getMessage());
	    }
	    
        String sql_insert = "INSERT INTO ms3Interview VALUES (\n"
        		+ "?,?,?,?,?,?,?,?,?,?,?)";

        
	        // Prepared statement here will save execution time
	   try (Connection conn = DriverManager.getConnection(url);
			   PreparedStatement pstmt = conn.prepareStatement(sql_insert)) {	        
	        

	        // create a new table
		   	for(int i = 0; i < entries.size();i++) {
	    		Entry temp = entries.get(i);
	    		pstmt.setInt(1, i);
	    		pstmt.setString(2, temp.getfirst());
	    		pstmt.setString(3, temp.getlast());
	    		pstmt.setString(4, temp.getemail());
	    		pstmt.setString(5, temp.getgender());
	    		pstmt.setString(6, temp.getimage());
	    		pstmt.setString(7, temp.getpayment());
	    		pstmt.setString(8, temp.getprice());	    		
	    		pstmt.setString(9, temp.getH());
	    		pstmt.setString(10, temp.getI());
	    		pstmt.setString(11, temp.getJ());	    		
	    
	    		pstmt.executeUpdate();
		   	}	
		} catch (SQLException e) {
		     System.out.println(e.getMessage());
	    }
	} 
 
// data type below will hold a single csv row
  
private static Entry createEntry(String[] metadata) {  
	Boolean correct = false; 
	String first = null;
	String last = null;
	String email = null;
	String gender = null; 
	String image = null;
	String payment = null;
	String price = null; 
	String H = null;
	String I = null;
	String J = null;
	if(metadata.length == 11) {
		correct = true;
		first = metadata[0];
		last = metadata[1];
	    email = metadata[2];
	    gender = metadata[3];
	    image = metadata[4] + metadata[5];
	    payment = metadata[6];
	    price = metadata[7];
	    H = metadata[8];
	    I = metadata[9];
	    J = metadata[10];
	}
	else {
		if(metadata.length >= 1) first = metadata[0];
		if(metadata.length >= 2) last = metadata[1];
		if(metadata.length >= 3) email = metadata[2];
		if(metadata.length >= 4) gender = metadata[3];
		if(metadata.length >= 5) image = metadata[4] + metadata[5];
		if(metadata.length >= 6) payment = metadata[6];
		if(metadata.length >= 7) price = metadata[7];
		if(metadata.length >= 8) H = metadata[8];
		if(metadata.length >= 9) I = metadata[9];
		if(metadata.length >= 10) J = metadata[10];
	}
	
	  
    // create and return entry of this metadata 
    return new Entry(correct, first, last, email, gender, image, payment, price, H, I, J);
  }
    
  } class Entry { 
	private Boolean correct;
    private String first; 
    private String last;
    private String email;
    private String gender;
    private String image;
    private String payment;
    private String price;
    private String H;
    private String I;
    private String J;
    
    public Entry(Boolean correct, String first, String last, String email, String gender, String image, String payment,
    		String price, String H, String I, String J) { 
      this.correct = correct;
      this.first = first; 
      this.last = last;
      this.email = email;
      this.gender = gender;
      this.image = image;
      this.payment = payment;
      this.price = price;
      this.H = H;
      this.I = I;
      this.J = J;
    } 
    
    public Boolean getcorrect() {
    	return correct;
    }
    
    public void setcorrect(Boolean correct) {
    	this.correct = correct;
    }
    
    public String getfirst() { 
      return first; 
    }
    
    public void setfirst(String first) { 
    	this.first = first; 
    } 
    
    public String getlast() { 
        return last; 
    }
      
    public void setlast(String last) { 
      	this.last = last; 
    } 
      
    public String getemail() { 
        return email; 
    }
        
    public void setemail(String email) { 
      	this.email = email; 
    } 

    public String getgender() { 
        return gender; 
    }
        
    public void setgender(String gender) { 
      	this.gender = gender; 
    }
    
    public String getimage() { 
        return image; 
    }
        
    public void setimage(String image) { 
      	this.image = image; 
    } 

    public String getpayment() { 
        return payment; 
    }
        
    public void setpayment(String payment) { 
      	this.payment = payment; 
    }

    public String getprice() { 
        return price; 
    }
        
    public void setprice(String price) { 
      	this.price = price; 
    } 
    
    public String getH() { 
        return H; 
    }
        
    public void setH(String H) { 
      	this.H = H; 
    } 
    
    public String getI() { 
        return I; 
    }
        
    public void setI(String I) { 
      	this.I = I; 
    }
    
    public String getJ() { 
        return J; 
    }
        
    public void setJ(String J) { 
      	this.J = J; 
    } 
          
    
  } 
