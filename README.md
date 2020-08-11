# ms3-challenge
code will consume a csv file, parse the data, and insert valid records into a SQLITE database.
It will print all csv entries that do not match the table requirements into a "bad" csv file, as well as print 
statistics about the results (records received, records successful, records failed) into a .log file

 for the code to work without changing it at all, the csv file will need to be called ms3Interview,
 and will need to be placed in the source folder. The database file would also need to be stored in the same
 way that the parth at the start of the 'CreateTable' function shows, and would need the name ms3Interview.db.
 Those paths could easily be changed to accomodate storing, or naming, the csv file or database differently.
 
 Getting this app going would require more constraints on the input file names and file locations, as well as a guarantee 
 of what the expected data to be read in would be. 

** input file must have similar data to the given .csv file for this problem - there is one piece of data (E) containing
a comma that affects how the input had to be read. Without the comma, the entry class would need a slight change.

the .jar file needs to in its respective location within this directory, for reference to perform the sqlite queries.
