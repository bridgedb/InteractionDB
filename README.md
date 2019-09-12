Interaction Bridge Database
==========================

This is a java application to create a BridgeDb derby database to map identifiers of interactions from 
different databases to each other. It has been created using the interaction mapping from Rhea 
(http://www.ebi.ac.uk/rhea/). The identifier from Rhea is mapped to identifiers from Enzyme Code, 
EcoCyc, MetaCyc, Macie, Reactome, Kegg Reaction, Unipathway, and Uniprot. This database can be used to 
annotate interactions in PathVisio(http://www.pathvisio.org/), which are displayed in Wikipathways
(http://wikipathways.org/index.php/WikiPathways) as linkouts to the corresponding databases.

You can build the program by checking out the source code and running the build file using Maven:

```
mvn clean package
```

Running the `InteractionDb.jar` from your command line: 

```
java -jar target/interaction-db-0.1.0-SNAPSHOT-jar-with-dependencies.jar "/absolute/path/InteractionsDatabase.bridge"
```

Open this project in Eclipse:
1. Clone this repository into a local folder on your computer (with "git clone <link>")
1. Open a (new) workspace
1. Click on File -> Import... -> General/Existing Projects into Workspace...
1. Click Finish.

From Eclipse, you can build the jar file:
1. Find the build.xml file
1. Right mouse click on this file, Run As -> Ant Build.
Check the console output if the jar was build properly.
1. The latest version should be located in the "Dist" folder.

You can also run the IntdbBuilder class from Eclipse:
1. Open the IntdbBuilder (located under src/org/bridgedb/interaction).
1. Click on Run -> Run Configurations


