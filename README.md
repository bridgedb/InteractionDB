Interaction Bridge Database
==========================

This is a java application to create a BridgeDb derby database to map identifiers of interactions from 
different databases to each other. It has been created using the interaction mapping from Rhea 
(http://www.ebi.ac.uk/rhea/). The identifier from Rhea is mapped to identifiers from Enzyme Code, 
EcoCyc, MetaCyc, Macie, Reactome, Kegg Reaction, Unipathway, and Uniprot. This database can be used to 
annotate interactions in PathVisio(http://www.pathvisio.org/), which are displayed in Wikipathways
(http://wikipathways.org/index.php/WikiPathways) as linkouts to the corresponding databases fetched through the [webservice from BridgeDB](https://bridgedb.github.io/pages/webservice.html).

You can build the program by checking out the source code and running the build file using ant.

Or, alternatively you can use the program directly without having to first build it, by downloading the pre-built 
InteractionDB.jar file from the dist folder and run the jar providing the absolute path of the interaction database 
to be created as an argument.

Running the InteractionDb.jar from your command line: 

java -jar InteractionDB.jar "/absolute/path/InteractionsDatabase.bridge"

Open this project in Eclipse:
1. Clone this repository into a local folder on your computer (with "git clone <link>")
1. Open a (new) workspace
1. Click on File -> Import... -> General/Existing Projects into Workspace...
1. Select the folder where you've cloned this repository and click on Open.
1. Click Finish.
