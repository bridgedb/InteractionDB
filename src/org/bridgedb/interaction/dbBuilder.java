/**
 * PathVisio,
 * a tool for data visualization and analysis using Biological Pathways
 * Copyright 2006-2013 BiGCaT Bioinformatics Licensed under the Apache 
 * License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 *     
 */
package org.bridgedb.interaction;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.bridgedb.IDMapperException;
import org.bridgedb.Xref;
import org.bridgedb.bio.BioDataSource;
import org.bridgedb.rdb.construct.DBConnector;
import org.bridgedb.rdb.construct.DataDerby;
import org.bridgedb.rdb.construct.GdbConstruct;
import org.bridgedb.rdb.construct.GdbConstructImpl3;

/**
 * This script downloads a fresh version of the interaction mapping file from
 * Rhea, creates a copy of it in the resources folder and then creates the
 * Interaction bridge database with it.
 * 
 * @author anwesha
 * 
 */
public class dbBuilder {

	private String filesep = System.getProperty("file.separator");
	private String[] array = new String[5];
	private String direction;
	private String identifier;
	private String datasource;
	private String inputLine;
	File mappingfile = new File(System.getProperty("user.dir") + filesep
			+ "resources" + filesep + "org" + filesep + "bridgedb" + filesep
			+ "interaction" + filesep + "rhea2xrefs.txt");

	public void downloadMapping() {
		URL url;
		try {
			url = new URL(
					"ftp://ftp.ebi.ac.uk/pub/databases/rhea/tsv/rhea2xrefs.tsv");
			BufferedReader in = new BufferedReader(new InputStreamReader(
					url.openStream()));

			if (mappingfile.exists()) {
				mappingfile.delete();
			}
			BufferedWriter out = new BufferedWriter(new FileWriter(mappingfile,
					true));
			while ((inputLine = in.readLine()) != null) {
				if (!inputLine.startsWith("RHEA") & inputLine.length() > 0) {
					out.write(inputLine + "\n");
				}
			}
			out.close();
			in.close();
			System.out.println("Interaction Mapping Downloaded");
			} catch (Exception e) {
				System.out.println("Interaction Mapping Download failed!");
				e.printStackTrace();
		}
	}

	GdbConstruct newDb;
	String dbname;

	/**
	 * Creates an empty Derby database
	 * 
	 * @throws IDMapperException
	 *             when it cannot write to the database
	 */
	private void init(String dbname, GdbConstruct newDb)
			throws IDMapperException {

		this.newDb = newDb;
		this.dbname = dbname;

		String dateStr = new SimpleDateFormat("yyyyMMdd").format(new Date());
		newDb.setInfo("BUILDDATE", dateStr);
		newDb.setInfo("DATASOURCENAME", "EBI-RHEA");
		newDb.setInfo("DATASOURCEVERSION", "23-05-2013");
		newDb.setInfo("SERIES", "standard-interaction");
		newDb.setInfo("DATATYPE", "Interaction");
		System.out.println("Empty Database created");
	}

	/**
	 * Parsing the mapping file and populating the database
	 * 
	 * @throws MalformedURLException
	 * @throws IOException
	 * @throws IDMapperException
	 */
	private void run() throws MalformedURLException, IOException,
			IDMapperException {
		System.out.println("init");
		BioDataSource.init();
		InputStream input = dbBuilder.class.getClassLoader()
				.getResourceAsStream("org/bridgedb/interaction/rhea2xrefs.txt");
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		
		while ((inputLine = reader.readLine()) != null) {
			inputLine = reader.readLine();
			System.out.println(inputLine);
			array = inputLine.split("\t");
			direction = array[1];
			identifier = array[3];
			datasource = array[4];

			Xref idRhea = new Xref(array[0], BioDataSource.RHEA);
			newDb.addGene(idRhea);
			System.out.println("Rhea id: " + idRhea.getId());

			newDb.addAttribute(idRhea, "Direction", direction);
			System.out.println("Direction: " + direction);

			if (datasource.equalsIgnoreCase("EC")) {
				Xref idEC = new Xref(identifier, BioDataSource.ENZYME_CODE);
				newDb.addGene(idEC);
				newDb.addLink(idRhea, idEC);
			} else if (datasource.equals("KEGG REACTION")) {
				Xref idKeggRn = new Xref(identifier,
						BioDataSource.KEGG_REACTION);
				newDb.addGene(idKeggRn);
				newDb.addLink(idRhea, idKeggRn);
			} else if (datasource.equals("biocyc_id")) {
				Xref idReactome = new Xref(identifier, BioDataSource.BIOCYC);
				newDb.addGene(idReactome);
				newDb.addLink(idRhea, idReactome);
			} else if (datasource.equals("METACYC")) {
				Xref idMetacyc = new Xref(identifier, BioDataSource.BIOCYC);
				newDb.addGene(idMetacyc);
				newDb.addLink(idRhea, idMetacyc);
			} else if (datasource.equals("ECOCYC")) {
				Xref idEcocyc = new Xref(identifier, BioDataSource.BIOCYC);
				newDb.addGene(idEcocyc);
				newDb.addLink(idRhea, idEcocyc);
			} else if (datasource.equals("MACIE")) {
				Xref idMacie = new Xref(identifier, BioDataSource.MACIE);
				newDb.addGene(idMacie);
				newDb.addLink(idRhea, idMacie);
			} else if (datasource.equals("UNIPATHWAY")) {
				Xref idUnipathway = new Xref(identifier,
						BioDataSource.UNIPATHWAY);
				newDb.addGene(idUnipathway);
				newDb.addLink(idRhea, idUnipathway);
			} else if (datasource.equals("UNIPROT")) {
				Xref idUniprot = new Xref(identifier, BioDataSource.UNIPROT);
				newDb.addGene(idUniprot);
				newDb.addLink(idRhea, idUniprot);
			}

			System.out.println(datasource + ":" + identifier + " added");
			newDb.commit();
		}

		System.out.println("Database Populated");

	}

	/**
	 * Finalizing the database
	 * 
	 * @throws IDMapperException
	 */
	private void done() throws IDMapperException {
		newDb.commit();

		System.out.println("END processing text file");

		System.out.println("Compacting database");

		System.out.println("Closing connections");

		newDb.finalize();
	}

	/**
	 * Constructor
	 */
	public dbBuilder(String dbname) {
		this.dbname = dbname;

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String dbname = args[0];
		dbBuilder intdb = new dbBuilder(dbname);

		intdb.downloadMapping();

		try {
			GdbConstruct newDb = GdbConstructImpl3.createInstance(dbname,
					new DataDerby(), DBConnector.PROP_RECREATE);
			newDb.createGdbTables();
			newDb.preInsert();
			intdb.init(dbname, newDb);
			intdb.run();
			intdb.done();
		} catch (Exception e) {
			System.out.println("Interaction Database creation failed!");
			e.printStackTrace();
		}

	}
}
