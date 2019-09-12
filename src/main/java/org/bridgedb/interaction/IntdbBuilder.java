package org.bridgedb.interaction;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bridgedb.DataSource;
import org.bridgedb.IDMapperException;
import org.bridgedb.Xref;
import org.bridgedb.bio.DataSourceTxt;
import org.bridgedb.rdb.construct.DBConnector;
import org.bridgedb.rdb.construct.DataDerby;
import org.bridgedb.rdb.construct.GdbConstruct;
import org.bridgedb.rdb.construct.GdbConstructImpl3;
import org.bridgedb.tools.qc.BridgeQC;

/**
 * This program creates an Interaction annotation BridgeDB derby database
 * mappings from : Rhea
 * 
 * @author anwesha
 * @author DeniseSl22
 * 
 */

public class IntdbBuilder {
	static String dateStr = new SimpleDateFormat("yyyyMMdd").format(new Date());
	static float newpercent = 0;
	private static int mappingrows = 295919;
	private static int mappingrow = 0;
	private static File mappingfile = new File("rhea2xrefs.txt");
	private static String DB_NAME = "rhea_interactions";
	private static String OUTPUT_DIR = System.getProperty("user.home");
	private Xref idRhea;
	private Xref idCrossRefs;
	private static GdbConstruct newDb;
	private String identifier;
	private String direction;
	private String datasource;
	private String mainref;
	private static String dbname = "";
	private static boolean downloadMapping; 

	/**
	 * command line arguments: 0 - Full path of the interactions database to be
	 * created (eg: /home/user/interactions.bridge) 1 - Boolean download mapping
	 * (true/false) [Note : the download requires internet connection]
	 * 2 - Full path of the previous interactions database to run the QC
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("Process started!");
		try {
			if (!args[0].isEmpty()) {
				dbname = args[0];
				downloadMapping = Boolean.parseBoolean(args[1]);
			} else {
				dbname = OUTPUT_DIR + System.getProperty("file.separator")
						+ DB_NAME + "_" + dateStr + ".bridge";
				downloadMapping = false;
				System.out.println("Using previously downloaded mapping");
			}

		} catch (Exception e) {
			System.out.println("Using defaults name and output directory");
		}
		IntdbBuilder intdb = new IntdbBuilder(); //comment from here to 
		if (downloadMapping) {
			intdb.downloadMapping();
		}
		try {
			String newDbname = dbname;
			newDb = GdbConstructImpl3.createInstance(newDbname,
					new DataDerby(), DBConnector.PROP_RECREATE);
			InputStream mapping = new FileInputStream(mappingfile);
			intdb.init(newDb);
			intdb.run(mapping);
			intdb.done();
		} catch (Exception e) {
			System.out.println("Interaction Database creation failed!");
			e.printStackTrace();
		} // here, if you want to compare two files with each other (without creating a new mapping file.

		try {
			BridgeQC main = new BridgeQC (new File(args[2]),new File(args[0]));	
			main.run();
		} catch (IDMapperException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Downloads a fresh copy of interaction mappings from Rhea
	 */
	private void downloadMapping() {
		mappingrows = 0;
		URL url;
		String inputline = "";
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
			while ((inputline = in.readLine()) != null) {
					if (!inputline.startsWith("RHEA") & inputline.length() > 0) {
					mappingrows++;
					out.write(inputline + "\n");
				}
			}
			out.close();
			in.close();
			System.out.println("Interaction Mapping Downloaded from Rhea");
			System.out.println("Rows of data in downloaded mapping file = "
					+ mappingrows);

		} catch (Exception e) {
			System.out.println("Interaction Mapping Download failed!");
			e.printStackTrace();
		}
	}

	/**
	 * Creates an empty Derby database
	 * 
	 * @throws IDMapperException
	 *             when it cannot write to the database
	 */
	private void init(GdbConstruct newDb) throws IDMapperException {
		System.out.println("Initialising Interaction Database ...");
		IntdbBuilder.newDb = newDb;
		newDb.createGdbTables();
		newDb.preInsert();
		System.out.println("Setting Basic Information ...");
		newDb.setInfo("BUILDDATE", dateStr);
		newDb.setInfo("DATASOURCENAME", "EBI-RHEA");
		newDb.setInfo("DATASOURCEVERSION", "1.0.0");
		newDb.setInfo("SERIES", "standard-interaction");
		newDb.setInfo("DATATYPE", "Interaction");

	}

	/**
	 * Parses the mapping file and populates the database
	 * 
	 * @throws MalformedURLException
	 * @throws IOException
	 * @throws IDMapperException
	 */
	private void run(InputStream input) throws MalformedURLException,
			IOException, IDMapperException {
		System.out.println("Populating Database ...");
		DataSourceTxt.init();
		mainref = "";
		String inputline;
		String[] array = new String[5];
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));

		for (int i = 0; i < mappingrows; i++) {
			mappingrow++;
			inputline = reader.readLine();
			array = inputline.split("\t");
			direction = array[1];
			identifier = array[3];
			datasource = array[4];

			if (!mainref.equalsIgnoreCase(array[0])) {
				mainref = array[0];
				idRhea = new Xref(mainref,
						DataSource.getExistingBySystemCode("Rh"));
				newDb.addGene(idRhea);
				newDb.addAttribute(idRhea, "Direction", direction);
				newDb.addLink(idRhea, idRhea);
			}
			if (mainref.equalsIgnoreCase(array[0])) {
				if (datasource.equalsIgnoreCase("EC")) {
					idCrossRefs = new Xref(identifier,
							DataSource.getExistingBySystemCode("E"));
				} else if (datasource.equals("KEGG_REACTION")) {
					idCrossRefs = new Xref(identifier,
							DataSource.getExistingBySystemCode("Rk"));
				} else if (datasource.equals("REACTOME")) {
					idCrossRefs = new Xref(identifier,
							DataSource.getExistingBySystemCode("Re"));
				} else if (datasource.equals("METACYC")) {
					idCrossRefs = new Xref(identifier,
							DataSource.getExistingBySystemCode("Mc"));
				} else if (datasource.equals("ECOCYC")) {
					idCrossRefs = new Xref(identifier,
							DataSource.getExistingBySystemCode("Eco"));

				} else if (datasource.equals("MACIE")) {
					idCrossRefs = new Xref(identifier,
							DataSource.getExistingBySystemCode("Ma"));

				} else if (datasource.equals("UNIPATHWAY")) {
					idCrossRefs = new Xref(identifier,
							DataSource.getExistingBySystemCode("Up"));

				} 
//					else if (datasource.equals("UNIPROT")) {
//					idCrossRefs = new Xref(identifier,
//							DataSource.getExistingBySystemCode("S"));
//				}
			}

			newDb.addGene(idCrossRefs);
			newDb.addLink(idRhea, idCrossRefs);

			printProgBar(mappingrow);
		}
	}

	/**
	 * Prints a progress bar
	 */
	private float printProgBar(int count) {
		StringBuilder bar = new StringBuilder("[");
		float percent = (count * 100) / mappingrows;
		for (int i = 0; i < 50; i++) {
			if (i < (percent / 2)) {
				bar.append("=");
			} else if (i == (percent / 2)) {
				bar.append(">");
			} else {
				bar.append(" ");
			}

		}
		if (percent >= 1 && percent <= 100 && percent != newpercent) {
			newpercent = percent;
			bar.append("]   " + newpercent + "%     ");
			System.out.print("\r" + bar.toString());
		}
		return newpercent;
	}

	/**
	 * Finalizes the database
	 * 
	 * @throws IDMapperException
	 */
	private void done() throws IDMapperException {
		newDb.commit();

		System.out.println("\nEND processing text file");

		System.out.println("Compacting database : " + dbname);

		System.out.println("Closing connections");

		newDb.finalize();
	}

}
