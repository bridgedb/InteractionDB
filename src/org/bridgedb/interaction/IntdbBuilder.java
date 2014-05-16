package org.bridgedb.interaction;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bridgedb.IDMapperException;
import org.bridgedb.Xref;
import org.bridgedb.bio.BioDataSource;
import org.bridgedb.rdb.construct.DBConnector;
import org.bridgedb.rdb.construct.DataDerby;
import org.bridgedb.rdb.construct.GdbConstruct;
import org.bridgedb.rdb.construct.GdbConstructImpl3;

/**
 * This program creates an Interaction annotation BridgeDB derby database
 * mappings from : Rhea
 * 
 * @author anwesha
 * 
 */
@SuppressWarnings("deprecation")
public class IntdbBuilder {
	static String dateStr = new SimpleDateFormat("yyyyMMdd").format(new Date());
	static float newpercent = 0;
	private static int mappingrows = 0;
	private static int mappingrow = 0;
	private static File mappingfile = new File("rhea2xrefs.txt");
	private static String dbname = "rhea_interactions";
	private static String outputdir = System.getProperty("user.home");
	private Xref idRhea;
	private static GdbConstruct newDb;
	private List<Xref> intxrefs = new ArrayList<Xref>();
	private String identifier;
	private String datasource;
	private String mainref;

	/**
	 * command line arguments: 1 - Name of the interactions database to be
	 * created (eg: interactions) 2 - Output Directory (eg: /home/anwesha/test)
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try{
			if (!args[0].isEmpty()) {
				dbname = args[0];
			} 
			if (!args[1].isEmpty()) {
				outputdir = args[1];
			} 
		}catch (Exception e){
			System.out.println("Using defaults name and output directory");
		}
		IntdbBuilder intdb = new IntdbBuilder();

		intdb.downloadMapping();

		try {
			String newDbname = outputdir+System.getProperty("file.separator")+dbname + "_" + dateStr
					+ ".bridge";
			newDb = GdbConstructImpl3.createInstance(newDbname, new DataDerby(), DBConnector.PROP_RECREATE);
			InputStream mapping = new FileInputStream(mappingfile);
			intdb.init(newDb);
			intdb.run(mapping);
			intdb.done();
		} catch (Exception e) {
			System.out.println("Interaction Database creation failed!");
			e.printStackTrace();
		}

	}

	/**
	 * Downloads a fresh copy of interaction mappings from Rhea
	 */
	private void downloadMapping() {
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
				mappingrows++;
				if (!inputline.startsWith("RHEA") & inputline.length() > 0) {
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
		BioDataSource.init();
		mainref = "";
		String inputline;
		String[] array = new String[5];
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		while ((inputline = reader.readLine()) != null) {
//			 for (int i = 0; i <= 50; i++) {
			mappingrow++;
			inputline = reader.readLine();
			array = inputline.split("\t");
			identifier = array[3];
			datasource = array[4];
			if (!mainref.equalsIgnoreCase(array[0])) {
				mainref = array[0];
				// System.out.println(mainref + "added");
				intxrefs.clear();
				idRhea = new Xref(mainref, BioDataSource.RHEA);
				intxrefs.add(new Xref(mainref, BioDataSource.RHEA));
			}

			if (mainref.equalsIgnoreCase(array[0])) {
				if (datasource.equalsIgnoreCase("EC")) {
					intxrefs.add(new Xref(identifier, BioDataSource.ENZYME_CODE));
				} else if (datasource.equals("KEGG_REACTION")) {
					intxrefs.add(new Xref(identifier,
							BioDataSource.KEGG_REACTION));

				} else if (datasource.equals("REACTOME")) {
					intxrefs.add(new Xref(identifier, BioDataSource.REACTOME));

				} else if (datasource.equals("METACYC")) {
					intxrefs.add(new Xref(identifier, BioDataSource.BIOCYC));

				} else if (datasource.equals("ECOCYC")) {
					intxrefs.add(new Xref(identifier, BioDataSource.BIOCYC));

				} else if (datasource.equals("MACIE")) {
					intxrefs.add(new Xref(identifier, BioDataSource.MACIE));

				} else if (datasource.equals("UNIPATHWAY")) {
					intxrefs.add(new Xref(identifier, BioDataSource.UNIPATHWAY));

				} else if (datasource.equals("UNIPROT")) {
					intxrefs.add(new Xref(identifier, BioDataSource.UNIPROT));

				}
			}
			Xref ref = idRhea;
			newDb.addGene(ref);
			newDb.addLink(ref, ref);
			for (Xref right : intxrefs) {
				newDb.addGene(right);
				newDb.addLink(ref, right);
			}
			printProgBar(mappingrow);
		}
	}

	/**
	 * Prints a progress bar
	 */
	private static void printProgBar(int count) {
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
			float newpercent = percent;
			bar.append("]   " + newpercent + "%     ");
			System.out.print("\r" + bar.toString());
		}
	}

	/**
	 * Finalizes the database
	 * 
	 * @throws IDMapperException
	 */
	private void done() throws IDMapperException {
		newDb.commit();

		System.out.println("\nEND processing text file");

		System.out.println("Compacting database");

		System.out.println("Closing connections");

		newDb.finalize();
	}

}
