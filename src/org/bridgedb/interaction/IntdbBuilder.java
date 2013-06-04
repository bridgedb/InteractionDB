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

public class IntdbBuilder {

	private static String filesep = System.getProperty("file.separator");
	private static File mappingfile = new File("resources" + filesep
			+ "rhea2xrefs.txt");
	private Xref idRhea;
	private GdbConstruct newDb;
	private List<Xref> intxrefs = new ArrayList<Xref>();

	/**
	 * command line arguments
	 * 1 - absolute path of the interactions database to be created (for eg: /home/anwesha/interactions.bridge)
	 * 2 - absolute path of the mapp
	 * @param args
	 */
	public static void main(String[] args) {

		String dbname = args[0];
//		String file = args[1];

		IntdbBuilder intdb = new IntdbBuilder();

		intdb.downloadMapping();

		try {
			GdbConstruct newDb = GdbConstructImpl3.createInstance(dbname,
					new DataDerby(), DBConnector.PROP_RECREATE);
			InputStream mapping = new FileInputStream(mappingfile);
			intdb.init(dbname, newDb);
			intdb.run(mapping);
			intdb.done();
		} catch (Exception e) {
			System.out.println("Interaction Database creation failed!");
			e.printStackTrace();
		}

	}

	/**
	 * Downloads a fresh copy of the maaping from Rhea
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
//			while ((inputline = in.readLine()) != null) {
			for (int i =0; i<=50;i++){
			inputline = in.readLine();
				if (!inputline.startsWith("RHEA") & inputline.length() > 0) {
					out.write(inputline + "\n");
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

	/**
	 * Creates an empty Derby database
	 * 
	 * @throws IDMapperException
	 *             when it cannot write to the database
	 */
	private void init(String dbname, GdbConstruct newDb)
			throws IDMapperException {

		this.newDb = newDb;
		// this.dbname = dbname;

		newDb.createGdbTables();
		newDb.preInsert();

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
	private void run(InputStream input) throws MalformedURLException,
			IOException, IDMapperException {
		String identifier;
		String datasource;
		String mainref = "";
		String inputline;
		String[] array = new String[5];
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		while ((inputline = reader.readLine()) != null) {
			array = inputline.split("\t");
			identifier = array[3];
			datasource = array[4];
			if (!mainref.equalsIgnoreCase(array[0])) {
				mainref = array[0];
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
		}
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

}
