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

	private String filesep = System.getProperty("file.separator");
	private File mappingfile = new File("resources" + filesep
			+ "rhea2xrefs.txt");
	// private String[] array = new String[5];
	// private String mainref = "";
	 private Xref idRhea;
	// private String identifier;
	// private String datasource;
	// private String inputline;
	private GdbConstruct newDb;
	private String dbname;
//	private int intcount = 0;
	private List<Xref> intxrefs = new ArrayList<Xref>();

	/**
	 * @param args
	 *            command line arguments
	 * 
	 *            Commandline: - output database: .pgdb - input metabocards .txt
	 *            file
	 */
	public static void main(String[] args) {

		String dbname = args[0];
		String file = args[1];

		// InteractionMapper r2 = new InteractionMapper();
		IntdbBuilder intdb = new IntdbBuilder();

		// intdb.downloadMapping();

		try {
			GdbConstruct newDb = GdbConstructImpl3.createInstance(dbname,
					new DataDerby(), DBConnector.PROP_RECREATE);
			InputStream mapping = new FileInputStream(new File(file));
//			InputStream count = new FileInputStream(new File(file));
//			intdb.countInt(count);
			intdb.init(dbname, newDb);
			System.out.println("working");
			intdb.run(mapping);
			intdb.done();
		} catch (Exception e) {
			System.out.println("Interaction Database creation failed!");
			e.printStackTrace();
		}

	}

//	private void countInt(InputStream input) throws IOException {
//		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
//		String inputline1 = "";
//		String[] array1 = new String[5];
//		String mainref = "";
//		while ((inputline1 = reader.readLine()) != null) {
//			array1 = inputline1.split("\t");
//			if (!mainref.equalsIgnoreCase(array1[0])) {
//				intcount++;
//				mainref = array1[0];
//			}
//		}
//		System.out.println("interactions:" + intcount);
//
//	}

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
			// while ((inputLine = in.readLine()) != null) {
			for (int i = 0; i <= 10; i++) {
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

	// protected void displayrefs(){
	// for(Xref ref:idRhea){
	// System.out.println("rhea"+ref.getId());
	// }
	// }

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

		newDb.createGdbTables();
		newDb.preInsert();

		String dateStr = new SimpleDateFormat("yyyyMMdd").format(new Date());
		newDb.setInfo("BUILDDATE", dateStr);
		newDb.setInfo("DATASOURCENAME", "EBI-RHEA");
		newDb.setInfo("DATASOURCEVERSION", "23-05-2013");
		newDb.setInfo("SERIES", "standard-interaction");
		newDb.setInfo("DATATYPE", "Interaction");
		// newDb.
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
		// System.out.println("init");
		// BioDataSource.init();
		String identifier;
		String datasource;
		String mainref = "";
		String inputline;
		String[] array = new String[5];
		BufferedReader reader = new BufferedReader(
				new InputStreamReader(input));
		while ((inputline = reader.readLine()) != null) {
			array = inputline.split("\t");
			identifier = array[3];
			datasource = array[4];
			if (!mainref.equalsIgnoreCase(array[0])) {
				// intcount++;
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

			// System.out.println("interaction count: " + intcount);
			// System.out.println("intxrefs: " + intxrefs);
			// System.out.println("contains: " + intxrefs.contains(idRhea));
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