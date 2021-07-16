package AIDFD;

import AIDFD.bitset.IBitSet;
import AIDFD.bitset.LongBitSet;
import AIDFD.helpers.Cluster;
import AIDFD.helpers.FastBloomFilter;
import AIDFD.helpers.Partition;
import AIDFD.helpers.StrippedPartition;

import java.io.IOException;
import java.util.*;
import java.util.stream.DoubleStream;
import com.csvreader.CsvReader;

public class AIDFD {
	
	public static final String INPUT_RELATION_FILE = "input file";
	public static final String INPUT_USE_BLOOMFILTER = "use bloomfilter";
	public static final String INPUT_CHECK_CORRECTNESS = "check correctness";
	public static final String INPUT_UNTIL_ITERATION_K = "until iteration k";
	public static final String INPUT_TIMEOUT = "timeout [s]";
	public static final String INPUT_NEG_COVER_THRESH = "neg-cover growth thresh [x/1000000]";
	public static final String INPUT_NEG_COVER_WINDOW_SIZE = "neg-cover growth window size";


	/* Configuration parameters */
	private boolean useBloomfilter;
	private boolean checkCorrectness;
	private int untilIterationK = -1;
	private int timeout = -1;
	private double negCoverThresh = -1;
	private int negCoverWindowSize = 1;
	
	/* Data Structures */
	/** For each cell, stores a reference to the cluster */
	private ArrayList<Cluster[]> clusters;
	/** For each cell, stores its index in its cluster */
	private ArrayList<int[]> clusterIndices;
	/** For each tuple, stores whether it still needs to be looked at */

	private String path;
	private int numberTuples;
	private int numberAttributes;

	private long startTime;
	private PrefixTreeResultGen resultGenerator;
	private FastBloomFilter bloomFilter;
	private double[] lastNegCoverRatios;
	private IBitSet constantColumns;

	public void execute() throws IOException {
		startTime = System.currentTimeMillis();
		resultGenerator = new PrefixTreeResultGen(path);

		readInput(path);

		checkConstantColumns();
		checkClusters();

		resultGenerator.generateResults();
		
		System.out.println("Finished execution after " + (System.currentTimeMillis() - startTime) + "ms");
	}

	@SuppressWarnings("unchecked")
	private void readInput(String path) throws IOException {
		clusters = new ArrayList<>();
		clusterIndices = new ArrayList<>();

		long time = System.currentTimeMillis();
        CsvReader csvReader = new CsvReader(path);
        csvReader.readRecord();
        String[] head = csvReader.getRawRecord().split(",");
        int numberOfColumns = head.length;
		Map<String, Cluster> map[] = new HashMap[numberOfColumns];
		for (int i = 0; i < numberOfColumns; ++i) {
			map[i] = new HashMap<String, Cluster>();
		}
		int lineNumber = 0;
		while (csvReader.readRecord()) {
			String rawRecord = csvReader.getRawRecord();
			String[] splited = rawRecord.split(",");
			List<String> line = new ArrayList<>();
			int len = splited.length;
			for (int i = 0; i < len; i++){
				line.add(splited[i]);
			}

			Cluster[] lineClusters = new Cluster[numberOfColumns];
			int[] lineClusterIndices = new int[numberOfColumns];

			for (int i = 0; i < numberOfColumns; ++i) {
				Cluster cluster;
				if (map[i].containsKey(line.get(i))) {
					cluster = map[i].get(line.get(i));
				} else {
					cluster = new Cluster(i);
					map[i].put(line.get(i), cluster);
				}
				lineClusters[i] = cluster;
				lineClusterIndices[i] = cluster.size();
				cluster.add(lineNumber);
			}

			clusters.add(lineClusters);
			clusterIndices.add(lineClusterIndices);

			++lineNumber;
		}

		numberTuples = lineNumber;
		numberAttributes = numberOfColumns;

		if (checkCorrectness) {
			StrippedPartition.columns = new ArrayList[numberAttributes];
			for (int i = 0; i < numberAttributes; ++i) {
				StrippedPartition.columns[i] = new ArrayList<Partition>();
				for (Cluster c : map[i].values()) {
					if (c.size() > 1)
						StrippedPartition.columns[i].add(new Partition(c));
				}
			}

			StrippedPartition.clusters = clusters;
		}

		System.out.println("Reading data finished after " + (System.currentTimeMillis() - time) + "ms");
		System.out.println("Initial neg-cover size: " + resultGenerator.getNegCoverSize());
	}

	private int pseudoRandom(int n, int k) {
		int prim = 10619863 % n;
		return (prim * k) % n;
	}

	private boolean makeCheck(int tuple, int k) {
		boolean madeCheck = false;

		Cluster[] tupleClusters = clusters.get(tuple);
		int[] tupleIndices = clusterIndices.get(tuple);

		for (int i = 0; i < numberAttributes; i++) {
			// we don't need to sample inside a constant column
			if(constantColumns.get(i))
				continue;

			Cluster cluster = tupleClusters[i];
			int index = tupleIndices[i];

			// all tuples in question are checked already
			if (index < k) {
				continue;
			}

			int otherTuple = cluster.get(pseudoRandom(index, k));

			// use bloomFilter to avoid duplicate checks
			if(useBloomfilter && bloomFilter.containsAndAdd(((long)tuple << 32) + otherTuple)) {
			 continue;
			}

			madeCheck = true;

			IBitSet bitset = LongBitSet.FACTORY.create(numberAttributes);
			Cluster[] otherTupleClusters = clusters.get(otherTuple);

			for (int j = 0; j < numberAttributes; j++) {
				bitset.set(j, tupleClusters[j] == otherTupleClusters[j]);
			}

			resultGenerator.add(bitset);
		}

		return madeCheck;
	}

	private void checkConstantColumns()  {
		constantColumns = LongBitSet.FACTORY.create();
		for (int i = 0; i < numberAttributes; ++i) {
			if(clusters.get(0)[i].size() == numberTuples) {
				constantColumns.set(i);
			}
		}
		resultGenerator.setConstantColumns(constantColumns);
	}

	private void checkClusters() {
		if (useBloomfilter) {
		bloomFilter = new FastBloomFilter(numberTuples * numberTuples / 10, 2);
		}

		if(negCoverThresh >= 0) {
			lastNegCoverRatios = new double[negCoverWindowSize];
			Arrays.fill(lastNegCoverRatios, 1);
		}

		double negCoverRatio;
		boolean madeCheck;

		// Iterate over k
		int k = 0;
		do {
			k++;
			long time = System.currentTimeMillis();
			int lastNegCoverSize = resultGenerator.getNegCoverSize();
			madeCheck = false;

			for (int tuple = 0; tuple < numberTuples; tuple++) {
				madeCheck |= makeCheck(tuple, k);
				// If it didn't do anything in this iteration, it won't in the next either
			}
			
			negCoverRatio = lastNegCoverSize > 0.0
				? (double)(resultGenerator.getNegCoverSize() - lastNegCoverSize) / lastNegCoverSize
			: Double.MAX_VALUE;

			System.out.println("k=" + k +
				" in " + (System.currentTimeMillis() - time) + "ms -" +
				" negCoverSize: " + resultGenerator.getNegCoverSize() + "," +
				" negCoverRatio: " + negCoverRatio);
		} while((useBloomfilter || madeCheck) && k < numberTuples && !terminationCriteriaMet(k, negCoverRatio));
	}

	private boolean terminationCriteriaMet(int k, double negCoverRatio) {
		if (untilIterationK >= 0 && k >= untilIterationK) {
			System.out.println("Termination criterion met: until iteration k");
			return true;
		}
		
		if (timeout >= 0) {
			long timeDiff = System.currentTimeMillis() - startTime;
			if(timeDiff / 1000 >= timeout) {
				System.out.println("Termination criterion met: timeout");
				return true;
			}
		}
		
		if (negCoverThresh >= 0.0) {
			int index = k % negCoverWindowSize;
			lastNegCoverRatios[index] = negCoverRatio;
			double averageRatio = (DoubleStream.of(lastNegCoverRatios).sum()) / negCoverWindowSize;
			if (averageRatio <= negCoverThresh) {
				System.out.println("Termination criterion met: neg-cover growth ratio");
				return true;
			}
		}

		return false;
	}

	public static void main(String[] args) throws IOException {
		String pwd = System.getProperty("user.dir");
		AIDFD fd = new AIDFD();
		fd.path = pwd + "/data/horse.csv";
		fd.execute();
	}








}
