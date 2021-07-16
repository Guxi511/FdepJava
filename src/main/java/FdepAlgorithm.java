import tools.structure.FDTree;
import tools.structure.FDTreeElement;
import tools.CSVUtil;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;


public class FdepAlgorithm {

    public static String path ;
    private String tableName;
    private List<String> columnNames;
    private int numberAttributes;
    private List<List<String>> tuples;
    private FDTree negCoverTree;
    private FDTree posCoverTree;


    public void execute() throws FileNotFoundException {
        initialize();
        negativeCover();
        this.tuples = null;
        posCoverTree = new FDTree(numberAttributes);
        posCoverTree.addMostGeneralDependencies();
        BitSet activePath = new BitSet();
        calculatePositiveCover(negCoverTree, activePath);
//		posCoverTree.filterGeneralizations();
        // addAllDependenciesToResultReceiver();
    }

    private void initialize() throws FileNotFoundException {
        loadData();
    }

    /**
     * Calculate a set of fds, which do not cover the invalid dependency lhs -> a.
     */
    private void specializePositiveCover(BitSet lhs, int a) {
        BitSet specLhs = new BitSet();

        while (posCoverTree.getGeneralizationAndDelete(lhs, a, posCoverTree, 0, specLhs)) {
            for (int attr = this.numberAttributes; attr > 0; attr--) {
                if (!lhs.get(attr) && (attr != a)) {
                    specLhs.set(attr);
                    if (!posCoverTree.containsGeneralization(specLhs, a, 0)) {
                        posCoverTree.addFunctionalDependency(specLhs, a);
                    }
                    specLhs.clear(attr);
                }
            }
            specLhs = new BitSet();
        }
    }

    private void calculatePositiveCover(FDTreeElement negCoverSubtree, BitSet activePath) {
        for (int attr = 1; attr <= numberAttributes; attr++) {
            if (negCoverSubtree.isFd(attr - 1)) {
                specializePositiveCover(activePath, attr);
            }
        }

        for (int attr = 1; attr <= numberAttributes; attr++) {
            if (negCoverSubtree.getChild(attr - 1) != null) {
                activePath.set(attr);
                this.calculatePositiveCover(negCoverSubtree.getChild(attr - 1), activePath);
                activePath.clear(attr);
            }
        }
    }


    /**
     * Calculate the negative Cover for the current relation.
     */
    private void negativeCover() {
        negCoverTree = new FDTree(this.numberAttributes);
        negCoverTree.fds_num = 0;
        for (int i = 0; i < tuples.size(); i++) {
            for (int j = i + 1; j < tuples.size(); j++) {
                violatedFds(tuples.get(i), tuples.get(j));
            }
        }
        this.negCoverTree.filterSpecializations();
    }


    /**
     * Find the least general functional dependencies violated by t1 and t2
     * and add update the negative cover accordingly.<br/>
     * Note: t1 and t2 must have the same length.
     *
     * @param t1 An ObjectArrayList with the values of one entry of the relation.
     * @param t2 An ObjectArrayList with the values of another entry of the relation.
     */
    private void violatedFds(List<String> t1, List<String> t2) {
        BitSet equalAttr = new BitSet();
        equalAttr.set(1, this.numberAttributes + 1);
        BitSet diffAttr = new BitSet();
        for (int i = 0; i < t1.size(); i++) {
            Object val1 = t1.get(i);
            Object val2 = t2.get(i);
            // Handling of null values. Currently assuming NULL values are equal.
            if (val1 == null && val2 == null) {
                continue;
            } else if ((val1 == null && val2 != null) || !(val1.equals(val2))) {
                // BitSet start with 1 for first attribute
                diffAttr.set(i + 1);
            }
        }
        equalAttr.andNot(diffAttr);
        for (int a = diffAttr.nextSetBit(0); a >= 0; a = diffAttr.nextSetBit(a + 1)) {
            negCoverTree.addFunctionalDependency(equalAttr, a);
        }
    }


    /**
     * Fetch the data from the database and keep it as List of Lists.
     *
     */

    private void loadData() throws FileNotFoundException {
        tuples = new ArrayList<>();
        CSVUtil csvRead = new CSVUtil();
        tuples = csvRead.readCSV(path);
        columnNames = tuples.get(0);
        tuples.remove(0);
        //System.out.println(columnNames);
        this.numberAttributes = columnNames.size();
        /*
        for(int i = 0;i<tuples.size();i++){
            System.out.println(tuples.get(i));
        }

         */
    }

    public static void main(String arg[]) throws FileNotFoundException {
        long t1 = System.currentTimeMillis();
        FdepAlgorithm algo = new FdepAlgorithm();
        String pwd = System.getProperty("user.dir");
        FdepAlgorithm.path = pwd + "/data/iris.csv";
        algo.execute();
        algo.posCoverTree.printDependencies();
        long t2 = System.currentTimeMillis();
        System.out.println(algo.posCoverTree.fds_num);
        System.out.println("Total time:"+(double)(t2-t1)/1000+"s");
    }


    /*
    private void addAllDependenciesToResultReceiver(FDTreeElement fds, BitSet activePath) throws CouldNotReceiveResultException, ColumnNameMismatchException {
        if (this.fdResultReceiver == null) {
            return;
        }
        for (int attr = 1; attr <= numberAttributes; attr++) {
            if (fds.isFd(attr - 1)) {
                int j = 0;
                ColumnIdentifier[] columns = new ColumnIdentifier[activePath.cardinality()];
                for (int i = activePath.nextSetBit(0); i >= 0; i = activePath.nextSetBit(i + 1)) {
                    columns[j++] = this.columnIdentifiers.get(i - 1);
                }
                ColumnCombination colCombination = new ColumnCombination(columns);
                FunctionalDependency fdResult = new FunctionalDependency(colCombination, columnIdentifiers.get(attr - 1));
//				System.out.println(fdResult.toString());
                fdResultReceiver.receiveResult(fdResult);
            }
        }

        for (int attr = 1; attr <= numberAttributes; attr++) {
            if (fds.getChild(attr - 1) != null) {
                activePath.set(attr);
                this.addAllDependenciesToResultReceiver(fds.getChild(attr - 1), activePath);
                activePath.clear(attr);
            }
        }
    }
    */

    /**
     * Add all functional Dependencies to the FunctionalDependencyResultReceiver.
     * Do nothing if the object does not have a result receiver.
     *
     * @throws CouldNotReceiveResultException
     * @throws ColumnNameMismatchException 
     */
    /*
    private void addAllDependenciesToResultReceiver() throws CouldNotReceiveResultException, ColumnNameMismatchException {
        if (this.fdResultReceiver == null) {
            return;
        }
        this.addAllDependenciesToResultReceiver(posCoverTree, new BitSet());
    }

	@Override
	public String getAuthors() {
		return "Jannik Marten, Jan-Peer Rudolph";
	}

	@Override
	public String getDescription() {
		return "Dependency Induction-based FD discovery";
	}
	*/
}
