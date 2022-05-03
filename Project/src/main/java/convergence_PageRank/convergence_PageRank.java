package convergence_PageRank;

//import com.clearspring.analytics.util.Lists;
//import org.apache.commons.math3.stat.correlation.KendallsCorrelation;
//import org.apache.parquet.filter2.predicate.Operators;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
// import org.graphframes.lib.PageRank;
// import org.spark_project.jetty.util.ArrayUtil;

// open and reading files
import java.io.*;
import java.util.*;

// import static java.lang.Math.round;
import static jdk.nashorn.internal.objects.NativeMath.round;
import static org.apache.spark.sql.functions.desc;

// import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class convergence_PageRank{

		// create the graph containing edges and the nodes to be used for the calculation of PageRank
		public static GraphFrame createGraph(JavaSparkContext ctx, SQLContext sqlCtx) {

			// vertices reading

			// list containing the vertexes
			java.util.List<Row> vertices_list = new ArrayList<>();

			try {
				File myObj = new File("/Users/pietro/Desktop/SDM/LAB 2/KnowledgeGraphLab/SparkGraphXassignment/src/main/resources/wiki-vertices.txt");
				Scanner myReader = new Scanner(myObj);
				// adding rows [ID, name] to the list
				while (myReader.hasNextLine()) {
					String data = myReader.nextLine();
					// sperating ID and name
					String[] list = data.split("\t", 2);
					// adding the new row to the list of vertices
					vertices_list.add(RowFactory.create(list[0], list[1]));
				}
				myReader.close();
			} catch (FileNotFoundException e) {
				System.out.println("An error occurred.");
				e.printStackTrace();
			}
			// System.out.println(vertices_list);
			JavaRDD<Row> vertices_rdd = ctx.parallelize(vertices_list);

			StructType vertices_schema = new StructType(new StructField[]{
					new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
					new StructField("name", DataTypes.StringType, true, new MetadataBuilder().build())
			});

			Dataset<Row> vertices = sqlCtx.createDataFrame(vertices_rdd, vertices_schema);

			// edges reading
			// list containing the edges
			java.util.List<Row> edges_list = new ArrayList<>();

			try {
				File myObj = new File("/Users/pietro/Desktop/SDM/LAB 2/KnowledgeGraphLab/SparkGraphXassignment/src/main/resources/wiki-edges.txt");
				Scanner myReader = new Scanner(myObj);

				// adding rows [source, destination] to the list
				while (myReader.hasNextLine()) {
					String data = myReader.nextLine();
					// sperating ID and name
					String[] list = data.split("\t");
					// adding the new row to the list of vertices
					edges_list.add(RowFactory.create(list[0], list[1]));
				}
				myReader.close();
			} catch (FileNotFoundException e) {
				System.out.println("An error occurred.");
				e.printStackTrace();
			}

			// System.out.println(edges_list);
			JavaRDD<Row> edges_rdd = ctx.parallelize(edges_list);

			StructType edges_schema = new StructType(new StructField[]{
					new StructField("src", DataTypes.StringType, true, new MetadataBuilder().build()),
					new StructField("dst", DataTypes.StringType, true, new MetadataBuilder().build())
			});

			Dataset<Row> edges = sqlCtx.createDataFrame(edges_rdd, edges_schema);

			// printing the graph
			// System.out.println(gf);
			// gf.edges().show();
			// gf.vertices().show();


			return GraphFrame.apply(vertices, edges);

		}


		// calculate the top 10 elements in the pageRank using the pageRank() method
		// Parameters: sample graph; reset probability value (1-dumping factor) and maximum number of iterations are
		// needed by pageRank(); id is set to true if instead of the name of the page you want to retrieve its id.
		// Return a Dataset of the ranking of the top 10 pages (id or the name) with their pageRank score.
		public static Dataset computePageRankTop10(GraphFrame gf, double resProb, int maxIt, boolean id ) {
			GraphFrame pr = gf.pageRank().resetProbability(resProb).maxIter(maxIt).run();

			if(id){ return pr.vertices().select("id", "pagerank").sort(desc("pagerank")).limit(10); }
			else{ return pr.vertices().select("name", "pagerank").sort(desc("pagerank")).limit(10); }
		}


		// convert List<Row> into double[]
		public static double[] Row2Double(List<Row> Rows, boolean alreadyDouble){
			double[] Doubles = new double[Rows.size()];
			for(int p=0; p<Rows.size(); p++){
				// if the elements are already doubles
				if(alreadyDouble){
					double d = (double) Rows.get(p).get(0);
					Doubles[p] = d;
				}
				// if the elements are to be converted into doubles
				else{
					String string = (String) Rows.get(p).get(0);
					double d = Double.parseDouble(string);
					Doubles[p] = d;
				}
			}
			return Doubles;
		}

		// helper to calculate the Rank Biased Overlap
		public static double helper(double ret, int i, int d, double[] a, double[] b, double p){
			List<Double> l1list = new ArrayList<>();
			List<Double> l2list = new ArrayList<>();
			for(int j = 0; j < i; j++) {
				l1list.add(a[j]);
				l2list.add(b[j]);
			}
			l1list.retainAll(l2list);
			double a_d = (double) l1list.size()/i;
			double term = Math.pow(p, i) * a_d;
			if (d == i){
				return ret + term;
			}
			return helper( (ret + term), i + 1, d, a, b, p);
		}

		// calculate the Rank Biased Overlap metric over two rankings a and b with p in (0,1) defining
		// the weight of the top positions.
		public static double RBO(double[] a, double[] b, double p){
			int k = a.length;

			List<Double> l1list = new ArrayList<>();
			List<Double> l2list = new ArrayList<>();
			for(int j = 0; j < a.length; j++) {
				l1list.add(a[j]);
				l2list.add(b[j]);
			}
			l1list.retainAll(l2list);
			int x_k = l1list.size();
			double summation = helper(0, 1, k, a, b, p);
			System.out.println((( (float) (x_k)/k) * Math.pow(p, k)) + ((1-p)/p * summation));
			return (( (float) (x_k)/k) * Math.pow(p, k)) + ((1-p)/p * summation);
		}

		// Method to calculate the similarity btw one pageRank and some others.
		// Parameter: list of datasets containing the pageRanks to be analized. In the first position there
		// is the current one, the others follow. Alpha is a parameter in [0,1] that determine the weight given
		// to the order of the ids and to the numerical differences btw the scores.
		// The similarity is defined as alpha*(orderSimilarity) + (1-alpha)*(1-numericalChanges).
		// orderSimilarity is defined as the RBO similarity
		// numericalChange is the sum of the squares of the scores of the current pagerank minus the scores of
		// the same page in the neighbor (0 if not present). An example follows:
		// Current PageRank: [(1, score1), (2,score2), (3, score3)]
		// Neigbhor PageRank: [(1, score1neig), (2,score2neig), (4, score4neig)].
		// numericalChange = (score1 - score1neig)^2 + (score2 - score2neig)^2 + (score3 - 0)^2 [page with id=3 not in neig]
		// then normalized for the sum of the squares of the scores in the current (so that it goes from 0 to 1)
		// numericalChange = numericalChange / (score1^2 + score2^2 + score3^2).

		public static double similarity(ArrayList<Dataset> PageRanks, double alpha){
			// select the pages' Ids of the pageRank I'm evaluating now
			List  currentIds = PageRanks.get(0).select("id").collectAsList();
			List  currentScores = PageRanks.get(0).select("pagerank").collectAsList();
			// list of values of orderSimilarities
			double[] orderSimilarity = new double[currentIds.size()];
			// list of values of numericalChanges
			double[] numericalChange = new double[currentIds.size()];

			// array with the similarity for each neigbhor
			double[] similarities = new double[PageRanks.size()-1];

			// We need to convert the ids into doubles for managing them
			double[] currentIdsDouble = Row2Double(currentIds, false);
			double[] currentScoresDouble = Row2Double(currentScores, true);

			// now we are ready to do the same for the neighbor and calculate the distances we need.
			// calculate orderSimilarity and numericalChanges btw the current pageRank and the other 4
			for(int k=1; k<PageRanks.size(); k++){
				//ids and scores of the neighbor I'm considering now
				// if we do them all together, we can use way less time
				List neigbhorIds = PageRanks.get(k).select("id").collectAsList();
				List  neigbhorScores = PageRanks.get(k).select("pagerank").collectAsList();

				// We need to convert these lists into vectors of doubles (to use KendallsCorrelation and to do the sums)
				double[] neigbhorIdsDouble = Row2Double(neigbhorIds, false);
				double[] neigbhorScoresDouble = Row2Double(neigbhorScores, true);

				// variable to calculate the numericalChange
				double sumOfSquaresCurr = 0;
				double sumOfDiffScores = 0;
				for(int iterCurr=0; iterCurr<currentIds.size(); iterCurr++){
					// numericalChanges
					// for each Id in the neighbor, see if it is also in the current ones
					for (int iterNeig=0; iterNeig<neigbhorIds.size(); iterNeig++){
						sumOfSquaresCurr = sumOfSquaresCurr + currentScoresDouble[iterCurr]*currentScoresDouble[iterCurr]; // increment the total
						// if the element in curr is also in the neigbhor, add (scoreCurr - scoreNeig)^2
						if (currentIdsDouble[iterCurr] == neigbhorIdsDouble[iterNeig]){
							double diffOfScores = currentScoresDouble[iterCurr] - neigbhorScoresDouble[iterNeig]; //diff of the scores
							sumOfDiffScores = sumOfDiffScores + diffOfScores*diffOfScores; //use the power of 2
							/*System.out.println(String.format("FOUNDED %f And %f, their diff is %f ",currentIdsDouble[iterCurr], neigbhorIdsDouble[iterNeig], diffOfScores ));
							System.out.println(String.format("they have scores %f And %f ",currentScoresDouble[iterCurr], neigbhorScoresDouble[iterNeig]));
							System.out.println("sumOfDiffScores: ");
							System.out.println(sumOfDiffScores);
							System.out.println("\n");*/
						}
					}
					// if the el in curr has not been founded in the neig, add (scoreCurr-0)^2
					sumOfDiffScores = sumOfDiffScores + currentScoresDouble[iterCurr]*currentScoresDouble[iterCurr];
				}
				// calculate and add numericalChange
				numericalChange[k] = sumOfDiffScores/sumOfSquaresCurr;
				//System.out.println("NUMCHANGE: ");
				//System.out.println(numericalChange[k]);
				// calculate the orderSimilarity
				orderSimilarity[k] = RBO(currentIdsDouble, neigbhorIdsDouble, 0.9);
				// System.out.println("SIMILARITY: ");
				// System.out.println(orderSimilarity[k]);
				similarities[k-1] = alpha*(orderSimilarity[k]) + (1-alpha)*(1-numericalChange[k]);
				System.out.println(String.format("numChang: %f\norderSim: %f\nalpha: %f", numericalChange[k], orderSimilarity[k], alpha));
				System.out.println(String.format("SIMILARITY btw current and neig %d:  %f", k, similarities[k-1]));
			}
			// PageRanks.get(0).show();
			// PageRanks.get(1).show();

			// the final value will be the average of the similarities
			return Arrays.stream(similarities).average().orElse(Double.NaN);
		}

		// defining class of object that will store the pageRanks and the time spend to find it
		public static class SpacePoints{
			Dataset PR;
			long time;
			double similarityValue;

			//constructor
			public SpacePoints(Dataset PageRank){
				PR = PageRank;
				similarityValue = 0;
			}

			public void setPR(Dataset PageRank) {
				PR = PageRank;
			}
			public void setTime(long Time) {
				time = Time;
			}
			void setSimilarityValue(double SimilarityValue) {
				similarityValue = SimilarityValue;
			}

			public Dataset getPR() {
				return PR;
			}
			public long getTime() {
				return time;
			}
			public double getSimilarityValue() {
				return similarityValue;
			}
		}

		// class to contain the 2 parameters of interest
		public static class parameters{
			double resProb;
			int maxIter;

			//constructor
			public parameters(double ResProb, int MaxIter){
				resProb = ResProb;
				maxIter = MaxIter;
			}
			public parameters(){
			}

			public void setResProb(double ResProb) {
				resProb = ResProb;
			}
			public void setMaxIter(int MaxIter) {
				maxIter = MaxIter;
			}

			public double getResProb() {
				return resProb;
			}
			public int getMaxIter() {
				return maxIter;
			}
		}

		// calculate pageranks and timeElapsed in multiple points (pairs of resProb and maxIter)
		// this function could also print the optimal page rank, anyways we cast it to only return the parameters
		// maxIterValues contain the ranges of the search space (2 elements[min, max])
		// resProbValues contain the ranges and the granularity of the search space (3 elements[min, max, granularity])
		// threshold represents the similarity at which we consider the algorithm to be converging.

		public static parameters hyperparameters(JavaSparkContext ctx, SQLContext sqlCtx,
												 double threshold, int[] maxIterValues,
												 double[] resProbValues){
			// create the graph
			GraphFrame gf = createGraph(ctx, sqlCtx);

			// iterate over the possible values of the hyperparameters
			// list of resProbs values
			List<Double> resProbs = new ArrayList<>();
			for(double i = resProbValues[0]; i <= resProbValues[1]+resProbValues[2]; i = i + resProbValues[2] ){
				double v = Math.round(i * 100);
				resProbs.add(v/100);
			}

			// list of maxIterations values
			int minI = maxIterValues[0];
			int maxI = maxIterValues[1];
			List<Integer> maxIts = IntStream.rangeClosed(minI, maxI).boxed().collect(Collectors.toList());

			// initialize the list with the results
			// ArrayList<ArrayList<SpacePoints>> results = new ArrayList<>();
			Map<Double, Map<Integer, SpacePoints>> results = new HashMap<>();
			// results.put(0.75, new HashMap<>());
			// results.get(0.75).get(15);


			// iterate over the reset probabilities to calculate multiple rankings
			for (Double resProb : resProbs) {
				results.put(resProb, new HashMap<>());
				System.out.println("MODELLO CON RESAMPLING PROB");
				System.out.println(resProb);
				// iterate over the maximum iterations
				for (Integer maxIt : maxIts) {
					System.out.println("MODELLO CON MAXITER");
					System.out.println(maxIt);
					long startTime = System.nanoTime();
					// adding the top 10 pages to the list
					results.get(resProb).put(maxIt, new SpacePoints(computePageRankTop10(gf, resProb, maxIt, true)));
					// results.get(i).add(new SpacePoints(computePageRankTop10(gf, resProbs.get(i), maxIts.get(j), true)));
					// PRs.add(computePageRankTop10(gf, resProb, maxIt, true));
					long endTime = System.nanoTime();
					long timeElapsed = endTime - startTime;
					// adding the time to the list
					results.get(resProb).get(maxIt).setTime(timeElapsed);
				}
			}

			/*// print the results
			for(int i=0; i<results.size(); i++){
				System.out.println(results.get(i).getTime());
				results.get(i).getPR().show();
			}*/

			//find the optimal values for the hyperparameters
			 // calculate similarity for each of the points

			// add the similarity measure to the objects
			// the similarity cannot be calculated for the values that are at the boundaries
			// in those cases, use only the available neighbours

			for(int i=0; i<resProbs.size(); i++) {
				for (int j=0; j<maxIts.size(); j++) {

					// define which are the neighbours and take track of them
					// could be optimized
					ArrayList<Dataset> PageRanks = new ArrayList<>();
					// add in position 0 the current 1
					PageRanks.add(results.get(resProbs.get(i)).get(maxIts.get(j)).getPR());
					if( (i-1) >= 0 ){
						// System.out.printf("1");
						PageRanks.add(results.get(resProbs.get(i-1)).get(maxIts.get(j)).getPR());
						// results.get(resProbs.get(i-1)).get(maxIts.get(j)).getPR().show();
					}
					if( (i+1) < (resProbs.size()) ){
						// System.out.printf("2");
						PageRanks.add(results.get(resProbs.get(i+1)).get(maxIts.get(j)).getPR());
						// results.get(resProbs.get(i+1)).get(maxIts.get(j)).getPR().show();
					}
					if( (j-1) >= 0 ){
						// System.out.printf("3");
						PageRanks.add(results.get(resProbs.get(i)).get(maxIts.get(j-1)).getPR());
						//results.get(resProbs.get(i)).get(maxIts.get(j-1)).getPR().show();
					}
					if( (j+1) < maxIts.size() ){
						//System.out.printf("4");
						PageRanks.add(results.get(resProbs.get(i)).get(maxIts.get(j+1)).getPR());
						//results.get(resProbs.get(i)).get(maxIts.get(j+1)).getPR().show();
					}
					// else{ continue; } // in case of errors
					// calculate and set the similarity for the current

					results.get(resProbs.get(i)).get(maxIts.get(j)).setSimilarityValue(similarity(PageRanks, 0.3));
					// System.out.printf("the value of similarity for %d, %d is : %f", i, j, results.get(resProbs.get(i)).get(maxIts.get(j)).getSimilarityValue() );
				}
			}


			// At this point, "results" maps some values of resamplingProbability and MaxIter to the
			// respective PageRanks, time of execution and similarity.
			// from now on we don't care anymore about the PageRank, but only about the parameters and the similarity.
			// We need to define a threshold to define "convergence".

			File myObj = new File("/Users/pietro/Desktop/SDM/LAB 2/KnowledgeGraphLab/SparkGraphXassignment/src/main/resources/bin/optimization.txt");

			// print results and give the optimal value
			// minimum time observed btw the optimal. At the begining is set to be maximal
			long time = Long.MAX_VALUE;
			// we will take track of the positions of the optimals
			int ii = -1;
			int jj = -1;
			// take track of the best similarity value observed
			double maxSim = 0.0;
			int maxSimi = -1;
			int maxSimj = -1;

			System.out.printf("----------------------------------\n  resProb maxIter similarity   time \n");
			for(int i=0; i<resProbs.size(); i++){
				for(int j=0; j<maxIts.size(); j++){
					System.out.printf(String.format(" %f   %d      %f   %d \n", resProbs.get(i), maxIts.get(j),
							results.get(resProbs.get(i)).get(maxIts.get(j)).getSimilarityValue(),
							results.get(resProbs.get(i)).get(maxIts.get(j)).getTime()));

					// if the similarity is larger than the threshold, and the time is lower, this pair is the optimal
					if( (results.get(resProbs.get(i)).get(maxIts.get(j)).getSimilarityValue() >= threshold) &&
							(results.get(resProbs.get(i)).get(maxIts.get(j)).getTime() < time)){
						time = results.get(resProbs.get(i)).get(maxIts.get(j)).getTime();
						ii = i;
						jj = j;
					}
					// in none overperforms the threshold, the best will be the one with the maximum similarity.
					// Need to take track of it
					if(results.get(resProbs.get(i)).get(maxIts.get(j)).getSimilarityValue() > maxSim){
						maxSim = results.get(resProbs.get(i)).get(maxIts.get(j)).getSimilarityValue();
						maxSimi = i;
						maxSimj = j;
					}
				}
			}
			// set the optimal
			parameters optimalPar = new parameters();

			// print the optimal
			if(ii != -1){ // if any pairs have overperformed the maxSim
				System.out.printf(String.format("The optimum pair of parameters is resProb: %f and maxIter: %d\n " +
								"The running time is: %d",
						resProbs.get(ii), maxIts.get(jj), time));
				optimalPar.setMaxIter(maxIts.get(jj));
				optimalPar.setResProb(resProbs.get(ii));
			}
			else{ // if none of the pair overperformed maxSim will give the one with maximum similarity

				System.out.printf(String.format("\n------------------\nWARNING: none of the pairs has similarity " +
								"values higher than %f\n \nThe optimum pair of parameters is resProb: %f and maxIter: %d\n ",
						threshold, resProbs.get(maxSimi), maxIts.get(maxSimj)));
				optimalPar.setMaxIter(maxIts.get(maxSimj));
				optimalPar.setResProb(resProbs.get(maxSimi));
			}

			return optimalPar;
		}




	public static void main(JavaSparkContext ctx, SQLContext sqlCtx, double similarityThreshold, int[] maxIterValues,
							double[] resProbValues) {
		GraphFrame gf = createGraph(ctx, sqlCtx);

		System.out.println("TUNING HYPERPARAMETERS");
		parameters param = hyperparameters(ctx, sqlCtx, similarityThreshold, maxIterValues, resProbValues);

		System.out.println("\n\n ---------------------------------\n" +
				"The final pageRank is the following \n ---------------------------------\n");
		computePageRankTop10(gf, param.getResProb(), param.getMaxIter(), false ).show();
	}
}
