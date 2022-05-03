import com.google.common.io.Files;
import convergence_PageRank.convergence_PageRank;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import org.graphframes.GraphFrame;
import utils.Utils;

import static convergence_PageRank.convergence_PageRank.*;

public class Main {

    public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("SparkGraphs_II").setMaster("local[*]");
		JavaSparkContext ctx = new JavaSparkContext(conf);
		ctx.setCheckpointDir(Files.createTempDir().getAbsolutePath());

		SQLContext sqlctx = new SQLContext(ctx);

        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR);

        GraphFrame gf = createGraph(ctx, sqlctx);
        double similarityThreshold = 0.925;
        int[] maxIterValues = {7,12};
        double[] resProbValues ={0.1,0.5,0.1};

        System.out.println("TUNING HYPERPARAMETERS");

        // maxIterValues contain the ranges of the search space (2 elements[min, max])
        // resProbValues contain the ranges and the granularity of the search space (3 elements[min, max, granularity])
        // threshold represents the similarity at which we consider the algorithm to be converging.

        // tune the hyper parameters
        convergence_PageRank.parameters param = hyperparameters(ctx, sqlctx, similarityThreshold, maxIterValues, resProbValues);

        System.out.println("\n\n ---------------------------------\n" +
                "The final pageRank is the following \n ---------------------------------\n");
        computePageRankTop10(gf, param.getResProb(), param.getMaxIter(), false ).show();
        }

}
