# PageRankSimilarityMeasure
A measure of similarity to compare instances of pageRanks built on the same graph (data) generated using different parameters and eventually rigorously define the convergence of pageRank algorithm (tuning of the parameters).


### Tools Required

To run this project IntelliJ is suggested as java IDE.
For visualization of the results, R is needed.


### Abstract

PageRank is a common algorithm proposed by Google to evaluate the impact of each page in the net based on the number of links referring to that page and the "importance" of the quoting page. 
In the Spark's contest, pageRank is often calculated on the top of Graphs to generate a ranking of the nodes. In many applications the interested is in the top part of the ranking (in what follows, we consider the top 10).

In general, the issue is the choice of the parameters to be set to efficiently obtain results. In the specific Spark's GraphX PageRank implementation, we will discuss about the _resampling probability_ and the _maximum number of iterations_.

The problem is that it looks like there is a **lack of metrics** to define when the algorithm is converging. In other words, there is no a well-known way to define if two instances of pageRanks calculated on the same data using different parameters are similar enough to say that we can stop to look for better parameters to be used since the results are convergent.

What I tried to do here is an implementation of a naive measure of similarity between page ranks.


### Similarity between pageRanks

To calculate the similarity of two pageRanks there are three concepts to consider:
a)	Differences in the order of the pages present in the ranking (e.g., PR1= [ [‘a’,23], [’b’,18], [’c’,16] ] and PR2 = [ [‘b’,23],[’a’,18],[’c’,16] ]. The different order should be considered when comparing the two of them).
b)	Differences in which pages are in the top 10 (e.g., PR1= [ [‘a’,23], [’b’,18], [’c’,16] ] and PR2 = [ [‘a’,23], [’f’,18], [’c’,16] ]. The fact that there are different elements should be considered).
c)	Difference in the scores (e.g., PR1= [ [‘a’,23000], [’b’,18], [’c’,16] ] and PR2 = [ [‘a’,23],[’b’, 18],[’c’, 16]]. These two rankings are very different in terms of the scores that have been assigned even if the order is the same).

What we did was to create a measure that is able, given two pageRanks PR1 and PR2, to summarize in one scalar all these aspects:

**_similarity= α orderSimilarity + (1-α)(1-numericalChange)_**

**_orderSimilarity_** is defined as the Rank Biased Overlap measure . We choose to use this measure of similarity because it does not require the two rankings to be conjoint (i.e., with the same elements). We set the p parameters of RBO to 0.9. This part answers concept a).

**_numericalChange_** is defined as the sum of the squares of the scores of each page p that is the PR1 minus the score of p in PR2 (if present) or minus 0 (if not present), then normalized to assume values in [0,1].
For example, having PR1= [ [‘a’,27], [’b’,16], [’c’,10]] and PR2 = [ [‘a’,23], [’f’,18], [’c’,16]],

numericChang=( (score_(a_PR1 )-score_(a_PR2 ))^2 + (score_(b_PR1 )-0)^2 + (score_(c_PR1 )-score_(c_PR2 ))^2 )  / _den_

where  _den_ = (∑_(x page in PR1)(score_x^2))

-> numericChang = ((27-23)^2 + (16)^2 - (10-16)^2) / 1085 = 0.28

**α** is a parameter in [0,1] to give different weights to the two parts. We set it to 0.3 to equally balance the 3 sources of similarity a), b) and c).


It should be noticed that this measure is not always commutative. More precisely, it is commutative only if all the pages contained in the first pageRank are contained also in the second one (this is always the case when working with the all pageRank and not only with the top10).


### Convergence of the algorithm

Having this measure of similarity, once the pageRanks have been calculated for different values of the parameters (_resampling probability_ and the _maximum number of iterations_ in our case), each pageRank is compared to its 4 closest neigbhors (based on the values of the parameters) and the average of the similarity between the current instance of the pageRanks is saved.
In oder words, having this similarity measure, we are able to find, for each pair of _resampling probability_ and _maximum number of iterations_, how similar to the neighbors was the pageRank they identified. If an instance of pageRank is very similar to the close ones, it means that the algorithm is converging. 

Thanks to this idea, it's possible to systematically define the convergence of the algorithm simply defining a threshold of similarity after which the algorithm is assumed to be converging. After a few attempts it has been set to 0.925.
The idea is that, for all the couples of parameters that overtake the threshold, the values to be used are the ones for which the time of execution is lower.

### Implementation

The repository should be open as an Intellij Project. The actual implementaion of the t src/main/java/convergence_PageRank. For more details go to the comments.


### Visualization of the results
Visualization_of_similarity.R contains the code to visualize the output of the optimization. An example can be seen at http://rpubs.com/ferrazzipietro/895710.

## Resources

RBO measure https://towardsdatascience.com/rbo-v-s-kendall-tau-to-compare-ranked-lists-of-items-8776c5182899

PageRank source code https://github.com/apache/spark/blob/v3.2.1/graphx/src/main/scala/org/apache/spark/graphx/lib/PageRank.scala




