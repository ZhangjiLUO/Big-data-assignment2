Assignment 2
Author: Ying Yi, Zhangji Luo

PART1

1. Upload jar file assignment2_2.11-0.1.jar to Amazon S3 Storage. Run class MoviePlot.

2. MoviePlot class should have two args: (1) the input directory that stores "plot_summaries.txt" 
   and "movie.metadata.tsv"; (2) an search term entered by the user. e.g. If the files are 
	 stored on S3, the first arg should look like "s3://YourBucketName/Directory".

3. Stop-words in the original file were removed using the built-in spark StopWordsRemover class.
   In addition, the words of which the lengths are less than 1 are removed as well.

4. After the user entered the search term, a list of movie names are returned sorted by their 
	 tf-idf value in descending order. The movie name on top indicates the most relevant movie for 
	 the search term.
	 
5. The stdout file can be found in directory "/aws-logs-<ID_NUMBER>-<Region>/elasticmapreduce/
   <CLUSTER_ID>/containers/application_<APPLICATION_ID>/container_<APPLICATION_AND_JOB_ID>/stdout.gz"
	 
PART2

1. Upload jar file assignment2_2.11-0.1.jar to Amazon S3 Storage. Run class PageRank.

2. PageRank should have three args: 
(1) the input directory that stores file 
(2) maximum number of iteration; 
(3) the output file directory. 

3. Inside the output directory (arg[2]), Open part-00000 and part-00001 files. And you can see tuples of (Airport Code, pagerank) in descending order (sorted by the pagerank value from high to low).
	 
4. The data of "July 2018" was used to generate "553174251_T_ONTIME_REPORTING.csv". The fields include
   Reporting Airline, Origin (Origin Airport Code), OriginCityName, Dest (Destination Airport Code), and DestCityName.
