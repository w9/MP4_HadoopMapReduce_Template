.PHONY: package
package:
	cd mp4 && make

.PHONY: test-a
test-a:
	hadoop fs -mkdir -p ./output \
     && hadoop fs -rm -r ./output \
     && hadoop jar ./mp4/target/mp4-1.0-SNAPSHOT.jar TopTitles -D stopwords=JavaTemplate/stopwords.txt -D delimiters=JavaTemplate/delimiters.txt JavaTemplate/dataset/titles ./output \
     && hadoop fs -cat ./output/part* \
     && hadoop fs -ls ./output

.PHONY: stage-input
stage-input:
	hadoop fs -mkdir -p /tutorial/input \
     && hadoop fs -put ./dataset/* /tutorial/input
