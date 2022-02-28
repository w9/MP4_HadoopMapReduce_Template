.PHONY: test
test-a:
	hadoop fs -mkdir -p ./A-output \
     && hadoop fs -rm -r ./A-output \
     && hadoop jar ./mp4/target/mp4-1.0-SNAPSHOT.jar TopTitles -D stopwords=JavaTemplate/stopwords.txt -D delimiters=JavaTemplate/delimiters.txt JavaTemplate/dataset/titles ./A-output \
     && hadoop fs -cat ./A-output/part* \
     && hadoop fs -ls ./A-output

.PHONY: stage-input
stage-input:
	hadoop fs -mkdir -p /tutorial/input \
     && hadoop fs -put ./dataset/* /tutorial/input
