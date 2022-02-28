.PHONY: test
test:
	hadoop fs -mkdir -p /tutorial/output \
     && hadoop fs -rm -r /tutorial/output \
     && hadoop jar ./mp4/target/mp4-1.0-SNAPSHOT.jar TopWords /tutorial/input /tutorial/output \
     && hadoop fs -cat /tutorial/output/part* \
     && hadoop fs -ls /tutorial/output/

.PHONY: stage-input
stage-input:
	hadoop fs -mkdir -p /tutorial/input \
     && hadoop fs -put ./dataset/* /tutorial/input
