
clean:
	rm -rf /tmp/peejay/*
	killall test.py
	ps -ef | grep -i pee | grep python  | cut -c 10-15 | xargs kill

