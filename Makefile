df-master:
	go build -o bin/master ./cmd/master
	
df-executor:
	go build -o bin/executor ./cmd/executor
