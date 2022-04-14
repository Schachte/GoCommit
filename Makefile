gen:
	protoc api/v1/**/*.proto --go_out=plugins=grpc:.

clean:
	rm -rf */**/*.pb.go
