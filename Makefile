gen:
	protoc api/v1/**/*.proto --go_out=plugins=grpc:.

gen_test_certs:
	mkdir test_certs 

	cfssl gencert \
		-initca internal/certs/test/configs/ca-csr.json | cfssljson -bare test_certs/ca

	cfssl gencert \
		-ca=test_certs/ca.pem \
		-ca-key=test_certs/ca-key.pem \
		-config=internal/certs/test/configs/ca-config.json \
		-profile=server \
		internal/certs/test/configs/server-csr.json | cfssljson -bare test_certs/server

	cfssl gencert \
		-ca=test_certs/ca.pem \
		-ca-key=test_certs/ca-key.pem \
		-config=internal/certs/test/configs/ca-config.json \
		-profile=server \
		internal/certs/test/configs/server-csr.json | cfssljson -bare test_certs/client

clean_test_certs:
	rm -rf test_certs

clean:
	rm -rf */**/*.pb.go
