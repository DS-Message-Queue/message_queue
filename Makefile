copy_protos: genereate_protos
	cp ./src/protos/*.py ./src/broker/
	cp ./src/protos/*.py ./src/manager/

genereate_protos:
	python3 -m grpc_tools.protoc -I./src/protos/ --python_out=./src/protos/ --grpc_python_out=./src/protos/ ./src/protos/managerservice.proto
	python3 -m grpc_tools.protoc -I./src/protos/ --python_out=./src/protos/ --grpc_python_out=./src/protos/ ./src/protos/brokerservice.proto
