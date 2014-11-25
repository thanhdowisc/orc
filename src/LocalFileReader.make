all: LocalFileReader.cc  orc_proto.proto
	protoc --cpp_out=. orc_proto.proto
	touch orc_proto.proto
	g++ -std=c++11 -O3 -o local_reader -I. LocalFileReader.cc Reader.cc DataReader.cc Compression.cc RLE.cc orc_proto.pb.cc -lpthread -lprotobuf

clean:
	rm -f local_reader  
