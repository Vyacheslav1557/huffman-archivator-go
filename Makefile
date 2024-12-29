build:
	@go build -o huffman.exe .
encode:
	@huffman encode archive.txt input.txt
decode:
	@huffman decode archive.txt input.txt