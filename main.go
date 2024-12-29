package main

import (
	"bufio"
	"container/heap"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
)

const (
	bufSize = 1024 * 1024 * 100
)

func main() {
	flag.Parse()

	if flag.NArg() != 3 {
		fmt.Printf("unexpected number of arguments: expected 3, got %d\n", flag.NArg())
	}

	action := flag.Arg(0)

	huffman := &Huffman{}

	if action == "encode" {
		archive, err := os.OpenFile(flag.Arg(1), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			fmt.Printf("error opening file: %s\n", err)
		}
		defer archive.Close()

		file, err := os.OpenFile(flag.Arg(2), os.O_RDONLY, 0644)
		if err != nil {
			fmt.Printf("error opening file: %s\n", err)
		}
		defer file.Close()

		freq := make(map[byte]uint64)
		buf := make([]byte, bufSize)
		for {
			n, err := file.Read(buf)
			if err != nil {
				if err == io.EOF {
					break
				}
				fmt.Printf("error reading file: %s\n", err)
			}

			for i := 0; i < n; i++ {
				freq[buf[i]]++
			}
		}
		_, err = file.Seek(0, 0)
		if err != nil {
			fmt.Printf("error seeking file: %s\n", err)
		}

		reader := bufio.NewReaderSize(file, bufSize)
		writer := bufio.NewWriterSize(archive, bufSize)

		err = huffman.Encode(reader, writer, freq)
		if err != nil {
			fmt.Printf("error encoding: %s\n", err)
		}

		err = writer.Flush()
		if err != nil {
			fmt.Printf("error flushing: %s\n", err)
		}
	} else if action == "decode" {
		archive, err := os.OpenFile(flag.Arg(1), os.O_RDONLY, 0644)
		if err != nil {
			fmt.Printf("error opening file: %s\n", err)
		}
		defer archive.Close()

		file, err := os.OpenFile(flag.Arg(2), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			fmt.Printf("error opening file: %s\n", err)
		}
		defer file.Close()

		reader := bufio.NewReaderSize(archive, bufSize)
		writer := bufio.NewWriterSize(file, bufSize)

		err = huffman.Decode(reader, writer)
		if err != nil {
			fmt.Printf("error decoding: %s\n", err)
		}

		err = writer.Flush()
		if err != nil {
			fmt.Printf("error flushing: %s\n", err)
		}
	} else {
		fmt.Printf("unexpected argument: \"%s\", expected \"encode\" or \"decode\"\n", action)
	}
}

type Item struct {
	value    byte
	priority uint64
	left     *Item
	right    *Item
	index    int
}

type Huffman struct{}

type EncodedSymbol struct {
	Code   uint64
	Length int
}

func buildCodes(node *Item, codes map[byte]EncodedSymbol, code uint64, length int) {
	if node.left == nil && node.right == nil {
		codes[node.value] = EncodedSymbol{
			Code:   code,
			Length: length,
		}
	}

	if node.left != nil {
		buildCodes(node.left, codes, code<<1, length+1)
	}

	if node.right != nil {
		buildCodes(node.right, codes, code<<1|1, length+1)
	}
}

func uint64ToBytes(i uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, i)
	return b
}

func (*Huffman) Encode(r io.Reader, w *bufio.Writer, freq map[byte]uint64) error {
	if len(freq) == 0 {
		return nil
	}

	pq := make(PriorityQueue, len(freq))
	i := 0
	for value, priority := range freq {
		pq[i] = &Item{
			value:    value,
			priority: priority,
			index:    i,
		}
		i++
	}
	heap.Init(&pq)

	left := heap.Pop(&pq).(*Item)
	right := heap.Pop(&pq).(*Item)
	pq.Push(&Item{
		value:    0,
		priority: left.priority + right.priority,
		left:     left,
		right:    right,
	})

	for pq.Len() > 1 {
		left = heap.Pop(&pq).(*Item)
		right = heap.Pop(&pq).(*Item)

		pq.Push(&Item{
			value:    0,
			priority: left.priority + right.priority,
			left:     left,
			right:    right,
		})
	}

	root := pq.Pop().(*Item)
	encodedSymbols := make(map[byte]EncodedSymbol, len(freq))
	buildCodes(root, encodedSymbols, 0, 0)

	var err error
	err = serializeHuffmanTree(w, root)
	if err != nil {
		return err
	}

	err = w.Flush()
	if err != nil {
		return err
	}

	var p uint64
	var k int
	s := 0
	for {
		b := make([]byte, bufSize)
		n, err := r.Read(b)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		for j := 0; j < n; j++ {
			enc, ok := encodedSymbols[b[j]]
			if !ok {
				return fmt.Errorf("unexpected symbol: %d", b[j])
			}

			if k+enc.Length >= 64 {
				p = p | (enc.Code >> (enc.Length + k - 64))
				_, err = w.Write(uint64ToBytes(p))
				if err != nil {
					return err
				}
				p = enc.Code << (128 - enc.Length - k)
				k = enc.Length + k - 64
			} else {
				p = p | (enc.Code << (64 - k - enc.Length))
				k += enc.Length
			}
		}
		s += n

		if s > bufSize/2 {
			err = w.Flush()
			if err != nil {
				return err
			}
		}
	}

	_, err = w.Write(append(uint64ToBytes(p), byte(k)))
	if err != nil {
		return err
	}

	return nil
}

func serializeHuffmanTree(w io.Writer, node *Item) error {
	nodeType := byte(0)
	if node.left == nil && node.right == nil {
		nodeType = 1
	}
	if _, err := w.Write([]byte{nodeType}); err != nil {
		return err
	}

	if nodeType == 1 {
		if _, err := w.Write([]byte{node.value}); err != nil {
			return err
		}
	} else {
		if err := serializeHuffmanTree(w, node.left); err != nil {
			return err
		}
		if err := serializeHuffmanTree(w, node.right); err != nil {
			return err
		}
	}
	return nil
}

func (*Huffman) Decode(r io.Reader, w *bufio.Writer) error {
	root, err := deserializeHuffmanTree(r)
	if err != nil {
		return err
	}
	const p63 = uint64(1) << 63
	buf := make([]byte, bufSize)
	var b uint64
	var k int
	droot := root
	s := 0
	for {
		n, err := r.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		for i := 0; i < n; i++ {
			b = b | (uint64(buf[i]) << (56 - k))
			k += 8

			if n < len(buf) && i == n-9 {
				b = binary.BigEndian.Uint64(buf[n-9:])
				k = int(buf[n-1])
			}

			for k > 0 {
				if droot.left != nil && b&(p63) == 0 {
					droot = droot.left
					b <<= 1
					k--
				} else if droot.right != nil && b&(p63) == p63 {
					droot = droot.right
					b <<= 1
					k--
				}
				if droot.left == nil && droot.right == nil {
					w.Write([]byte{droot.value})
					droot = root
				}
			}

			if n < len(buf) && i == n-9 {
				break
			}
		}

		s += n

		if s > bufSize/2 {
			err = w.Flush()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func deserializeHuffmanTree(r io.Reader) (*Item, error) {
	var nodeTypeBytes [1]byte
	if _, err := io.ReadFull(r, nodeTypeBytes[:]); err != nil {
		return nil, err
	}

	node := &Item{}
	if nodeTypeBytes[0] == 1 {
		var valueBytes [1]byte
		if _, err := io.ReadFull(r, valueBytes[:]); err != nil {
			return nil, err
		}
		node.value = valueBytes[0]
	} else {
		left, err := deserializeHuffmanTree(r)
		if err != nil {
			return nil, err
		}
		right, err := deserializeHuffmanTree(r)
		if err != nil {
			return nil, err
		}
		node.left = left
		node.right = right
	}
	return node, nil
}

type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority < pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x any) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}
