package buffer

import (
	"encoding/gob"
	"errors"
	"io"
	"math"
)

type partition struct {
	List
	Pool
}

// NewPartition returns a Buffer which uses a Pool to extend or shrink its size as needed.
// It automatically allocates new buffers with pool.Get() to extend is length, and
// pool.Put() to release unused buffers as it shrinks.
func NewPartition(pool Pool, buffers ...Buffer) BufferPeek {
	return &partition{
		Pool: pool,
		List: buffers,
	}
}

func (buf *partition) Cap() int64 {
	return math.MaxInt64
}

func (buf *partition) GetDataLen() int64 {
	var dataLen int
	for _, v := range buf.List {
		src := v.(*memory)
		dataLen += len(src.Bytes())
	}
	return int64(dataLen)
}

func (buf *partition) ReadPeekByIndex(partitionIndex int) (data []byte, n int, err error) {
	if len(buf.List) <= partitionIndex {
		return nil, n, io.EOF
	}

	buffer := buf.List[partitionIndex]

	src, ret := buffer.(*memory)
	if !ret {
		panic("type error")
	}

	data = make([]byte, src.Len())
	p := data
	n = copy(p, src.Bytes())
	return data, n, nil
}

func (buf *partition) Read(p []byte) (n int, err error) {
	for len(p) > 0 {

		if len(buf.List) == 0 {
			return n, io.EOF
		}

		buffer := buf.List[0]

		if Empty(buffer) {
			buf.Pool.Put(buf.Pop())
			continue
		}

		m, er := buffer.Read(p)
		n += m
		p = p[m:]

		if er != nil && er != io.EOF {
			return n, er
		}

	}
	return n, nil
}

func (buf *partition) ReadPeek() (data []byte, n int, err error) {
	i := 0
	if len(buf.List) == 0 {
		return nil, n, io.EOF
	}
	data = make([]byte, buf.GetDataLen())
	p := data
	lenList := len(buf.List)
	for i < lenList {
		buffer := buf.List[i]

		src, ret := buffer.(*memory)
		if !ret {
			panic("type error")
		}

		m := copy(p, src.Bytes())
		if m == 0 {
			return data, n, nil
		}
		n += m
		p = p[m:]
		i++
	}
	return data, n, nil
}

func (buf *partition) WriteByIndex(p []byte, off int64) (n int, err error) {
	var i int64
	pMem := buf.Pool.(*memPool)
	if pMem.N < int64(len(p)) {
		return n, errors.New("The data is greater than one slice")
	}

	lenList := len(buf.List)

	for len(p) > 0 {
		for i = 0; i < off-int64(lenList)+1; i++ {
			next, err := buf.Pool.Get()
			if err != nil {
				return n, err
			}
			buf.Push(next)
		}

		//覆盖写
		buffer := buf.List[off]
		buffer.Reset()
		m, er := buffer.Write(p)
		n += m
		p = p[m:]

		if er == io.ErrShortWrite {
			er = nil
		} else if er != nil {
			return n, er
		}
	}
	return n, nil
}

func (buf *partition) Write(p []byte) (n int, err error) {
	for len(p) > 0 {

		if len(buf.List) == 0 {
			next, err := buf.Pool.Get()
			if err != nil {
				return n, err
			}
			buf.Push(next)
		}
		buffer := buf.List[len(buf.List)-1]

		if Full(buffer) {
			next, err := buf.Pool.Get()
			if err != nil {
				return n, err
			}
			buf.Push(next)
			continue
		}

		m, er := buffer.Write(p)
		n += m
		p = p[m:]

		if er == io.ErrShortWrite {
			er = nil
		} else if er != nil {
			return n, er
		}

	}
	return n, nil
}

func (buf *partition) Reset() {
	for len(buf.List) > 0 {
		buf.Pool.Put(buf.Pop())
	}
}

func init() {
	gob.Register(&partition{})
}
