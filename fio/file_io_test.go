package fio

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewFileIO(t *testing.T) {
	io, err := NewFileIOManager("./test_file")
	assert.Nil(t, err)
	assert.NotNil(t, io)
}

func TestFileIO_Write(t *testing.T) {
	io, err := NewFileIOManager("./test_file")
	assert.Nil(t, err)
	assert.NotNil(t, io)
	n, err := io.Write([]byte(""))
	assert.Equal(t, 0, n)
	n, err = io.Write([]byte("abc"))
	t.Log(n, err)
}

func TestFileIO_Read(t *testing.T) {

	io, err := NewFileIOManager("./test_file")
	assert.Nil(t, err)
	assert.NotNil(t, io)
	n, err := io.Write([]byte(""))
	assert.Equal(t, 0, n)
	n, err = io.Write([]byte("abc"))
	t.Log(n, err)
}
