package frm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
)

var (
	WrongFRMFileErr = errors.New("Wrong FRM file type.")
)

type Frm struct {
	fileType          uint16
	version           uint8
	legacyDbType      uint8
	ioSize            uint16
	length            uint32
	tmpKeyLength      uint16
	recLength         uint16
	maxRows           uint32
	minRows           uint32
	dbCreatePack      uint16
	keyInfoLength     uint16
	tableOptions      uint16
	fileVersion       uint8
	avgRowLength      uint32
	defaultCharset    uint8
	rowType           uint8
	charsetLow        uint8
	statSamplePages   uint16
	statAutoRecalc    uint8
	keyLength         uint32
	mySQLVersionId    uint32
	extraSize         uint32
	extraRecBufLen    uint16
	defaultPartDbType uint8
	keyBlockSize      uint16
	columns           []column
	keys              []key
}

func NewFrm(path string) (*Frm, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) < 64 {
		return nil, WrongFRMFileErr
	}
	frm := &Frm{}
	frm.read(data)
	if frm.fileType != tableFileType {
		return nil, WrongFRMFileErr
	}
	frm.readKeys(data)
	frm.readColumns(data)
	return frm, nil
}

func (f *Frm) read(data []byte) {
	f.fileType = binary.LittleEndian.Uint16(data[0:2])
	f.version = data[2]
	f.legacyDbType = data[3]
	// skip 2
	f.ioSize = binary.LittleEndian.Uint16(data[6:8])
	// skip 2
	f.length = binary.LittleEndian.Uint32(data[10:14])
	f.tmpKeyLength = binary.LittleEndian.Uint16(data[14:16])
	f.recLength = binary.LittleEndian.Uint16(data[16:18])
	f.maxRows = binary.LittleEndian.Uint32(data[18:22])
	f.minRows = binary.LittleEndian.Uint32(data[22:26])
	f.dbCreatePack = binary.LittleEndian.Uint16(data[26:28])
	f.keyInfoLength = binary.LittleEndian.Uint16(data[28:30])
	f.tableOptions = binary.LittleEndian.Uint16(data[30:32])
	// skip 1
	f.fileVersion = data[33]
	f.avgRowLength = binary.LittleEndian.Uint32(data[34:38])
	f.defaultCharset = data[38]
	// skip 1
	f.rowType = data[40]
	f.charsetLow = data[41]
	f.statSamplePages = binary.LittleEndian.Uint16(data[42:44])
	f.statAutoRecalc = data[45]
	// skip 2
	f.keyLength = binary.LittleEndian.Uint32(data[47:51])
	f.mySQLVersionId = binary.LittleEndian.Uint32(data[51:55])
	f.extraSize = binary.LittleEndian.Uint32(data[55:59])
	f.extraRecBufLen = binary.LittleEndian.Uint16(data[59:61])
	f.defaultPartDbType = data[61]
	f.keyBlockSize = binary.LittleEndian.Uint16(data[62:64])
}

func (f *Frm) columnsPos() int {
	ioSize := int(f.ioSize)
	offset := ioSize + int(f.tmpKeyLength) + int(f.recLength)
	offset = ((offset / ioSize) + 1) * ioSize
	return offset + 256
}

func (f *Frm) keysPos() int {
	return int(f.ioSize)
}

func (f *Frm) readColumns(data []byte) {
	data = data[f.columnsPos():]
	numScreens := int(binary.LittleEndian.Uint16(data[0:2]))
	numColumns := int(binary.LittleEndian.Uint16(data[2:4]))
	data = data[32:]
	f.columns = make([]column, numColumns)
	colNum := 0
	for i := 0; i < numScreens; i++ {
		numNames := int(data[3])
		data = data[48:]
		for j := 0; j < numNames; j++ {
			col := &f.columns[colNum]
			colNum++
			nameSize := data[2]
			col.name = string(data[3:(3 + nameSize - 1)])
			data = data[(3 + nameSize):]
		}
	}
	for i := 0; i < numColumns; i++ {
		f.columns[i].read(data)
		data = data[columnStructSize:]
	}
}

func (f *Frm) readKeys(data []byte) {
	data = data[f.keysPos():]
	numKeys := int(data[0])
	numParts := int(data[1])
	if (numKeys & 0x80) > 0 {
		numKeys = (int(numParts) << 7) | (numKeys & 0x7f)
		numParts = int(data[2]) + (int(data[3]) << 8)
	}
	data = data[6:]
	f.keys = make([]key, numKeys)
	for i := 0; i < numKeys; i++ {
		key := &f.keys[i]
		key.read(data)
		data = data[keyStructSize:]
		key.parts = make([]part, key.numParts)
		for j := 0; j < int(key.numParts); j++ {
			part := &key.parts[j]
			part.read(data)
			data = data[partStructSize:]
		}
	}
	term := data[0]
	data = data[1:]
	for i := 0; i < numKeys; i++ {
		j := bytes.IndexByte(data, term)
		f.keys[i].name = string(data[:j])
		data = data[(j + 1):]
	}
}

func (f *Frm) WriteCreateTable(w io.Writer, table string) {
	io.WriteString(w, "CREATE TABLE")
	writeSpace(w)
	writeQuoted(w, table)
	writeOpenParen(w)
	l := len(f.columns)
	for i := 0; i < l; i++ {
		if i > 0 {
			writeComma(w)
		}
		io.WriteString(w, "\n")
		c := &f.columns[i]
		c.write(w)
	}
	l = len(f.keys)
	for i := 0; i < l; i++ {
		writeComma(w)
		io.WriteString(w, "\n")
		k := &f.keys[i]
		k.write(w, f.columns)
	}
	writeCloseParen(w)
}
