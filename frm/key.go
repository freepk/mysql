package frm

import (
	"encoding/binary"
	"io"
)

type key struct {
	flags     uint16
	length    uint16
	numParts  uint8
	algorithm uint8
	blockSize uint16
	name      string
	parts     []part
}

func (k *key) read(d []byte) {
	k.flags = binary.LittleEndian.Uint16(d[0:2])
	k.length = binary.LittleEndian.Uint16(d[2:4])
	k.numParts = d[4]
	k.algorithm = d[5]
	k.blockSize = binary.LittleEndian.Uint16(d[6:8])
}

func (k *key) write(w io.Writer, columns []column) {
	if k.name == "PRIMARY" {
		io.WriteString(w, "PRIMARY KEY")
	} else {
		if (k.flags & allowDupsKeyFlag) == 0 {
			io.WriteString(w, "UNIQUE KEY")
		} else {
			io.WriteString(w, "KEY")
		}
		writeSpace(w)
		writeQuoted(w, k.name)
	}
	writeOpenParen(w)
	l := len(k.parts)
	for i := 0; i < l; i++ {
		p := &k.parts[i]
		c := &columns[p.fieldNumA()]
		if i > 0 {
			writeComma(w)
		}
		writeQuoted(w, c.name)
		z := int(p.length)
		switch c.fieldType {
		case varCharFieldType,
			stringFieldType,
			tinyBlobFieldType,
			mediumBlobFieldType,
			longBlobFieldType,
			blobFieldType:
			cn := c.charsetNum()
			if cn != binaryCharset {
				z = z / c.charsetA().maxLen
			}
			writeParened(w, z)
		case geometryFieldType:
			writeParened(w, z)
		}
	}

	writeCloseParen(w)
	switch k.algorithm {
	case bTreeKeyAlgo:
		io.WriteString(w, " USING BTREE")
	case rTreeKeyAlgo:
		io.WriteString(w, " USING RTREE")
	case hashKeyAlgo:
		io.WriteString(w, " USING HASH")
	case fullTextKeyAlgo:
		io.WriteString(w, " USING FULLTEXT")
	}
}
