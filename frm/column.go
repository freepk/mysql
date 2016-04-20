package frm

import (
	"encoding/binary"
	"io"
	"strconv"
)

type column struct {
	fieldLength  uint16
	unireg       uint8
	flags        uint16
	uniregType   uint8
	charsetLow   uint8
	intervalNr   uint8
	fieldType    uint8
	charset      uint8
	comentLength uint16
	name         string
}

func (c *column) read(d []byte) {
	// skip 3
	c.fieldLength = binary.LittleEndian.Uint16(d[3:5])
	// skip 2
	c.unireg = d[7]
	c.flags = binary.LittleEndian.Uint16(d[8:10])
	c.uniregType = d[10]
	c.charsetLow = d[11]
	c.intervalNr = d[12]
	c.fieldType = d[13]
	c.charset = d[14]
	c.comentLength = binary.LittleEndian.Uint16(d[15:17])
}

func (c *column) charsetNum() int {
	return (int(c.charsetLow) << 8) + int(c.charset)
}

func (c *column) writeSize(w io.Writer) {
	writeOpenParen(w)
	io.WriteString(w, strconv.Itoa(int(c.fieldLength)))
	writeCloseParen(w)
}

func (c *column) writeSign(w io.Writer) {
	if (int(c.flags) & signedFieldFlag) == 0 {
		io.WriteString(w, " UNSIGNED")
	}
}

func (c *column) writeCharset(w io.Writer) {
	cs := charsets[c.charsetNum()]
	io.WriteString(w, " CHARSET ")
	io.WriteString(w, cs.name)
	io.WriteString(w, " COLLATE ")
	io.WriteString(w, cs.collate)
}

func (c *column) writeSized(w io.Writer, s string) {
	io.WriteString(w, s)
	c.writeSize(w)
}

func (c *column) writeSigned(w io.Writer, s string) {
	io.WriteString(w, s)
	c.writeSign(w)
}

func (c *column) writeSizedSigned(w io.Writer, s string) {
	io.WriteString(w, s)
	c.writeSize(w)
	c.writeSign(w)
}

func (c *column) writeBlob(w io.Writer, s string) {
	io.WriteString(w, s)
	if c.charsetNum() == binaryCharset {
		io.WriteString(w, "BLOB")
	} else {
		io.WriteString(w, "TEXT")
		c.writeCharset(w)
	}
}

func (c *column) writeBinary(w io.Writer, s string) {
	cn := c.charsetNum()
	io.WriteString(w, s)
	if cn == binaryCharset {
		io.WriteString(w, "BINARY")
		c.writeSize(w)
	} else {
		cs := charsets[cn]
		io.WriteString(w, "CHAR")
		writeOpenParen(w)
		io.WriteString(w, strconv.Itoa(int(c.fieldLength)/cs.maxLen))
		writeCloseParen(w)
		c.writeCharset(w)
	}
}

func (c *column) write(w io.Writer) {
	writeQuoted(w, c.name)
	writeSpace(w)
	switch c.fieldType {
	case newDateFieldType:
		io.WriteString(w, "DATE")
	case dateTime2FieldType:
		io.WriteString(w, "DATETIME")
	case time2FieldType:
		io.WriteString(w, "TIME")
	case timeStamp2FieldType:
		io.WriteString(w, "TIMESTAMP")
	case geomertyFieldType:
		switch c.charsetNum() {
		case geometryGeomType:
			io.WriteString(w, "GEOMETRY")
		case pointGeomType:
			io.WriteString(w, "POINT")
		case lineStringGeomType:
			io.WriteString(w, "LINESTRING")
		case polygonGeomType:
			io.WriteString(w, "POLYGON")
		case multiPointGeomType:
			io.WriteString(w, "MULTIPOINT")
		case multiLineStrintGeomType:
			io.WriteString(w, "MULTILINESTRING")
		case multiPolygonGeomType:
			io.WriteString(w, "MULTIPOLYGON")
		case geometryCollectionGeomType:
			io.WriteString(w, "GEOMETRYCOLLECTION")
		}
	case doubleFieldType:
		c.writeSigned(w, "DOUBLE")
	case floatFieldType:
		c.writeSigned(w, "FLOAT")
	case bitFieldType:
		c.writeSized(w, "BIT")
	case tinyFieldType:
		c.writeSizedSigned(w, "TINYINT")
	case shortFieldType:
		c.writeSizedSigned(w, "SMALLINT")
	case longFieldType:
		c.writeSizedSigned(w, "INT")
	case int24FieldType:
		c.writeSizedSigned(w, "MEDIUMINT")
	case longLongFieldType:
		c.writeSizedSigned(w, "BIGINT")
	case tinyBlobFieldType:
		c.writeBlob(w, "TINY")
	case mediumBlobFieldType:
		c.writeBlob(w, "MEDIUM")
	case longBlobFieldType:
		c.writeBlob(w, "LONG")
	case blobFieldType:
		c.writeBlob(w, emptyString)
	case varCharFieldType:
		c.writeBinary(w, "VAR")
	case stringFieldType:
		c.writeBinary(w, emptyString)
	case newDecimalFieldType:
		io.WriteString(w, "DECIMAL")
		i := int(c.fieldLength) - (int(c.flags) & signedFieldFlag)
		f := (int(c.flags) >> decimalShift) & decimalMask
		if f > 0 {
			i--
		}
		writeOpenParen(w)
		io.WriteString(w, strconv.Itoa(i))
		writeComma(w)
		io.WriteString(w, strconv.Itoa(f))
		writeCloseParen(w)
		c.writeSign(w)
	default:
		io.WriteString(w, "<UNKNOWN_TYPE>")
	}
	if (c.flags & nullableFieldFlag) == 0 {
		io.WriteString(w, " NOT NULL")
	}
	if c.uniregType == 15 {
		io.WriteString(w, " AUTO_INCREMENT")
	}
}
