package frm

import (
	"encoding/binary"
)

type part struct {
	fieldNum    uint16
	offset      uint16
	keyType     uint16
	keyPartFlag uint8
	length      uint16
}

func (p *part) read(d []byte) {
	p.fieldNum = binary.LittleEndian.Uint16(d[0:2])
	p.offset = binary.LittleEndian.Uint16(d[2:4])
	p.keyType = binary.LittleEndian.Uint16(d[4:6])
	p.keyPartFlag = d[6]
	p.length = binary.LittleEndian.Uint16(d[7:9])
}

func (p *part) fieldNumA() int {
	return (int(p.fieldNum) & 0x3FFF) - 1
}
