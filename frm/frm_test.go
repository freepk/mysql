package frm

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"
)

const (
	dataDir = "./data/"
)

func TestNewFrm(t *testing.T) {
	fi, err := ioutil.ReadDir(dataDir)
	if err != nil {
		t.Fail()
	}
	l := len(fi)
	b := &bytes.Buffer{}
	for i := 0; i < l; i++ {
		p := fmt.Sprint(dataDir, fi[i].Name())
		t.Log(p)
		if frm, err := NewFrm(p); err != nil {
			t.Fail()
		} else {
			b.Reset()
			frm.WriteCreateTable(b, "unknown")
			t.Log(b.String())
		}
	}
}
