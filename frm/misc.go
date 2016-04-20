package frm

import (
	"io"
)

func writeQuoted(w io.Writer, s string) {
	io.WriteString(w, "`")
	io.WriteString(w, s)
	io.WriteString(w, "`")
}

func writeOpenParen(w io.Writer) {
	io.WriteString(w, "(")
}

func writeCloseParen(w io.Writer) {
	io.WriteString(w, ")")
}

func writeSpace(w io.Writer) {
	io.WriteString(w, " ")
}

func writeComma(w io.Writer) {
	io.WriteString(w, ",")
}
