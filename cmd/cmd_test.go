package cmd

import (
	"log"
	"os"
	"testing"
)

var (
	cmd *Cmd
)

const ()

func init() {
	c, err := NewCmd("tank/srv2", "/var/db/mysql/tank/srv2", "root:qwer1234@tcp(localhost:33062)/")
	if err != nil {
		log.Fatal(err)
	}
	cmd = c
}

func TestCreateDrop(t *testing.T) {
	name := "boomoo"
	err := cmd.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	err = cmd.Drop(name)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSnapshot(t *testing.T) {
	name := "boomoo"
	snap0 := "first"
	snap1 := "second"
	err := cmd.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	err = cmd.Snapshot(name, snap0)
	if err != nil {
		t.Fatal(err)
	}
	err = cmd.Snapshot(name, snap1)
	if err != nil {
		t.Fatal(err)
	}
	err = cmd.Drop(name)
	if err != nil {
		t.Fatal(err)
	}
}

func TestBackup(t *testing.T) {
	name := "boomoo"
	snap0 := "first"
	snap1 := "second"
	back0, _ := os.Create("test-dump-full")
	defer back0.Close()
	back1, _ := os.Create("test-dump-diff")
	defer back1.Close()

	err := cmd.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	err = cmd.Snapshot(name, snap0)
	if err != nil {
		t.Fatal(err)
	}
	err = cmd.Snapshot(name, snap1)
	if err != nil {
		t.Fatal(err)
	}
	err = cmd.Backup(name, snap0, back0)
	if err != nil {
		t.Fatal(err)
	}
	err = cmd.BackupDiff(name, snap0, snap1, back1)
	if err != nil {
		t.Fatal(err)
	}
	err = cmd.Drop(name)
	if err != nil {
		t.Fatal(err)
	}
}

func TestRestore(t *testing.T) {
	name := "boomoo"
	err := cmd.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	full, err := os.Open("./full-dump")
	if err != nil {
		t.Fatal(err)
	}
	defer full.Close()
	err = cmd.Restore(name, full)
	if err != nil {
		t.Fatal(err)
	}
	diff, err := os.Open("./diff-dump")
	if err != nil {
		t.Fatal(err)
	}
	defer diff.Close()
	err = cmd.Restore(name, diff)
	if err != nil {
		t.Fatal(err)
	}
	err = cmd.Drop(name)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSnapList(t *testing.T) {
	name := "boomoo"
	snap0 := "first"
	snap1 := "second"
	err := cmd.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	err = cmd.Snapshot(name, snap0)
	if err != nil {
		t.Fatal(err)
	}
	err = cmd.Snapshot(name, snap1)
	if err != nil {
		t.Fatal(err)
	}
	if list, err := cmd.ListSnap(name); err != nil {
		t.Fatal(err)
	} else {
		t.Log(list)
	}
	err = cmd.Drop(name)
	if err != nil {
		t.Fatal(err)
	}
}
