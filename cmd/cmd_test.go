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
	c, err := NewCmd("/var/db/mysql/tank/srv2", "tank/srv2", "root:qwer1234@tcp(localhost:33062)/")
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
	back0, _ := os.Create("backup-full")
	defer back0.Close()
	back1, _ := os.Create("backup-diff")
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
