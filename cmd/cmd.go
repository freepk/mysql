package cmd

import (
	"bytes"
	"database/sql"
	"github.com/freepk/mysql/frm"
	"github.com/freepk/zfs"
	_ "github.com/go-sql-driver/mysql"
	"io"
	"io/ioutil"
	"os"
	"strings"
)

func selectDb(db *sql.DB, name string) error {
	_, err := db.Exec("USE " + name)
	if err != nil {
		return err
	}
	return nil
}

func showTables(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tables := make([]string, 0)
	for rows.Next() {
		table := ""
		err = rows.Scan(&table)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, nil
}

type Cmd struct {
	fileSys string
	dataDir string
	dataSrc string
}

func NewCmd(fileSys, dataDir, dataSrc string) (*Cmd, error) {
	cmd := &Cmd{dataDir: dataDir, fileSys: fileSys, dataSrc: dataSrc}
	return cmd, nil
}

func (c *Cmd) Create(name string) error {
	db, err := sql.Open("mysql", c.dataSrc)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec("CREATE DATABASE " + name)
	if err != nil {
		return err
	}
	err = zfs.Create(c.fileSys + "/" + name)
	if err != nil {
		return err
	}
	return nil
}

func (c *Cmd) GrantPrivileges(dbname string, username string) error {
	db, err := sql.Open("mysql", c.dataSrc)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec("GRANT ALL ON `" + dbname + "`.* TO `"+ username +"`@`%`")
	if err != nil {
		return err
	}
	return nil
}

func (c *Cmd) CreateUser(username string, passwd string) error {
	db, err := sql.Open("mysql", c.dataSrc)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec("CREATE USER IF NOT EXISTS '"+username+"'@'%' IDENTIFIED BY '"+passwd+"'")
	if err != nil {
		return err
	}
	return nil
}

func (c *Cmd) Drop(name string) error {
	db, err := sql.Open("mysql", c.dataSrc)
	if err != nil {
		return err
	}
	defer db.Close()
	err = selectDb(db, name)
	if err != nil {
		return err
	}
	tables, err := showTables(db)
	if err != nil {
		return err
	}
	if len(tables) > 0 {
		_, err = db.Exec("DROP TABLE " + strings.Join(tables, ", "))
		if err != nil {
			return err
		}
	}
	fileSys := c.fileSys + "/" + name
	err = zfs.Destroy(fileSys, true, false)
	if err != nil {
		return err
	}
	_, err = db.Exec("DROP DATABASE " + name)
	if err != nil {
		return err
	}
	return nil
}

func (c *Cmd) Snapshot(name, snap string) error {
	db, err := sql.Open("mysql", c.dataSrc)
	if err != nil {
		return err
	}
	defer db.Close()
	err = selectDb(db, name)
	if err != nil {
		return err
	}
	_, err = db.Exec("FLUSH TABLES WITH READ LOCK")
	if err != nil {
		return err
	}
	tables, err := showTables(db)
	if err != nil {
		return err
	}
	if len(tables) > 0 {
		_, err = db.Exec("FLUSH TABLES " + strings.Join(tables, ", ") + " FOR EXPORT")
		if err != nil {
			return err
		}
	}
	err = zfs.Snapshot(c.fileSys + "/" + name + "@" + snap)
	if err != nil {
		return err
	}
	_, err = db.Exec("UNLOCK TABLES")
	if err != nil {
		return err
	}
	return nil
}

func (c *Cmd) Backup(name, snap string, w io.Writer) error {
	snap = c.fileSys + "/" + name + "@" + snap
	return zfs.Send(snap, w)
}

func (c *Cmd) BackupDiff(name, snap0, snap1 string, w io.Writer) error {
	snap0 = c.fileSys + "/" + name + "@" + snap0
	snap1 = c.fileSys + "/" + name + "@" + snap1
	return zfs.SendDiff(snap0, snap1, false, w)
}

func (c *Cmd) Restore(name string, r io.Reader) error {
	db, err := sql.Open("mysql", c.dataSrc)
	if err != nil {
		return err
	}
	defer db.Close()
	err = selectDb(db, name)
	if err != nil {
		return err
	}
	oldTables, err := showTables(db)
	if err != nil {
		return err
	}
	if len(oldTables) > 0 {
		_, err = db.Exec("DROP TABLE " + strings.Join(oldTables, ", "))
		if err != nil {
			return err
		}
	}
	fileSys := c.fileSys + "/" + name
	snap, err := zfs.Recv(fileSys, true, r)
	if err != nil {
		return err
	}
	dataDir := c.dataDir + "/" + name
	files, err := ioutil.ReadDir(dataDir)
	if err != nil {
		return err
	}
	ddl := new(bytes.Buffer)
	newTables := make(map[string]string)
	for _, file := range files {
		path := dataDir + "/" + file.Name()
		if frm, err := frm.NewFrm(path); err == nil {
			ddl.Reset()
			table := strings.Split(file.Name(), ".")[0]
			frm.WriteCreateTable(ddl, table)
			newTables[table] = ddl.String()
		}
		os.Remove(path)
	}
	for table, ddl := range newTables {
		_, err = db.Exec(ddl)
		if err != nil {
			return err
		}
		_, err = db.Exec("ALTER TABLE " + table + " DISCARD TABLESPACE")
		if err != nil {
			return err
		}
	}
	if err = zfs.Rollback(snap, true); err != nil {
		return err
	}
	for table, _ := range newTables {
		_, err = db.Exec("ALTER TABLE " + table + " IMPORT TABLESPACE")
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Cmd) ListSnap(name string) ([]string, error) {
	return zfs.ListSnap(c.fileSys + "/" + name)
}
