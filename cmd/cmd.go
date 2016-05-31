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

type Cmd struct {
	dataDir string
	fileSys string
	dataSrc string
	db      *sql.DB
}

func NewCmd(dataDir, fileSys, dataSrc string) (*Cmd, error) {
	db, err := sql.Open("mysql", dataSrc)
	if err != nil {
		return nil, err
	}
	cmd := &Cmd{dataDir: dataDir, fileSys: fileSys, dataSrc: dataSrc, db: db}
	return cmd, nil
}

func (c *Cmd) use(name string) error {
	_, err := c.db.Exec("USE " + name)
	if err != nil {
		return err
	}
	return nil
}

func (c *Cmd) showTables() ([]string, error) {
	rows, err := c.db.Query("SHOW TABLES")
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

func (c *Cmd) Create(name string) error {
	_, err := c.db.Exec("CREATE DATABASE " + name)
	if err != nil {
		return err
	}
	err = zfs.Create(c.fileSys + "/" + name)
	if err != nil {
		return err
	}
	return nil
}

func (c *Cmd) Drop(name string) error {
	err := c.use(name)
	if err != nil {
		return err
	}
	tables, err := c.showTables()
	if err != nil {
		return err
	}
	if len(tables) > 0 {
		_, err = c.db.Exec("DROP TABLE " + strings.Join(tables, ", "))
		if err != nil {
			return err
		}
	}
	fileSys := c.fileSys + "/" + name
	err = zfs.Destroy(fileSys, true, false)
	if err != nil {
		return err
	}
	_, err = c.db.Exec("DROP DATABASE " + name)
	if err != nil {
		return err
	}
	return nil
}

func (c *Cmd) Snapshot(name, snap string) error {
	err := c.use(name)
	if err != nil {
		return err
	}
	_, err = c.db.Exec("FLUSH TABLES WITH READ LOCK")
	if err != nil {
		return err
	}
	tables, err := c.showTables()
	if err != nil {
		return err
	}
	if len(tables) > 0 {
		_, err = c.db.Exec("FLUSH TABLES " + strings.Join(tables, ", ") + " FOR EXPORT")
		if err != nil {
			return err
		}
	}
	err = zfs.Snapshot(c.fileSys + "/" + name + "@" + snap)
	if err != nil {
		return err
	}
	_, err = c.db.Exec("UNLOCK TABLES")
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
	err := c.use(name)
	if err != nil {
		return err
	}
	tables, err := c.showTables()
	if err != nil {
		return err
	}
	if len(tables) > 0 {
		_, err = c.db.Exec("DROP TABLE " + strings.Join(tables, ", "))
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
	fileInfos, err := ioutil.ReadDir(dataDir)
	if err != nil {
		return err
	}
	ddl := new(bytes.Buffer)
	tableMap := make(map[string]string)
	for _, fileInfo := range fileInfos {
		filePath := dataDir + "/" + fileInfo.Name()
		if frm, err := frm.NewFrm(filePath); err == nil {
			ddl.Reset()
			table := strings.Split(fileInfo.Name(), ".")[0]
			frm.WriteCreateTable(ddl, table)
			tableMap[table] = ddl.String()
		}
		os.Remove(filePath)
	}
	for table, ddl := range tableMap {
		_, err = c.db.Exec(ddl)
		if err != nil {
			return err
		}
		_, err = c.db.Exec("ALTER TABLE " + table + " DISCARD TABLESPACE")
		if err != nil {
			return err
		}
	}
	if err = zfs.Rollback(snap, true); err != nil {
		return err
	}
	for table, _ := range tableMap {
		_, err = c.db.Exec("ALTER TABLE " + table + " IMPORT TABLESPACE")
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Cmd) ListSnap(name string) ([]string, error) {
	return zfs.ListSnap(c.fileSys + "/" + name)
}
