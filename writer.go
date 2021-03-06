package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mattn/go-sqlite3"
)

// DNSWriter allows parser to write extracted data for a specific item to a storage
type DNSWriter interface {
	Write(id int, name string, price int, bonus int) (bool, bool, error)
	Close()
}

type consoleWriter struct {
}

func (*consoleWriter) Write(id int, price int, bonus int) (bool, error) {
	fmt.Printf("Id = %10d, price = %7d, bonus = %5d\r\n", id, price, bonus)
	return true, nil
}

func (*consoleWriter) Close() {
}

// SqliteWriter implements DNSWriter for Sqlite3 DB
type SqliteWriter struct {
	db       *sql.DB
	conn     *sql.Conn
	tx       *sql.Tx
	filename string
	time     time.Time
	city     string
	cityID   int
	fail     bool
}

func (writer *SqliteWriter) connStr() string {
	// return fmt.Sprintf("file:%s?cache=shared&mode=memory", writer.filename)
	return fmt.Sprintf("file:%s?cache=shared&_fk=1", writer.filename)
}

func (writer *SqliteWriter) initDB() error {
	_, err := writer.db.Exec(`
		CREATE TABLE IF NOT EXISTS dns_cities (
			id integer PRIMARY KEY, 
			name text UNIQUE);
		CREATE TABLE IF NOT EXISTS dns_items (
			id integer PRIMARY KEY, 
			name text);			
		CREATE TABLE IF NOT EXISTS dns_prices (
			itemId integer CONSTRAINT FK_DNSITEMID REFERENCES dns_items(id) ON UPDATE CASCADE ON DELETE CASCADE, 
			cityId integer CONSTRAINT FK_DNSCITYID REFERENCES dns_cities(id) ON UPDATE CASCADE ON DELETE CASCADE, 			
			price integer NOT NULL, 
			date datetime NOT NULL,
			CONSTRAINT UQ_DNS UNIQUE (itemId, cityId, date));
		CREATE TABLE IF NOT EXISTS dns_bonuses (
			itemId integer CONSTRAINT FK_DNSITEMID REFERENCES dns_items(id) ON UPDATE CASCADE ON DELETE CASCADE, 
			cityId integer CONSTRAINT FK_DNSCITYID REFERENCES dns_cities(id) ON UPDATE CASCADE ON DELETE CASCADE,
			bonus integer NOT NULL,
			date datetime NOT NULL,
			CONSTRAINT UQ_DNS UNIQUE (itemId, cityId, date));`)
	return err
}

func (writer *SqliteWriter) initCityID() error {
	var err error
	var needInsert bool
	if writer.cityID == 0 {
		row := writer.db.QueryRow("SELECT id from dns_cities WHERE name = ?", writer.city)
		err = row.Scan(&writer.cityID)
		if err == sql.ErrNoRows {
			row := writer.db.QueryRow("SELECT COALESCE(MAX(id), 0) as MaxId FROM dns_cities")
			var maxID int
			row.Scan(&maxID)
			writer.cityID = maxID + 1
			needInsert = true
		}
	} else {
		row := writer.db.QueryRow("SELECT COUNT(*) from dns_cities WHERE id = ?", writer.cityID)
		var count int
		row.Scan(&count)
		if count == 0 {
			needInsert = true
		}
	}
	if needInsert {
		_, err = writer.db.Exec("INSERT INTO dns_cities VALUES(?, ?)", writer.cityID, writer.city)
	}
	return nil
}

func (writer *SqliteWriter) open() error {
	// Gotta have this here to init sqlite package
	sqlite3.Version()
	var db *sql.DB
	var err error
	if db, err = sql.Open("sqlite3", writer.connStr()); err != nil {
		return err
	}
	writer.db = db
	if err = db.Ping(); err != nil {
		return err
	}
	if err = writer.initDB(); err != nil {
		return err
	}
	if err = writer.initCityID(); err != nil {
		return err
	}
	if writer.conn, err = db.Conn(context.TODO()); err != nil {
		return err
	}
	writer.tx, err = writer.conn.BeginTx(context.TODO(), nil)
	return err
}

func (writer *SqliteWriter) Write(id int, name string, price int, bonus int) (bool, bool, error) {
	var priceChanged = false
	var bonusChanged = false
	if writer.db == nil && !writer.fail {
		if err := writer.open(); err != nil {
			fmt.Println(err)
			writer.fail = true
		}
	}
	if writer.fail {
		return false, false, errors.New("No connection")
	}
	writer.tx.Exec("INSERT INTO dns_items VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET name = ? WHERE id = ? AND name is NULL", id, name, name, id)

	var err error
	var oldPrice, oldBonus int
	row := writer.tx.QueryRow("SELECT price FROM dns_prices WHERE itemId = ? AND cityID = ? ORDER BY date DESC LIMIT 1", id, writer.cityID)

	err = row.Scan(&oldPrice)
	if err == sql.ErrNoRows || oldPrice != price {
		_, err = writer.tx.Exec("INSERT INTO dns_prices VALUES (?, ?, ?, ?)", id, writer.cityID, price, writer.time)
		priceChanged = err == nil
	}
	if err == nil {
		row = writer.tx.QueryRow("SELECT bonus FROM dns_bonuses WHERE itemId = ? AND cityID = ? ORDER BY date DESC LIMIT 1", id, writer.cityID)
		err = row.Scan(&oldBonus)
		if err == sql.ErrNoRows || oldBonus != bonus {
			_, err = writer.tx.Exec("INSERT INTO dns_bonuses VALUES (?, ?, ?, ?)", id, writer.cityID, bonus, writer.time)
			bonusChanged = err == nil
		}
	}
	return priceChanged, bonusChanged, err
}

/*func (writer *SqliteWriter) save() error {
	var backupConn, memConn driver.Conn
	var err error
	sqlite := sqlite3.SQLiteDriver{}
	// Open connection to the in-memory DB
	if memConn, err = sqlite.Open(writer.connStr()); err != nil {
		return err
	}
	defer memConn.Close()
	src := memConn.(*sqlite3.SQLiteConn)
	// Open connection to the file DB
	fileDSN := fmt.Sprintf("file:%s", writer.filename)
	if backupConn, err = sqlite.Open(fileDSN); err != nil {
		return err
	}
	defer backupConn.Close()
	dest := backupConn.(*sqlite3.SQLiteConn)
	// Now copy backup DB from memory to the file
	var backup *sqlite3.SQLiteBackup
	backup, err = dest.Backup("main", src, "main")
	if err != nil {
		return err
	}
	backup.Step(-1)
	backup.Finish()
	return nil
}*/

// Close save changes to the database to file and closes DB connection
func (writer *SqliteWriter) Close() {
	if writer.tx != nil {
		if writer.fail {
			writer.tx.Rollback()
		} else {
			writer.tx.Commit()
		}
	}
	if writer.conn != nil {
		writer.conn.Close()
	}
	/*if err := writer.save(); err != nil {
		fmt.Printf("Saving failed: %s\r\n", err)
	}*/
	if writer.db != nil {
		writer.db.Close()
	}
}

// NewSqliteWriter returns a SqliteWriter object
func NewSqliteWriter(filename string, cityID int, city string) DNSWriter {
	w := SqliteWriter{
		filename: filename,
		time:     time.Now(),
		city:     city,
		cityID:   cityID,
	}
	return &w
}
